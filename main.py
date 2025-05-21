from functions_framework import http
from flask import make_response, Request
import os
import logging
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

class BigQueryTransactionService:
    def __init__(self, project_id, dataset_id, table_id):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.client = bigquery.Client(project=project_id)

    def fetch_transactions(self):
        query = f"""
        SELECT * FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
        WHERE date = CURRENT_DATE("America/El_Salvador") AND total_sale_price IS NULL;
        """
        query_job = self.client.query(query)
        return [dict(row) for row in query_job.result()]

    def update_total_sale_price(self, transaction_id, total_sale_price):
        update_query = f"""
        UPDATE `{self.project_id}.{self.dataset_id}.{self.table_id}`
        SET total_sale_price = {total_sale_price}
        WHERE id = {transaction_id}
        """
        try:
            update_job = self.client.query(update_query)
            update_job.result()
            return True, None
        except BadRequest as e:
            # Streaming buffer issue: skip and log
            if "Streaming buffer" in str(e):
                logging.warning(f"Transaction {transaction_id} is in the streaming buffer. Skipping update.")
                return False, "streaming_buffer"
            raise
        except Exception as e:
            raise

def get_env_var(name):
    value = os.environ.get(name)
    if not value:
        logging.error(f"{name} environment variable is not set.")
        raise EnvironmentError(f"{name} environment variable is not set.")
    return value

@http
def synchronize_transactions(request: Request) -> str:
    # Initialize logging
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting synchronization process")

    try:
        project_id = get_env_var("BQ_PROJECT")
        dataset_id = get_env_var("BQ_DATASET")
        table_id = get_env_var("BQ_TABLE")
    except EnvironmentError as e:
        return make_response(str(e), 500)

    bq_service = BigQueryTransactionService(project_id, dataset_id, table_id)

    try:
        transactions = bq_service.fetch_transactions()
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        return make_response(f"Error executing query: {e}", 500)
    logging.info(f"Retrieved {len(transactions)} transactions")

    updated_count = 0
    skipped_streaming = 0

    for transaction in transactions:
        sales = transaction.get("sales")
        if not sales:
            continue
        total_sale_price = sum(sale["quantity"] * sale["unit_price"] for sale in sales)
        try:
            updated, reason = bq_service.update_total_sale_price(transaction["transaction_id"], total_sale_price)
            if updated:
                updated_count += 1
                logging.info(f"Updated transaction {transaction['transaction_id']} with total sale price {total_sale_price}")
            elif reason == "streaming_buffer":
                skipped_streaming += 1
        except Exception as e:
            logging.error(f"Error updating transaction {transaction['transaction_id']}: {e}")
            return make_response(f"Error updating transaction {transaction['transaction_id']}: {e}", 500)

    logging.info(f"All transactions processed. Updated: {updated_count}, Skipped (streaming buffer): {skipped_streaming}")

    return make_response(f"Synchronization complete. Updated: {updated_count}, Skipped (streaming buffer): {skipped_streaming}", 200)