from functions_framework import http
from flask import make_response, Request
import os
import logging
from google.cloud import bigquery, firestore
from google.api_core.exceptions import BadRequest

class FirestoreLogger:
    def __init__(self, project_id):
        self.firestore_client = firestore.Client(project=project_id)

    def log_transaction(self, transaction):
        try:
            doc_ref = self.firestore_client.collection("transactions_to_check").document(str(transaction["transaction_id"]))
            doc_ref.set(transaction)
            logging.info(f"Logged transaction {transaction['transaction_id']} to Firestore.")
        except Exception as e:
            logging.error(f"Failed to log transaction {transaction['transaction_id']} to Firestore: {e}")

class BigQueryTransactionService:
    def __init__(self, project_id, dataset_id, table_id, firestore_logger=None):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.client = bigquery.Client(project=project_id)
        self.firestore_logger = firestore_logger

    def fetch_transactions(self):
        query = f"""
        SELECT * FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
        WHERE date = CURRENT_DATE("America/El_Salvador") AND total_sale_price IS NULL;
        """
        query_job = self.client.query(query)
        return [dict(row) for row in query_job.result()]

    def update_total_sale_price(self, transaction_id, total_sale_price):
        # If transaction_id is a string, quote it in SQL
        if isinstance(transaction_id, str):
            id_value = f"'{transaction_id}'"
        else:
            id_value = str(transaction_id)
        update_query = f"""
        UPDATE `{self.project_id}.{self.dataset_id}.{self.table_id}`
        SET total_sale_price = {total_sale_price}
        WHERE transaction_id = {id_value}
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

    def log_transaction_to_firestore(self, transaction):
        if self.firestore_logger:
            self.firestore_logger.log_transaction(transaction)

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

    firestore_logger = FirestoreLogger(project_id)
    bq_service = BigQueryTransactionService(project_id, dataset_id, table_id, firestore_logger=firestore_logger)

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
                # Log to Firestore after successful update
                bq_service.log_transaction_to_firestore(transaction)
            elif reason == "streaming_buffer":
                skipped_streaming += 1
        except Exception as e:
            logging.error(f"Error updating transaction {transaction['transaction_id']}: {e}")
            return make_response(f"Error updating transaction {transaction['transaction_id']}: {e}", 500)

    logging.info(f"All transactions processed. Updated: {updated_count}, Skipped (streaming buffer): {skipped_streaming}")

    return make_response(f"Synchronization complete. Updated: {updated_count}, Skipped (streaming buffer): {skipped_streaming}", 200)