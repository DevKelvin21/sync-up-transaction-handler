# sync-up-transaction-handler

## Overview

This repository contains a Google Cloud Function designed to synchronize transaction data in a BigQuery table. The function is intended for use in environments where transactional sales data is ingested into BigQuery, and certain fields (such as `total_sale_price`) need to be calculated and updated after initial ingestion.

## What does the Cloud Function do?

- **Fetches Transactions:**  
  The function queries a specified BigQuery table for all transactions from the current date (in the "America/El_Salvador" timezone) where the `total_sale_price` field is `NULL`.

- **Calculates Total Sale Price:**  
  For each transaction, it calculates the `total_sale_price` by summing the product of `quantity` and `unit_price` for each sale item in the transaction.

- **Updates Transactions:**  
  It updates the `total_sale_price` field in BigQuery for each eligible transaction.

- **Handles Streaming Buffer Issues:**  
  If a transaction is still in BigQuery's streaming buffer (i.e., not yet available for DML updates), the function will skip updating that transaction and log a warning. This prevents errors related to BigQuery's streaming buffer limitations.

- **Logging:**  
  All major actions and errors are logged using Google Cloud Logging for easy monitoring and troubleshooting.

## Design & Best Practices

- **Service Pattern:**  
  BigQuery operations are encapsulated in a service class (`BigQueryTransactionService`) to promote maintainability and separation of concerns.

- **Environment Variables:**  
  Project, dataset, and table IDs are loaded from environment variables for flexibility and security.

- **Error Handling:**  
  The function gracefully handles missing environment variables, BigQuery query errors, and streaming buffer update issues.

## Usage

1. **Set Environment Variables:**  
   Ensure the following environment variables are set:
   - `BQ_PROJECT`: Your GCP project ID
   - `BQ_DATASET`: The BigQuery dataset name
   - `BQ_TABLE`: The BigQuery table name

2. **Deploy the Function:**  
   Deploy the function to Google Cloud Functions using the Google Cloud Console or CLI.

3. **Trigger:**  
   The function is designed to be triggered via HTTP requests.

## Example Table Schema

The BigQuery table should have at least the following fields:
- `id`: Unique identifier for the transaction
- `date`: Date of the transaction
- `sales`: Array of sale items, each with `quantity` and `unit_price`
- `total_sale_price`: Field to be calculated and updated by this function

## Requirements

See `requirements.txt` for Python dependencies.

## License

MIT License