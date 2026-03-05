# 🔀 Azure Data Factory: ETL Pipeline Architecture

To achieve the 8-hour reduction in the monthly closing cycle, I had to ensure the data pipelines feeding the SQL Server were optimized and governed. Below is the high-level data mapping and transformation logic used in the Azure Data Factory (ADF) pipelines.

## Pipeline: `PL_Finance_Ledger_Ingestion`
**Trigger:** Scheduled (Monthly, Last Business Day at 23:00 UTC)

| Source System | Source Format | ADF Activity | Target SQL Table | Transformation Logic Applied |
| :--- | :--- | :--- | :--- | :--- |
| **Dynamics 365 (ERP)** | REST API (JSON) | Copy Data | `Staging.Raw_Ledger` | Flatten nested JSON; drop redundant `system_modstamp` columns. |
| **Staging.Raw_Ledger** | SQL Server | Stored Procedure | `Finance.GeneralLedger` | Cast `Transaction_Date` to DATE; standardize currency to USD; flag null `Account_IDs`. |
| **Warehouse System** | CSV (Azure Blob) | Data Flow | `Warehouse.InventoryValuation` | Aggregate daily physical counts into monthly snapshots; filter out 'Damaged' status. |

## 🛡️ Data Governance & Error Handling
* **Automated Alerts:** If the `Copy Data` activity fails or pulls 0 rows, a webhook triggers an alert to the Data Engineering Teams channel.
* **Data Typing:** Forced strict schema validation on ingestion to prevent downstream SQL calculation errors, which directly contributed to the **40% reduction in reconciliation errors**.
