/* =============================================================================
Script: monthly_reconciliation.sql
Purpose: Automate the reconciliation of inventory valuation and ledger balances.
Optimization: Replaced legacy RBAR (Row-By-Agonizing-Row) logic with Window Functions.
=============================================================================
*/

-- Step 1: Isolate the current month's transactions using a CTE to reduce the data footprint
WITH CurrentMonthLedger AS (
    SELECT 
        Account_ID,
        Transaction_Date,
        Transaction_Type,
        Amount,
        -- Calculate running total without expensive self-joins
        SUM(Amount) OVER (
            PARTITION BY Account_ID 
            ORDER BY Transaction_Date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS Running_Balance
    FROM 
        Finance.GeneralLedger
    WHERE 
        Transaction_Date >= DATEADD(month, DATEDIFF(month, 0, GETDATE()) - 1, 0) -- First day of previous month
        AND Transaction_Date < DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)  -- First day of current month
),

-- Step 2: Flag discrepancies between physical inventory counts and ledger balances
ReconciliationFlags AS (
    SELECT 
        cml.Account_ID,
        cml.Running_Balance AS System_Balance,
        inv.Physical_Count_Value,
        ABS(cml.Running_Balance - inv.Physical_Count_Value) AS Variance_Amount,
        CASE 
            WHEN ABS(cml.Running_Balance - inv.Physical_Count_Value) > 500.00 THEN 'High Variance - Audit Required'
            WHEN ABS(cml.Running_Balance - inv.Physical_Count_Value) > 0.00 THEN 'Minor Variance'
            ELSE 'Reconciled'
        END AS Audit_Status
    FROM 
        CurrentMonthLedger cml
    LEFT JOIN 
        Warehouse.InventoryValuation inv ON cml.Account_ID = inv.Account_ID
    WHERE 
        -- Only pull the final balance record for the month for each account
        cml.Transaction_Date = (SELECT MAX(Transaction_Date) FROM CurrentMonthLedger WHERE Account_ID = cml.Account_ID)
)

-- Step 3: Output final report for Power BI ingestion
SELECT 
    Account_ID,
    System_Balance,
    Physical_Count_Value,
    Variance_Amount,
    Audit_Status
FROM 
    ReconciliationFlags
WHERE 
    Audit_Status != 'Reconciled'
ORDER BY 
    Variance_Amount DESC;

/*
Implementation Note:
To support this query in production, a covering index was added:
CREATE NONCLUSTERED INDEX IX_GeneralLedger_Date_Account 
ON Finance.GeneralLedger (Transaction_Date) INCLUDE (Account_ID, Transaction_Type, Amount);
*/
