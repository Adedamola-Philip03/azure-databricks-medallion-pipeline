-- Silver layer processing for Customers:
-- 1. Dedup (latest ingested_at wins per CustomerID)
-- 2. Clean (trim, title-case names/countries, fix literal "null" strings)
-- 3. Validate (Country = hard rejection rule, Email = soft quality flag)
-- 4. Merge good rows into Silver, log rejects separately
USE DataEngineeringPractice;
GO

CREATE OR ALTER PROCEDURE cloud_silver.usp_process_customers
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        CustomerID,
        dbo.fn_InitCap(CustomerName) AS CustomerName,
        dbo.fn_InitCap(Country) AS Country,
        CASE
            WHEN LOWER(LTRIM(RTRIM(Email))) = 'null' THEN NULL
            ELSE LOWER(LTRIM(RTRIM(Email)))
        END AS Email,
        batch_id,
        source_file,
        ingested_at,
        ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY ingested_at DESC) AS rn
    INTO #deduped_cleaned
    FROM cloud_bronze.customers;

    SELECT *,
        CASE
            WHEN Email IS NULL THEN 'missing'
            WHEN Email NOT LIKE '%_@_%_.__%' THEN 'invalid_format'
            ELSE 'valid'
        END AS email_quality_flag,
        CASE
            WHEN Country NOT IN ('Nigeria', 'Ghana', 'Kenya', 'Togo') THEN 'invalid_country'
            ELSE NULL
        END AS rejection_reason
    INTO #validated
    FROM #deduped_cleaned
    WHERE rn = 1;

    MERGE cloud_silver.customers AS target
    USING (SELECT * FROM #validated WHERE rejection_reason IS NULL) AS source
    ON target.CustomerID = source.CustomerID
    WHEN MATCHED THEN
        UPDATE SET
            CustomerName = source.CustomerName,
            Country = source.Country,
            Email = source.Email,
            email_quality_flag = source.email_quality_flag,
            batch_id = source.batch_id,
            source_file = source.source_file,
            ingested_at = source.ingested_at
    WHEN NOT MATCHED THEN
        INSERT (CustomerID, CustomerName, Country, Email, email_quality_flag, batch_id, source_file, ingested_at)
        VALUES (source.CustomerID, source.CustomerName, source.Country, source.Email, source.email_quality_flag, source.batch_id, source.source_file, source.ingested_at);

    INSERT INTO cloud_silver.rejected_customers (CustomerID, CustomerName, Country, Email, batch_id, source_file, ingested_at, rejection_reason)
    SELECT CustomerID, CustomerName, Country, Email, batch_id, source_file, ingested_at, rejection_reason
    FROM #validated
    WHERE rejection_reason IS NOT NULL;

    DROP TABLE #deduped_cleaned;
    DROP TABLE #validated;

    PRINT 'Silver Customers processing complete.';
END;
GO