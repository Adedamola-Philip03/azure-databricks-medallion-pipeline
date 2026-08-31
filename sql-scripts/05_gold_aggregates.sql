-- Gold Layer — Business Aggregates (SQL version)
-- Equivalent to the PySpark version in databricks-notebooks/07_gold_aggregates.py

CREATE OR REPLACE TABLE dataengineering.cloud_pipeline.gold_customer_country_summary AS
SELECT
    Country,
    COUNT(*) AS total_customers,
    SUM(CASE WHEN email_quality_flag = 'valid' THEN 1 ELSE 0 END) AS valid_email_count,
    SUM(CASE WHEN email_quality_flag = 'missing' THEN 1 ELSE 0 END) AS missing_email_count,
    SUM(CASE WHEN email_quality_flag = 'invalid_format' THEN 1 ELSE 0 END) AS invalid_email_count,
    ROUND(
        (SUM(CASE WHEN email_quality_flag = 'valid' THEN 1 ELSE 0 END) * 100.0) / COUNT(*),
        1
    ) AS valid_email_rate_pct
FROM dataengineering.cloud_pipeline.silver_customers
GROUP BY Country
ORDER BY total_customers DESC;

CREATE OR REPLACE TABLE dataengineering.cloud_pipeline.gold_revenue_by_category AS
SELECT
    p.Category,
    ROUND(SUM(o.Quantity * p.Price), 2) AS total_revenue,
    SUM(o.Quantity) AS total_units_sold
FROM dataengineering.cloud_pipeline.silver_orders o
JOIN dataengineering.cloud_pipeline.silver_products p
    ON o.ProductID = p.ProductID
GROUP BY p.Category
ORDER BY total_revenue DESC;