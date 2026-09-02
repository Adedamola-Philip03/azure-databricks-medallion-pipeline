# Databricks notebook source
# ---------------------------------------------------------
# Unit Tests — Silver Layer Validation Functions
# Tests each validation function in isolation using controlled fake
# data — no real database or Bronze/Silver tables are touched. This
# verifies business rules behave correctly without needing to manually
# re-run the full pipeline and eyeball output every time a change is made.
# ---------------------------------------------------------

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# ---------------------------------------------------------
# Orders validation (referential integrity)
# ---------------------------------------------------------

MIN_VALID_QUANTITY = 0

def validate_orders(df: DataFrame, df_customers: DataFrame, df_products: DataFrame) -> DataFrame:
    """Rejects orders referencing a non-existent CustomerID/ProductID,
    a non-positive Quantity, or a missing/unparseable OrderDate."""
    valid_customer_ids = df_customers.select("CustomerID").distinct().withColumnRenamed("CustomerID", "valid_cust_id")
    valid_product_ids = df_products.select("ProductID").distinct().withColumnRenamed("ProductID", "valid_prod_id")

    df_joined = (
        df
        .join(valid_customer_ids, df["CustomerID"] == col("valid_cust_id"), "left")
        .join(valid_product_ids, df["ProductID"] == col("valid_prod_id"), "left")
    )

    return (
        df_joined
        .withColumn("is_customer_valid", col("valid_cust_id").isNotNull())
        .withColumn("is_product_valid", col("valid_prod_id").isNotNull())
        .withColumn("is_quantity_valid", col("Quantity").isNotNull() & (col("Quantity") > MIN_VALID_QUANTITY))
        .withColumn("is_date_valid", col("OrderDate").isNotNull())
        .withColumn(
            "rejection_reason",
            when(~col("is_customer_valid"), lit("unknown_customer"))
            .when(~col("is_product_valid"), lit("unknown_product"))
            .when(~col("is_quantity_valid"), lit("invalid_quantity"))
            .when(~col("is_date_valid"), lit("missing_order_date"))
            .otherwise(lit(None))
        )
        .drop("valid_cust_id", "valid_prod_id", "is_customer_valid", "is_product_valid", "is_quantity_valid", "is_date_valid")
    )

# COMMAND ----------

def test_validate_orders_rejects_unknown_product():
    """An order referencing a ProductID absent from Silver Products
    should be rejected with 'unknown_product'."""
    df_orders = spark.createDataFrame(
        [(3001, 1001, 9999, 1, "2026-01-01")],
        ["OrderID", "CustomerID", "ProductID", "Quantity", "OrderDate"]
    )
    df_customers = spark.createDataFrame([(1001,)], ["CustomerID"])
    df_products = spark.createDataFrame([(2001,)], ["ProductID"])

    result = validate_orders(df_orders, df_customers, df_products)
    reason = result.filter("OrderID = 3001").collect()[0]["rejection_reason"]
    assert reason == "unknown_product", f"Expected 'unknown_product', got '{reason}'"
    print("PASS: test_validate_orders_rejects_unknown_product")


def test_validate_orders_rejects_unknown_customer():
    """An order referencing a CustomerID absent from Silver Customers
    should be rejected with 'unknown_customer'."""
    df_orders = spark.createDataFrame(
        [(3002, 9999, 2001, 1, "2026-01-01")],
        ["OrderID", "CustomerID", "ProductID", "Quantity", "OrderDate"]
    )
    df_customers = spark.createDataFrame([(1001,)], ["CustomerID"])
    df_products = spark.createDataFrame([(2001,)], ["ProductID"])

    result = validate_orders(df_orders, df_customers, df_products)
    reason = result.filter("OrderID = 3002").collect()[0]["rejection_reason"]
    assert reason == "unknown_customer", f"Expected 'unknown_customer', got '{reason}'"
    print("PASS: test_validate_orders_rejects_unknown_customer")


def test_validate_orders_rejects_zero_quantity():
    """An order with Quantity = 0 should be rejected with
    'invalid_quantity'."""
    df_orders = spark.createDataFrame(
        [(3003, 1001, 2001, 0, "2026-01-01")],
        ["OrderID", "CustomerID", "ProductID", "Quantity", "OrderDate"]
    )
    df_customers = spark.createDataFrame([(1001,)], ["CustomerID"])
    df_products = spark.createDataFrame([(2001,)], ["ProductID"])

    result = validate_orders(df_orders, df_customers, df_products)
    reason = result.filter("OrderID = 3003").collect()[0]["rejection_reason"]
    assert reason == "invalid_quantity", f"Expected 'invalid_quantity', got '{reason}'"
    print("PASS: test_validate_orders_rejects_zero_quantity")


def test_validate_orders_accepts_valid_order():
    """A genuinely valid order — real customer, real product, positive
    quantity, real date — should NOT be rejected."""
    df_orders = spark.createDataFrame(
        [(3004, 1001, 2001, 2, "2026-01-01")],
        ["OrderID", "CustomerID", "ProductID", "Quantity", "OrderDate"]
    )
    df_customers = spark.createDataFrame([(1001,)], ["CustomerID"])
    df_products = spark.createDataFrame([(2001,)], ["ProductID"])

    result = validate_orders(df_orders, df_customers, df_products)
    reason = result.filter("OrderID = 3004").collect()[0]["rejection_reason"]
    assert reason is None, f"Expected no rejection, got '{reason}'"
    print("PASS: test_validate_orders_accepts_valid_order")

# COMMAND ----------

# ---------------------------------------------------------
# Customers validation (hard rule + soft flag)
# ---------------------------------------------------------

VALID_COUNTRIES = ["Nigeria", "Ghana", "Kenya", "Togo"]
EMAIL_PATTERN = r"^[A-Za-z0-9._-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

def validate_customers(df: DataFrame) -> DataFrame:
    """Country is a hard rejection rule. Email is a soft quality flag."""
    return (
        df.withColumn("is_country_valid", col("Country").isin(VALID_COUNTRIES))
          .withColumn(
              "email_quality_flag",
              when(col("Email").isNull(), "missing")
              .when(~col("Email").rlike(EMAIL_PATTERN), "invalid_format")
              .otherwise("valid")
          )
          .withColumn(
              "rejection_reason",
              when(~col("is_country_valid"), lit("invalid_country")).otherwise(lit(None))
          )
          .drop("is_country_valid")
    )

# COMMAND ----------

def test_validate_customers_rejects_invalid_country():
    """A customer with a country outside the accepted list should be
    rejected with 'invalid_country'."""
    df = spark.createDataFrame(
        [(1001, "France", "test@email.com")],
        ["CustomerID", "Country", "Email"]
    )
    result = validate_customers(df)
    reason = result.filter("CustomerID = 1001").collect()[0]["rejection_reason"]
    assert reason == "invalid_country", f"Expected 'invalid_country', got '{reason}'"
    print("PASS: test_validate_customers_rejects_invalid_country")


def test_validate_customers_flags_missing_email_not_rejected():
    """A customer with a null email should NOT be rejected — only
    flagged as 'missing'. This is the hard-rejection vs soft-flag
    distinction that is core to the design."""
    schema = StructType([
        StructField("CustomerID", IntegerType(), True),
        StructField("Country", StringType(), True),
        StructField("Email", StringType(), True),
    ])
    df = spark.createDataFrame([(1002, "Nigeria", None)], schema)

    result = validate_customers(df)
    row = result.filter("CustomerID = 1002").collect()[0]
    assert row["rejection_reason"] is None, "Missing email should not cause rejection"
    assert row["email_quality_flag"] == "missing", f"Expected 'missing', got '{row['email_quality_flag']}'"
    print("PASS: test_validate_customers_flags_missing_email_not_rejected")


def test_validate_customers_flags_invalid_email_format():
    """A malformed email (no domain extension) should be flagged as
    'invalid_format', not rejected."""
    df = spark.createDataFrame(
        [(1003, "Ghana", "broken@email")],
        ["CustomerID", "Country", "Email"]
    )
    result = validate_customers(df)
    row = result.filter("CustomerID = 1003").collect()[0]
    assert row["rejection_reason"] is None, "Bad email format should not cause rejection"
    assert row["email_quality_flag"] == "invalid_format", f"Expected 'invalid_format', got '{row['email_quality_flag']}'"
    print("PASS: test_validate_customers_flags_invalid_email_format")


def test_validate_customers_accepts_valid_record():
    """A genuinely valid customer should pass with no rejection and a
    'valid' email flag."""
    df = spark.createDataFrame(
        [(1004, "Kenya", "good@email.com")],
        ["CustomerID", "Country", "Email"]
    )
    result = validate_customers(df)
    row = result.filter("CustomerID = 1004").collect()[0]
    assert row["rejection_reason"] is None
    assert row["email_quality_flag"] == "valid"
    print("PASS: test_validate_customers_accepts_valid_record")

# COMMAND ----------

# ---------------------------------------------------------
# Products validation (hard rule only)
# ---------------------------------------------------------

MIN_VALID_PRICE = 0.0

def validate_products(df: DataFrame) -> DataFrame:
    """Price is a hard rejection rule — no soft-flag equivalent."""
    return (
        df.withColumn("is_price_valid", col("Price").isNotNull() & (col("Price") > MIN_VALID_PRICE))
          .withColumn(
              "rejection_reason",
              when(~col("is_price_valid"), lit("invalid_price")).otherwise(lit(None))
          )
          .drop("is_price_valid")
    )

# COMMAND ----------

def test_validate_products_rejects_null_price():
    """A product with a null Price should be rejected with
    'invalid_price'."""
    schema = StructType([
        StructField("ProductID", IntegerType(), True),
        StructField("Price", DoubleType(), True),
    ])
    df = spark.createDataFrame([(2001, None)], schema)

    result = validate_products(df)
    reason = result.filter("ProductID = 2001").collect()[0]["rejection_reason"]
    assert reason == "invalid_price", f"Expected 'invalid_price', got '{reason}'"
    print("PASS: test_validate_products_rejects_null_price")


def test_validate_products_rejects_zero_price():
    """A product with Price = 0 should be rejected with
    'invalid_price'."""
    df = spark.createDataFrame(
        [(2002, 0.0)],
        ["ProductID", "Price"]
    )
    result = validate_products(df)
    reason = result.filter("ProductID = 2002").collect()[0]["rejection_reason"]
    assert reason == "invalid_price", f"Expected 'invalid_price', got '{reason}'"
    print("PASS: test_validate_products_rejects_zero_price")


def test_validate_products_accepts_valid_price():
    """A product with a genuine positive price should NOT be
    rejected."""
    df = spark.createDataFrame(
        [(2003, 49.99)],
        ["ProductID", "Price"]
    )
    result = validate_products(df)
    reason = result.filter("ProductID = 2003").collect()[0]["rejection_reason"]
    assert reason is None, f"Expected no rejection, got '{reason}'"
    print("PASS: test_validate_products_accepts_valid_price")

# COMMAND ----------

# ---------------------------------------------------------
# Run all tests
# ---------------------------------------------------------

test_validate_orders_rejects_unknown_product()
test_validate_orders_rejects_unknown_customer()
test_validate_orders_rejects_zero_quantity()
test_validate_orders_accepts_valid_order()

test_validate_customers_rejects_invalid_country()
test_validate_customers_flags_missing_email_not_rejected()
test_validate_customers_flags_invalid_email_format()
test_validate_customers_accepts_valid_record()

test_validate_products_rejects_null_price()
test_validate_products_rejects_zero_price()
test_validate_products_accepts_valid_price()

print("\nAll 11 validation tests passed across Customers, Products, and Orders.")