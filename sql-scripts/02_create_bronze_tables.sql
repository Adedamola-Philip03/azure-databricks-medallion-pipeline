-- Bronze layer tables: raw data with lineage columns
-- Append-only, no cleaning or validation applied here
USE DataEngineeringPractice;
GO

CREATE TABLE cloud_bronze.customers (
    CustomerID INT,
    CustomerName NVARCHAR(200),
    Country NVARCHAR(100),
    Email NVARCHAR(200),
    batch_id UNIQUEIDENTIFIER,
    source_file NVARCHAR(500),
    file_hash NVARCHAR(64),
    ingested_at DATETIME2,
    ingested_by NVARCHAR(200),
    row_hash NVARCHAR(64)
);
GO

CREATE TABLE cloud_bronze.products (
    ProductID INT,
    ProductName NVARCHAR(200),
    Category NVARCHAR(100),
    Price NVARCHAR(50),  -- intentionally string; cast happens in Silver
    batch_id UNIQUEIDENTIFIER,
    source_file NVARCHAR(500),
    file_hash NVARCHAR(64),
    ingested_at DATETIME2,
    ingested_by NVARCHAR(200),
    row_hash NVARCHAR(64)
);
GO

CREATE TABLE cloud_bronze.orders (
    OrderID INT,
    CustomerID INT,
    ProductID INT,
    Quantity INT,
    OrderDate NVARCHAR(50),  -- intentionally string; mixed formats handled in Silver
    batch_id UNIQUEIDENTIFIER,
    source_file NVARCHAR(500),
    file_hash NVARCHAR(64),
    ingested_at DATETIME2,
    ingested_by NVARCHAR(200),
    row_hash NVARCHAR(64)
);
GO