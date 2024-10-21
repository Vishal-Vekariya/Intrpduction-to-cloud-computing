# Mid-term Project Scenario #2:

### Create master key

```sql
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'vishal@1234';
```
 
### Create database scoped credential

```sql
CREATE DATABASE SCOPED CREDENTIAL OrderStorageCredential 
WITH IDENTITY = 'microsoftstorageaccount',
SECRET = 'AJWY+ht8ITfUNBtkG8CLAKtZWrMf5tavQqptJ8cf5rC9jHAjM0Frp19I+Ak5FTKPL=sw=';
```

### Create external data source

```sql
CREATE EXTERNAL DATA SOURCE OrderExternalDataSource
WITH (
    TYPE = HADOOP,
    LOCATION = 'wasbs://orderstoragecontainer@orderstorageacq3.blob.core.windows.net',
    CREDENTIAL = OrderStorageCredential
);
```

 
### Create external file format

```sql
CREATE EXTERNAL FILE FORMAT CsvFileFormat
WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (FIELD_TERMINATOR = ',', STRING_DELIMITER = '"', FIRST_ROW = 2)
);
```

### Create a External Data Soruce:

```sql
CREATE EXTERNAL TABLE ExtraOrdersData1 (
    ORDERNUMBER INT,
    QUANTITYORDERED INT,
    PRICEEACH DECIMAL(10,2),
    ORDERLINENUMBER INT,
    SALES DECIMAL(10,2),
    ORDERDATE VARCHAR(50), -- Keeping as VARCHAR here as we will convert it later
    DAYS_SINCE_LASTORDER INT,
    STATUS VARCHAR(20),
    PRODUCTLINE VARCHAR(50),
    MSRP DECIMAL(10,2),
    PRODUCTCODE VARCHAR(50),
    CUSTOMERNAME VARCHAR(100),
    PHONE VARCHAR(50),
    ADDRESSLINE1 VARCHAR(100),
    CITY VARCHAR(50),
    POSTALCODE VARCHAR(20),
    COUNTRY VARCHAR(50),
    CONTACTLASTNAME VARCHAR(50),
    CONTACTFIRSTNAME VARCHAR(50),
    DEALSIZE VARCHAR(20)
)
WITH (
    LOCATION = '/Salesdata.csv',
    DATA_SOURCE = OrdersExternalDataSource,
    FILE_FORMAT = CsvFileFormat
);

CREATE TABLE SalesTable1
WITH (
    DISTRIBUTION = HASH(ORDERNUMBER),
    CLUSTERED COLUMNSTORE INDEX
)
AS 
SELECT 
    ORDERNUMBER,
    QUANTITYORDERED,
    PRICEEACH,
    ORDERLINENUMBER,
    SALES,
    TRY_CAST(ORDERDATE AS DATETIME) AS ORDERDATE, -- Converting ORDERDATE to DATETIME
    DAYS_SINCE_LASTORDER,
    STATUS,
    PRODUCTLINE,
    MSRP,
    PRODUCTCODE,
    CUSTOMERNAME,
    PHONE,
    ADDRESSLINE1,
    CITY,
    POSTALCODE,
    COUNTRY,
    CONTACTLASTNAME,
    CONTACTFIRSTNAME,
    DEALSIZE
FROM ExtraOrdersData1;
```

## SQL Queries for creating a view

### Create a View for the Salesbycountry

```sql
CREATE VIEW vw_SalesByCountry AS
SELECT
    COUNTRY,
    COUNT(DISTINCT ORDERNUMBER) AS TotalOrders,
    COUNT(DISTINCT CUSTOMERNAME) AS UniqueCustomers,
    SUM(SALES) AS TotalSales
FROM ExtraOrdersData1
GROUP BY COUNTRY;
```

### Create a View for the productperformance

```sql
CREATE VIEW vw_ProductPerformance AS
SELECT
    PRODUCTLINE,
    PRODUCTCODE,
    COUNT(DISTINCT ORDERNUMBER) AS TotalOrders,
    SUM(QUANTITYORDERED) AS TotalQuantity,
    SUM(SALES) AS TotalSales,
    AVG(PRICEEACH) AS AveragePrice
FROM ExtraOrdersData1
GROUP BY PRODUCTLINE, PRODUCTCODE;
```

### Create a View for the Topcustomer


```sql
CREATE VIEW vw_TopCustomersBySales1 AS
SELECT TOP 100
    CUSTOMERNAME,
    COUNT(DISTINCT ORDERNUMBER) AS TotalOrders,
    SUM(SALES) AS TotalSales,
    AVG(SALES) AS AverageSale
FROM ExtraOrdersData1
GROUP BY CUSTOMERNAME
ORDER BY TotalSales DESC;
