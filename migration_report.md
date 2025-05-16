# Database Migration Verification Report
Generated: 2025-05-16 12:36:24

## Migrated Tables
- **HumanResources**: Department, Employee, EmployeeDepartmentHistory, EmployeePayHistory, JobCandidate, Shift
- **Person**: Address, AddressType, BusinessEntity, BusinessEntityAddress, BusinessEntityContact, ContactType, CountryRegion, EmailAddress, Password, Person, PersonPhone, PhoneNumberType, StateProvince
- **Production**: BillOfMaterials, Culture, Document, Illustration, Location, Product, ProductCategory, ProductCostHistory, ProductDescription, ProductDocument, ProductInventory, ProductListPriceHistory, ProductModel, ProductModelIllustration, ProductModelProductDescriptionCulture, ProductPhoto, ProductProductPhoto, ProductReview, ProductSubcategory, ScrapReason, TransactionHistory, TransactionHistoryArchive, UnitMeasure, WorkOrder, WorkOrderRouting
- **Purchasing**: ProductVendor, PurchaseOrderDetail, PurchaseOrderHeader, ShipMethod, Vendor
- **Sales**: CountryRegionCurrency, CreditCard, Currency, CurrencyRate, Customer, PersonCreditCard, SalesOrderDetail, SalesOrderHeader, SalesOrderHeaderSalesReason, SalesPerson, SalesPersonQuotaHistory, SalesReason, SalesTaxRate, SalesTerritory, SalesTerritoryHistory, ShoppingCartItem, SpecialOffer, SpecialOfferProduct, Store
- **dbo**: AWBuildVersion, DatabaseLog, ErrorLog

## Row Count Mismatches
All row counts match.

## Column-wise Mean Summary
- HumanResources.Department.DepartmentID [numeric]: SQL Server = 8, PostgreSQL = 8.5000000000000000
- HumanResources.Employee.VacationHours [numeric]: SQL Server = 50, PostgreSQL = 50.6137931034482759
- HumanResources.Employee.SickLeaveHours [numeric]: SQL Server = 45, PostgreSQL = 45.3068965517241379
- HumanResources.Employee.OrganizationLevel [numeric]: SQL Server = 3, PostgreSQL = 3.5224913494809689
- HumanResources.EmployeeDepartmentHistory.DepartmentID [numeric]: SQL Server = 7, PostgreSQL = 7.2668918918918919
- HumanResources.EmployeeDepartmentHistory.ShiftID [numeric]: SQL Server = 1, PostgreSQL = 1.5608108108108108
- HumanResources.EmployeePayHistory.PayFrequency [numeric]: SQL Server = 1, PostgreSQL = 1.4303797468354430
- Production.BillOfMaterials.BOMLevel [numeric]: SQL Server = 1, PostgreSQL = 1.3598357596117954
- Production.Document.DocumentLevel [numeric]: SQL Server = 1, PostgreSQL = 1.6153846153846154
- Production.Document.Status [numeric]: SQL Server = 1, PostgreSQL = 1.9230769230769231
- Production.Location.LocationID [numeric]: SQL Server = 20, PostgreSQL = 20.2142857142857143
- Production.Product.ReorderPoint [numeric]: SQL Server = 401, PostgreSQL = 401.3630952380952381
- Production.Product.SafetyStockLevel [numeric]: SQL Server = 535, PostgreSQL = 535.1507936507936508
- Production.ProductInventory.LocationID [numeric]: SQL Server = 23, PostgreSQL = 23.6847521047708138
- Production.ProductInventory.Quantity [numeric]: SQL Server = 314, PostgreSQL = 314.2881197380729654
- Production.ProductInventory.Bin [numeric]: SQL Server = 8, PostgreSQL = 8.8999064546304958
- Production.ScrapReason.ScrapReasonID [numeric]: SQL Server = 8, PostgreSQL = 8.5000000000000000
- Production.WorkOrder.ScrappedQty [numeric]: SQL Server = 0, PostgreSQL = 0.14672617817635795071
- Production.WorkOrder.ScrapReasonID [numeric]: SQL Server = 8, PostgreSQL = 8.7242798353909465
- Production.WorkOrderRouting.OperationSequence [numeric]: SQL Server = 5, PostgreSQL = 5.1099641000431991
- Production.WorkOrderRouting.LocationID [numeric]: SQL Server = 43, PostgreSQL = 43.8007775841265585
- Purchasing.PurchaseOrderDetail.OrderQty [numeric]: SQL Server = 265, PostgreSQL = 265.5327303561334087
- Purchasing.PurchaseOrderHeader.Status [numeric]: SQL Server = 3, PostgreSQL = 3.8043369890329013
- Purchasing.PurchaseOrderHeader.RevisionNumber [numeric]: SQL Server = 4, PostgreSQL = 4.0830009970089731
- Purchasing.Vendor.CreditRating [numeric]: SQL Server = 1, PostgreSQL = 1.3557692307692308
- Sales.CreditCard.ExpYear [numeric]: SQL Server = 2006, PostgreSQL = 2006.5001046134532901
- Sales.CreditCard.ExpMonth [numeric]: SQL Server = 6, PostgreSQL = 6.5273041113087143
- Sales.SalesOrderDetail.OrderQty [numeric]: SQL Server = 2, PostgreSQL = 2.2660797744751354

<details><summary>📊 Full Mean Dump</summary>

```
HumanResources.Department.DepartmentID [numeric]: SQL Server = 8, PostgreSQL = 8.5000000000000000
HumanResources.Employee.VacationHours [numeric]: SQL Server = 50, PostgreSQL = 50.6137931034482759
HumanResources.Employee.SickLeaveHours [numeric]: SQL Server = 45, PostgreSQL = 45.3068965517241379
HumanResources.Employee.OrganizationLevel [numeric]: SQL Server = 3, PostgreSQL = 3.5224913494809689
HumanResources.Employee.HireDate [datetime]: SQL Server = None, PostgreSQL = 1242712651.03448276
HumanResources.Employee.BirthDate [datetime]: SQL Server = None, PostgreSQL = 268403089.65517241
HumanResources.EmployeeDepartmentHistory.DepartmentID [numeric]: SQL Server = 7, PostgreSQL = 7.2668918918918919
HumanResources.EmployeeDepartmentHistory.ShiftID [numeric]: SQL Server = 1, PostgreSQL = 1.5608108108108108
HumanResources.EmployeeDepartmentHistory.EndDate [datetime]: SQL Server = None, PostgreSQL = 1312660800.00000000
HumanResources.EmployeeDepartmentHistory.StartDate [datetime]: SQL Server = None, PostgreSQL = 1244132270.27027027
HumanResources.EmployeePayHistory.Rate [numeric]: SQL Server = 17.7588, PostgreSQL = 17.7588041139240506
HumanResources.EmployeePayHistory.PayFrequency [numeric]: SQL Server = 1, PostgreSQL = 1.4303797468354430
HumanResources.Shift.ShiftID [numeric]: SQL Server = 2, PostgreSQL = 2.0000000000000000
Production.BillOfMaterials.PerAssemblyQty [numeric]: SQL Server = 2.054124, PostgreSQL = 2.0541246733855916
Production.BillOfMaterials.BOMLevel [numeric]: SQL Server = 1, PostgreSQL = 1.3598357596117954
Production.Document.DocumentLevel [numeric]: SQL Server = 1, PostgreSQL = 1.6153846153846154
Production.Document.Status [numeric]: SQL Server = 1, PostgreSQL = 1.9230769230769231
Production.Location.LocationID [numeric]: SQL Server = 20, PostgreSQL = 20.2142857142857143
Production.Location.CostRate [numeric]: SQL Server = 8.5892, PostgreSQL = 8.5892857142857143
Production.Location.Availability [numeric]: SQL Server = 54.571428, PostgreSQL = 54.5714285714285714
Production.Product.Weight [numeric]: SQL Server = 74.069219, PostgreSQL = 74.0692195121951220
Production.Product.StandardCost [numeric]: SQL Server = 258.6029, PostgreSQL = 258.6029613095238095
Production.Product.ReorderPoint [numeric]: SQL Server = 401, PostgreSQL = 401.3630952380952381
Production.Product.ListPrice [numeric]: SQL Server = 438.6662, PostgreSQL = 438.6662500000000000
Production.Product.SafetyStockLevel [numeric]: SQL Server = 535, PostgreSQL = 535.1507936507936508
Production.ProductCostHistory.StandardCost [numeric]: SQL Server = 434.2658, PostgreSQL = 434.2658288607594937
Production.ProductInventory.LocationID [numeric]: SQL Server = 23, PostgreSQL = 23.6847521047708138
Production.ProductInventory.Quantity [numeric]: SQL Server = 314, PostgreSQL = 314.2881197380729654
Production.ProductInventory.Bin [numeric]: SQL Server = 8, PostgreSQL = 8.8999064546304958
Production.ProductListPriceHistory.ListPrice [numeric]: SQL Server = 747.6617, PostgreSQL = 747.6617622784810127
Production.ScrapReason.ScrapReasonID [numeric]: SQL Server = 8, PostgreSQL = 8.5000000000000000
Production.TransactionHistory.ActualCost [numeric]: SQL Server = 240.7141, PostgreSQL = 240.7141132877304021
Production.TransactionHistoryArchive.ActualCost [numeric]: SQL Server = 396.6242, PostgreSQL = 396.6242912832061667
Production.WorkOrder.ScrappedQty [numeric]: SQL Server = 0, PostgreSQL = 0.14672617817635795071
Production.WorkOrder.ScrapReasonID [numeric]: SQL Server = 8, PostgreSQL = 8.7242798353909465
Production.WorkOrderRouting.PlannedCost [numeric]: SQL Server = 51.9576, PostgreSQL = 51.9576574160968852
Production.WorkOrderRouting.OperationSequence [numeric]: SQL Server = 5, PostgreSQL = 5.1099641000431991
Production.WorkOrderRouting.ActualCost [numeric]: SQL Server = 51.9576, PostgreSQL = 51.9576574160968852
Production.WorkOrderRouting.LocationID [numeric]: SQL Server = 43, PostgreSQL = 43.8007775841265585
Production.WorkOrderRouting.ActualResourceHrs [numeric]: SQL Server = 3.410677, PostgreSQL = 3.4106776303049262
Purchasing.ProductVendor.LastReceiptCost [numeric]: SQL Server = 36.2758, PostgreSQL = 36.2758947826086957
Purchasing.ProductVendor.StandardPrice [numeric]: SQL Server = 34.6765, PostgreSQL = 34.6765434782608696
Purchasing.PurchaseOrderDetail.LineTotal [numeric]: SQL Server = 7212.2097, PostgreSQL = 7212.2097046919163369
Purchasing.PurchaseOrderDetail.StockedQty [numeric]: SQL Server = 254.900960, PostgreSQL = 254.9009609949123799
Purchasing.PurchaseOrderDetail.ReceivedQty [numeric]: SQL Server = 263.120293, PostgreSQL = 263.1202939513849633
Purchasing.PurchaseOrderDetail.RejectedQty [numeric]: SQL Server = 8.219332, PostgreSQL = 8.2193329564725834
Purchasing.PurchaseOrderDetail.UnitPrice [numeric]: SQL Server = 34.7429, PostgreSQL = 34.7429641153193895
Purchasing.PurchaseOrderDetail.OrderQty [numeric]: SQL Server = 265, PostgreSQL = 265.5327303561334087
Purchasing.PurchaseOrderHeader.Status [numeric]: SQL Server = 3, PostgreSQL = 3.8043369890329013
Purchasing.PurchaseOrderHeader.TotalDue [numeric]: SQL Server = 17567.1317, PostgreSQL = 17567.131764282154
Purchasing.PurchaseOrderHeader.TaxAmt [numeric]: SQL Server = 1272.0238, PostgreSQL = 1272.0238220338983051
Purchasing.PurchaseOrderHeader.Freight [numeric]: SQL Server = 394.8101, PostgreSQL = 394.8101261964107677
Purchasing.PurchaseOrderHeader.SubTotal [numeric]: SQL Server = 15900.2978, PostgreSQL = 15900.297816051844
Purchasing.PurchaseOrderHeader.RevisionNumber [numeric]: SQL Server = 4, PostgreSQL = 4.0830009970089731
Purchasing.ShipMethod.ShipRate [numeric]: SQL Server = 1.7500, PostgreSQL = 1.7500000000000000
Purchasing.ShipMethod.ShipBase [numeric]: SQL Server = 14.9580, PostgreSQL = 14.9580000000000000
Purchasing.Vendor.CreditRating [numeric]: SQL Server = 1, PostgreSQL = 1.3557692307692308
Sales.CreditCard.ExpYear [numeric]: SQL Server = 2006, PostgreSQL = 2006.5001046134532901
Sales.CreditCard.ExpMonth [numeric]: SQL Server = 6, PostgreSQL = 6.5273041113087143
Sales.CurrencyRate.AverageRate [numeric]: SQL Server = 79.2363, PostgreSQL = 79.2363002660360627
Sales.CurrencyRate.EndOfDayRate [numeric]: SQL Server = 79.2363, PostgreSQL = 79.2363360257168194
Sales.SalesOrderDetail.UnitPriceDiscount [numeric]: SQL Server = 0.0028, PostgreSQL = 0.00282606724531599034
Sales.SalesOrderDetail.LineTotal [numeric]: SQL Server = 905.449206, PostgreSQL = 905.4492066230454100
Sales.SalesOrderDetail.UnitPrice [numeric]: SQL Server = 465.0934, PostgreSQL = 465.0934956741429478
Sales.SalesOrderDetail.OrderQty [numeric]: SQL Server = 2, PostgreSQL = 2.2660797744751354
Sales.SalesOrderHeader.Status [numeric]: SQL Server = 5, PostgreSQL = 5.0000000000000000
Sales.SalesOrderHeader.TotalDue [numeric]: SQL Server = 3915.9951, PostgreSQL = 3915.9951093564277769
Sales.SalesOrderHeader.TaxAmt [numeric]: SQL Server = 323.7557, PostgreSQL = 323.7557432130939139
Sales.SalesOrderHeader.Freight [numeric]: SQL Server = 101.1736, PostgreSQL = 101.1736930494199905
Sales.SalesOrderHeader.SubTotal [numeric]: SQL Server = 3491.0656, PostgreSQL = 3491.0656730939138726
Sales.SalesOrderHeader.RevisionNumber [numeric]: SQL Server = 8, PostgreSQL = 8.0009534403305260
Sales.SalesPerson.SalesYTD [numeric]: SQL Server = 2133975.9943, PostgreSQL = 2133975.994317647059
Sales.SalesPerson.Bonus [numeric]: SQL Server = 2859.4117, PostgreSQL = 2859.4117647058823529
Sales.SalesPerson.SalesQuota [numeric]: SQL Server = 260714.2857, PostgreSQL = 260714.285714285714
Sales.SalesPerson.SalesLastYear [numeric]: SQL Server = 1393291.9779, PostgreSQL = 1393291.977905882353
Sales.SalesPerson.CommissionPct [numeric]: SQL Server = 0.0117, PostgreSQL = 0.01176470588235294118
Sales.SalesPersonQuotaHistory.SalesQuota [numeric]: SQL Server = 587202.4539, PostgreSQL = 587202.453987730061
Sales.SalesTaxRate.TaxType [numeric]: SQL Server = 2, PostgreSQL = 2.0000000000000000
Sales.SalesTaxRate.TaxRate [numeric]: SQL Server = 9.0913, PostgreSQL = 9.0913793103448276
Sales.SalesTerritory.SalesYTD [numeric]: SQL Server = 5275120.9953, PostgreSQL = 5275120.995340000000
Sales.SalesTerritory.CostLastYear [numeric]: SQL Server = 0.0000, PostgreSQL = 0E-20
Sales.SalesTerritory.CostYTD [numeric]: SQL Server = 0.0000, PostgreSQL = 0E-20
Sales.SalesTerritory.SalesLastYear [numeric]: SQL Server = 3271535.5435, PostgreSQL = 3271535.543530000000
Sales.SpecialOffer.DiscountPct [numeric]: SQL Server = 0.2200, PostgreSQL = 0.22000000000000000000
dbo.AWBuildVersion.SystemInformationID [numeric]: SQL Server = 1, PostgreSQL = 1.00000000000000000000
```
</details>

## Foreign Keys Failed to Add
All foreign keys added successfully.

## 🔧 Suggested Fixes (via Gemini)
Okay, let's analyze the provided data and suggest solutions for the database migration issues and mean mismatches.

**Understanding the Problem**

*   **Migration Errors (Not Provided):**  You haven't provided specific error messages, which makes it impossible to give precise instructions.  I'll assume these errors were related to data type conversions or constraints failing due to the mean mismatches.
*   **Mean Mismatches:** These are the core issue.  The data in your source database has a slightly different average (mean) value for specific columns compared to the target database *after* the migration. This usually indicates that something is happening during the migration process that is altering the data.  Common causes include:
    *   **Rounding Errors:** Data type conversions (e.g., from `float` to `int`) can cause rounding.
    *   **Data Truncation:**  If the target column is smaller than the source column, values may be truncated.
    *   **Implicit Conversions:** The database might be doing implicit type conversions that alter the value.
    *   **Default Values:** A default value may be being set, causing a slightly different mean
    *   **Missing Data:**  Null handling (or lack thereof) can affect means.  If `NULL` values are treated differently between the source and target, this will shift the average.
    *   **Precision Issues:** Different databases handle decimal precision differently.
    *   **Transformations:**  Accidental data transformations (e.g., multiplying by a factor, adding an offset) during migration.
*   **Row Mismatches (Empty):** The fact that there are no row mismatches suggests the migration mostly completed without rows being missing or completely failing.

**General Troubleshooting Steps**

1.  **Examine the Migration Script:** The first and most important step is to *carefully* review the migration script. Look for any explicit or implicit type conversions, calculations, or transformations that could be altering the data.  Pay close attention to:
    *   Data type mappings (e.g., what data type is being used for the source to the target column.)
    *   Any `CASE` statements or conditional logic.
    *   Any functions used to transform the data.
2.  **Isolate the Problem:** Try migrating a *smaller* subset of the data (e.g., just the `HumanResources.Department` table or just a few rows of the `Production.Product` table) to see if you can reproduce the mean mismatches in a more controlled environment. This can help narrow down the issue.
3.  **Source and Target Schema Definitions:**  Compare the table schemas (data types, lengths, constraints, nullability) of the source and target databases *exactly*.  Use a tool to script out the table definitions if necessary.  Look for subtle differences.
4.  **Null Value Handling:**  Make sure `NULL` values are being handled consistently.  A common issue is that a column that allows `NULL` in the source database is being populated with a default value (e.g., 0) in the target.
5.  **Character Set/Collation Issues:**  Though unlikely with purely numeric data, differences in character sets or collations *could* cause unexpected conversions if text data is involved anywhere in the process.

**Specific Fixes (Based on the Provided Data and Common Causes)**

The data suggests a tendency to shift the decimals higher and round up.

*   **Integer Columns with Decimal Means:** Many of the problematic columns (e.g., `DepartmentID`, `ReorderPoint`, `SafetyStockLevel`, `OrderQty`, `ExpYear`, etc.) appear to be *integer* types (based on the integer part of the "expected" value) in the source database, but the decimal portions of the calculated mean indicate decimal data is being calculated somewhere along the line. During the copy or migration, a float/decimal division or calculation is occurring, and the result is being converted to an integer, causing a change in the average.
    *   **Fix:** Ensure that any calculations during the migration script are performed using integer arithmetic *if* the target column is an integer. If decimal precision is important, the target column type may need to be changed to DECIMAL or NUMERIC with appropriate precision and scale.
*   **Precision Loss:** The columns `VacationHours`, `SickLeaveHours`, `BOMLevel`, `Status`, `Quantity`, `Bin`, `ScrappedQty`, `OperationSequence`, `LocationID`, `RevisionNumber`, `CreditRating`, `ExpMonth` appear to have decimals, meaning either a calculation is being performed or they are already float or double. To remedy this, you can increase the precision of columns to fit the decimal places, or round the decimal places to the nearest number during migration.
    *   **Fix:** Cast values to the correct precision, and check the means afterwards.
*   **Potential Calculation Errors:** Some of the mismatches (e.g., `ScrappedQty`, `OperationSequence`) seem small but consistent, which might point to a small error in a calculation being performed during the migration (perhaps a rounding error or an unintended offset).
    *   **Fix:** Review the migration script for calculations involving these columns and ensure the calculations are accurate and produce the expected results. If you are using a conversion function, ensure that it has the appropriate parameters for rounding and casting.

**Example Scenarios and Solutions in SQL (Illustrative)**

Let's say your migration script contains something like this:

```sql
-- Incorrect (Potential for rounding errors)
INSERT INTO TargetDB.HumanResources.Department (DepartmentID)
SELECT SourceDB.HumanResources.Department.DepartmentID * 1.1  -- Example calculation
FROM SourceDB.HumanResources.Department;

-- Incorrect
INSERT INTO TargetDB.Production.Product (SafetyStockLevel)
SELECT CAST(SourceDB.Production.Product.SafetyStockLevel AS INT)
FROM SourceDB.Production.Product;
```

**Corrected Versions:**

```sql
-- Assuming DepartmentID should be an INT, avoid calculations that produce decimals
INSERT INTO TargetDB.HumanResources.Department (DepartmentID)
SELECT SourceDB.HumanResources.Department.DepartmentID  -- Avoid the calculation if possible
FROM SourceDB.HumanResources.Department;

-- Ensure the column can hold the necessary digits.
ALTER TABLE TargetDB.Production.Product ALTER COLUMN SafetyStockLevel NUMERIC(18,2);

INSERT INTO TargetDB.Production.Product (SafetyStockLevel)
SELECT SourceDB.Production.Product.SafetyStockLevel
FROM SourceDB.Production.Product;
```

**Workflow and Verification**

1.  **Implement Fixes:**  Apply the appropriate fixes to your migration script.
2.  **Remigrate:** Run the migration again.
3.  **Verify:**  *After* the migration, *immediately* re-run the queries that identified the mean mismatches.  Compare the means in the target database with the expected values.  Also, re-run any data validation queries.

**Important Notes:**

*   **Data Integrity:**  Any data transformations must be carefully considered to ensure they do not compromise data integrity.  If you absolutely must perform calculations during migration, be very clear about the purpose and potential impact.
*   **Logging:** Implement detailed logging in your migration script to track data transformations, handle errors, and provide audit trails.  This makes debugging much easier.
*   **Testing:** Test the migration process thoroughly in a non-production environment *before* migrating your production database.
*   **Backup:** *Always* create a backup of your target database *before* running the migration.
*   **Data type precedence** Be aware of data type precedence when performing queries across multiple tables. This is important if a calculation is being performed using integers and decimals, as the integers will implicitly be converted to decimals.

By carefully reviewing your migration script, comparing schemas, handling null values correctly, and being mindful of potential data type conversions, you should be able to eliminate the mean mismatches and ensure a successful migration.  The key is to isolate the exact point where the data is being altered and correct the process there.