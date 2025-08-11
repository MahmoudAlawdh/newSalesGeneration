# Sales Data Generation Plan

## Objective

Our goal is to generate complete and reliable sales data for **all stores**. While some stores already have recorded sales figures, others have gaps or no sales at all. The generation process must account for **seasonal trends** by considering the following fields:

- **Sales_Month**
- **Sales_Year**
- **Location_Type**
- **Brand**
- **Product_Focus**
- **Industry_Level_2**

By leveraging these factors, we aim to produce realistic and seasonally consistent sales figures that can be confidently used for analysis, forecasting, and decision-making.

---

## Step 1 — Preparing the Sales Collection

Before generating or filling sales data, we must first ensure the sales collection is properly set up and contains a clean, complete foundation. This involves three sub-steps:

1. **Clear Existing Data**

   - Use the `new_sales_delete` function to remove all current data from the target sales collection.
   - This ensures we start with a clean slate, avoiding any legacy inconsistencies or duplicates.

2. **Copy Base Data**

   - Use the `copy_sales` function to import historical sales data from a source collection.
   - This step provides the initial dataset, which may already include complete data for some stores.

3. **Fill Missing Months**
   - Use the `fill_sales_gaps` function to ensure every store has continuous monthly records, even if some months have zero or missing sales.
   - This creates a time-consistent structure across all stores, preparing the dataset for sales generation.

---

## Step 2 — Generating Sales Data

Once the sales collection is structured and complete in terms of time coverage, we proceed to fill in the actual sales figures for missing data points.

1. **Fill Missing Sales Values**

   - Use the `fill_gaps` function to estimate and populate missing sales numbers between existing data points.
   - The estimation process should factor in **seasonality** (month and year), **location type**, **brand**, **product focus**, and **industry level** to generate values that are realistic and consistent with historical patterns.

2. **Validation & Quality Checks**
   - Review generated values to ensure they align with seasonal expectations.
   - Cross-check against known historical trends for accuracy and reliability.
