# Databricks notebook source
# MAGIC %md
# MAGIC ##Adidas sales data Analysis
# MAGIC

# COMMAND ----------

The objective of this project is to analyze the Adidas sales database for the year 2020 and 2021 and identify key insights to help improve sales performance and optimize business strategies.

By analyzing the sales data, we aim to understand factors influencing sales, identify trends, and uncover opportunities for growth. The analysis will be conducted using databricks Notebook to provide an interactive and insightful dashboard.

**Business Metrics requirements**

1. Total Sales, Total Profit, Average Price per Unit, and Total Units Sold
2. Total sales by month 
3. Total sales by state
4. total sales by region
5. Total sales by product 
6. Total sales by retailer
7. Units Sold by Product Category and Gender Type
8. Top Performing Cities by Profit**

# COMMAND ----------

data=spark.read.format('csv').load('/FileStore/tables/Adidas_US_Sales_Datasets.csv',inferSchema=True,header=True)
data.display()

# COMMAND ----------

data.createOrReplaceTempView('Sales_csv')

# COMMAND ----------

1. Total Sales, Total Profit, Average Price per Unit, and Total Units Sold


# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(`Total Sales`) as TotalSales, sum(`Operating Profit`) as Totalprofit, avg(`Price per Unit`) as AvgPricePerUnit,sum(`Units Sold`) as TotalUnitSold  from sales_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC select date_format(`Invoice Date`,'MMM')as Months,sum(`Total Sales`) as Total_Sales from sales_csv group by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(`Total Sales`) as Total_Sales,state from sales_csv group by 2
# MAGIC order by 1 desc limit 5
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select Region ,sum(`Total Sales`) as Total_Sales from sales_csv group by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select Product ,sum(`Total Sales`) as Total_Sales from sales_csv group by 1
# MAGIC order by 2 desc limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select Retailer ,sum(`Total Sales`) as Total_Sales from sales_csv group by 1
# MAGIC order by 2 desc limit 5
# MAGIC
# MAGIC

# COMMAND ----------

Women's Athletic Footwear
Men's Apparel

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(`Units Sold`) as Total_Sales,`Sales Method` from sales_csv group by 2
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select City ,sum(`Operating Profit`) as Total_Sales from sales_csv group by 1
# MAGIC order by 2 desc limit 5
# MAGIC
# MAGIC

# COMMAND ----------

