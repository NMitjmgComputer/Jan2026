# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "20142203-8bc5-48f8-ac0c-543ccf602749",
# META       "default_lakehouse_name": "LH_Jan2026",
# META       "default_lakehouse_workspace_id": "bd135b62-ea81-4df6-9212-d5905eb9b811",
# META       "known_lakehouses": [
# META         {
# META           "id": "20142203-8bc5-48f8-ac0c-543ccf602749"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType

# define the schema
schema = StructType() \
.add("ProductID", IntegerType(), True) \
.add("ProductName", StringType(), True) \
.add("Category", StringType(), True) \
.add("ListPrice", DoubleType(), True)

df = spark.read.format("csv").option("header","true").schema(schema).load("Files/products/products.csv")
# df now is a Spark DataFrame containing CSV data from "Files/products/products.csv".
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta").saveAsTable("external_products", path="abfss://bd135b62-ea81-4df6-9212-d5905eb9b811@onelake.dfs.fabric.microsoft.com/20142203-8bc5-48f8-ac0c-543ccf602749/Files/external_products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE FORMATTED sales;
# MAGIC DESCRIBE FORMATTED external_products;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
