#!/usr/bin/env python
# coding: utf-8

# ## Notebook 1
# 
# New notebook

# In[6]:


from pyspark.sql import SparkSession

# Azure Blob Storage access info
blob_account_name = "unilock"
blob_container_name = "rawdata"
blob_relative_path = "unilockrawdata_01-03-2024.csv"
storage_account_access_key = "2KCgLTUKPKWIqZhdU/rHvaKWjWF8QfNHrvcSaUDj7iV8Ty0F7tzMmLp3DuNrEcfpk8OyiybsObVJ+AStpCMMCg=="

# Construct connection path
wasbs_path = f'wasbs://{blob_container_name}@{blob_account_name}.blob.core.windows.net/{blob_relative_path}'

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Read CSV from Blob Storage") \
    .config("fs.azure.account.key." + blob_account_name + ".blob.core.windows.net", storage_account_access_key) \
    .getOrCreate()

# Read CSV data from Azure Blob Storage path
blob_df = spark.read.csv(wasbs_path, header=True, inferSchema=True)
filename="procorement"
parquet_output_path = f"abfss://unilock_workspace@onelake.dfs.fabric.microsoft.com/rawdata.Lakehouse/Files/unilock_rawdata/{filename}"

blob_df.write.mode("overwrite").parquet(parquet_output_path)


# In[7]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Read Parquet and Save as Delta") \
    .getOrCreate()

# Read the parquet data from the specified path
raw_df = spark.read.parquet(parquet_output_path)

# Clean column names
cleaned_column_names = [name.replace(" ", "_").replace(".", "_").replace("-", "_") for name in raw_df.columns]
raw_df = raw_df.toDF(*cleaned_column_names)
raw_df.printSchema()
# Enable column mapping on Delta table with mapping mode set to 'name'
table_name = "Procurement_data"
raw_df.write.mode("overwrite").format("delta").saveAsTable(table_name)
display(raw_df)

