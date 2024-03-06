#!/usr/bin/env python
# coding: utf-8

# ## SDS_SGDE_POC_Unilock_LoadDemoDataToProcessedSocialAndGovernanceDataLakehouseTables_INTB
# 
# null

# ### Overview
# Demo data for social and governance sustainability area in the MCFS ESG data model schema is deployed along with the capability in your workspace under Files section in ConfigAndDemoData lake house. The demo data can be used for exploring the capability. 
# This notebook can be used for loading the demo data as tables in the ‘ProcessedSocialAndGovernanceData’ Lakehouse.
# 
# Note: -
# 1.	Demo data is for illustration purposes only. No real association is intended or inferred.
# 2.	[Caution] In case you have loaded certain ESG data model tables with your data in the ‘ProcessedSocialAndGovernanceData’ Lakehouse that overlap with the tables present in the demo data then the overlapping tables will be overwritten.
# 
# For more information [click here](https://aka.ms/social-governance-metrics-and-reports) to view Social and governance metrics and reports (preview) documentation. 

# ### Parameters
# 

# __SOURCE_FOLDER_ABFSS_PATH__ : ABFSS Path to the Lakehouse storing the demo data\
# __TARGET_LAKEHOUSE_NAME__ : Name of the Lakehouse to load demo data in processed tables 

# In[1]:


DEMO_DATA_FOLDER_NAME = 'DemoData'
SOURCE_FOLDER_FILE_API_PATH = f'/lakehouse/default/Files/{DEMO_DATA_FOLDER_NAME}'
SOURCE_FOLDER_ABFSS_PATH = f'abfss://c7b8b97a-3b10-4031-8b76-717a2a16065a@onelake.dfs.fabric.microsoft.com/9324fdce-fd92-49a2-ba25-3275ef268375/Files/{DEMO_DATA_FOLDER_NAME}'
TARGET_LAKEHOUSE_NAME = 'SDS_SGDE_POC_Unilock_ProcessedSocialAndGovernanceData_LH'


# Import required libraries and set spark configuration

# In[2]:


import os

spark.conf.set("spark.sql.caseSensitive", "true")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")


# ### Load tables to Fabric

# In[3]:


for table in os.listdir(SOURCE_FOLDER_FILE_API_PATH):
    tableFolderABFSSPath = os.path.join(SOURCE_FOLDER_ABFSS_PATH, table)
    df = spark.read.format("delta").load(tableFolderABFSSPath)
    df.write.format("delta").mode('overwrite').saveAsTable(TARGET_LAKEHOUSE_NAME + '.' + table)

