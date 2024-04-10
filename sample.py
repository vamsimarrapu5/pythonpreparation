from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from datetime import datetime, timedelta
import json

# Create a SparkSession
spark = SparkSession.builder \
    .appName("AzureEventHubToADLS") \
    .getOrCreate()

connectionString='

checkpoint_location = "/mnt/eventhubdlraw/checkpoint/"

eh_conf = {
    'eventhubs.connectionString': connectionString,
    'eventhubs.consumerGroup': '$Default',
    'eventhubs.maxEventsPerTrigger': 100,
    'eventhubs.startingPosition': f'{{"offset":"{startingOffset}","enqueuedTime":null,"seqNo":-1,"isInclusive":true}}'
}
eh_conf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
# Read streaming data from Azure Event Hubs
df = spark \
    .readStream \
    .format("eventhubs") \
    .options(**eh_conf) \
    .option("checkpointLocation", checkpoint_location) \
    .load()

df = df.withColumn("body", df["body"].cast("string"))

df = df.withColumn("enqueuedTime", expr("CAST(enqueuedTime AS TIMESTAMP)"))

azure_adls_output_path = "/mnt/eventhubdlraw/rawdata/"

def write_to_json_with_date(df, epoch_id):

    current_date = datetime.now().strftime("%Y-%m-%d")
    output_filename = f"{azure_adls_output_path}/data_{current_date}"
    df.write.mode("append").json(output_filename)

# Write data to JSON files with date in filename
query = df \
    .writeStream \
    .foreachBatch(write_to_json_with_date) \
    .outputMode("append") \
    .start()

# Wait for the termination of the query
query.awaitTermination()


from office365.sharepoint.client_context import ClientContext
import pandas as pd
from pyspark.sql.functions import col
from datetime import datetime

site_url = 

username = 
password = 
# Define the header mapping
header_mapping = {
    'Title': 'Project_Name',
    'AC': 'AC',
    'DC': 'DC',
    'Probability': 'Probability',
    'State': 'State',
    'PanelSize': 'Panel_Size',
    'Tilt_x002f_Fixed': 'Tilt_Fixed',
    'CODYear': 'COD_Year',
    'DeliveryStartDate': 'Delivery_StartDate',
    'DeliveryEndDate': 'Delivery_EndDate',
    'PW_x0020_DCMW': 'PW_DCMW',
    'Status': 'Status',
}

ctx = ClientContext(site_url).with_user_credentials(username, password)

# Retrieve data from "Scout Clean Energy" list
list_name1 = "Scout Clean Energy"
list_obj1 = ctx.web.lists.get_by_title(list_name1)
items1 = list_obj1.items.get().execute_query()

# Extract data and map headers for "Scout Clean Energy" list
data1 = []
for item in items1:
    row = {}
    for internal_name, display_name in header_mapping.items():
        row[display_name] = item.properties.get(internal_name, None)
    row['Company'] = list_name1
    data1.append(row)

# Convert data to DataFrame for "Scout Clean Energy" list
df1 = pd.DataFrame(data1)

# Retrieve data from "Standard Solar" list
list_name2 = "Standard Solar"
list_obj2 = ctx.web.lists.get_by_title(list_name2)
items2 = list_obj2.items.get().execute_query()

# Extract data and map headers for "Standard Solar" list
data2 = []
for item in items2:
    row = {}
    for internal_name, display_name in header_mapping.items():
        row[display_name] = item.properties.get(internal_name, None)
    row['Company'] = list_name2
    data2.append(row)

# Convert data to DataFrame for "Standard Solar" list
df2 = pd.DataFrame(data2)

# Concatenate DataFrames
df_merged = df1.append(df2, ignore_index=True)
df_merged['Probability'] = df_merged['Probability'] * 100
df_merged['Probability'] = df_merged['Probability'].astype(str) + '%'
df_merged['PW_DCMW'] = df_merged['PW_DCMW'].astype(float)
dffinal = spark.createDataFrame(df_merged)
dffinal = dffinal.select(
    col('Project_Name'),
    col('AC'),
    col('DC'),
    col('Probability'),
    col('State'),
    col('Panel_Size'),
    col('Tilt_Fixed'),
    col('COD_Year'),
    col('Delivery_StartDate').cast('timestamp').alias('Delivery_StartDate'),  # Convert to timestamp
    col('Delivery_EndDate').cast('timestamp').alias('Delivery_EndDate'),
    col('PW_DCMW'),
    col('Status'),
    col('Company')
)
display(dffinal)
dffinal.write.mode("overwrite").saveAsTable("procore.ProcurementWorkingGroup")
