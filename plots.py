from azure.storage.blob import BlobServiceClient
import pandas as pd
import matplotlib.pyplot as plt


blob_account_name = "unilock"
blob_container_name = "rawdata"
blob_relative_path = "unilockrawdata_01-03-2024.csv"
storage_account_access_key = "2KCgLTUKPKWIqZhdU/rHvaKWjWF8QfNHrvcSaUDj7iV8Ty0F7tzMmLp3DuNrEcfpk8OyiybsObVJ+AStpCMMCg=="

blob_service_client = BlobServiceClient(account_url=f"https://{blob_account_name}.blob.core.windows.net",
                                        credential=storage_account_access_key)

with open(blob_relative_path, "wb") as my_blob:
    blob_client = blob_service_client.get_blob_client(container=blob_container_name, blob=blob_relative_path)
    download_stream = blob_client.download_blob()
    my_blob.write(download_stream.readall())

df = pd.read_csv(blob_relative_path, encoding='latin-1')
print(df)

df = df.dropna(axis=1, how='all')
print(df.columns)

#counts of transactions per company
plt.figure(figsize=(10, 6))
df['Company'].value_counts().plot(kind='bar')
plt.title('Transaction Counts per Company')
plt.xlabel('Company')
plt.ylabel('Count')
plt.xticks(rotation=45)
plt.show()

# Plot histograms for numerical variables
numerical_cols = df.select_dtypes(include=['float64', 'int64']).columns
plt.figure(figsize=(15, 10))
for i, col in enumerate(numerical_cols):
    plt.subplot(3, 3, i+1)
    plt.hist(df[col], bins=20, color='skyblue', edgecolor='black')
    plt.title(col)
plt.tight_layout()
plt.show()

# scatter plot to explore the relationship between two numerical variables
plt.figure(figsize=(8, 6))
plt.scatter(df['Amount'], df['Tax Amount'], alpha=0.5, color='orange')
plt.title('Scatter Plot: Amount vs. Tax Amount')
plt.xlabel('Amount')
plt.ylabel('Tax Amount')
plt.show()

# pie chart to visualize the distribution of transactions across revenue categories
plt.figure(figsize=(8, 8))
df['Revenue Category'].value_counts().plot(kind='pie', autopct='%1.1f%%', startangle=90)
plt.title('Distribution of Transactions Across Revenue Categories')
plt.ylabel('')
plt.show()
