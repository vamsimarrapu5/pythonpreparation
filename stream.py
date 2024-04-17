from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("RealTimeProcessing") \
    .getOrCreate()

# Define Kafka parameters
kafka_params = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "input_topic",
    "startingOffsets": "latest"
}

# Read data from Kafka
kafka_stream_df = spark \
    .readStream \
    .format("kafka") \
    .options(**kafka_params) \
    .load()

# Convert the value column to string
kafka_stream_df = kafka_stream_df.selectExpr("CAST(value AS STRING)")

# Split the input message into words
words_df = kafka_stream_df.select(
    explode(split(kafka_stream_df.value, " ")).alias("word")
)

# Perform word count
word_count_df = words_df.groupBy("word").count()

# Define database connection properties
db_properties = {
    "user": "username",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# Write the word count results to a database table
query = word_count_df \
    .writeStream \
    .foreachBatch(lambda batch_df, batch_id: batch_df.write.jdbc(
        url="jdbc:postgresql://localhost:5432/database_name",
        table="word_count_results",
        mode="append",
        properties=db_properties
    )) \
    .start()

# Await termination
query.awaitTermination()
