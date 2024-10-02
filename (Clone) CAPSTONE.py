# Databricks notebook source
aws_keys_df = (spark.read
               .format("csv")
               .options(header = True, inferschema = True)
               .load("/FileStore/databricks_capstone_accessKeys.csv")
               )

aws_keys_df.columns

# COMMAND ----------

access_key = aws_keys_df.select('Access key ID').take(1)[0]['Access key ID']
secret_key = aws_keys_df.select('Secret access key').take(1)[0]['Secret access key']

# COMMAND ----------

import urllib

encoded_secret_key = urllib.parse.quote(string = secret_key, safe="")

# COMMAND ----------


AWS_S3_BUCKET = "rohit-bucket-databricks-capstone"
MOUNT_NAME = '/mnt/bucket_s3'
SOURCE_URL = "s3a://%s:%s@%s" %(access_key, encoded_secret_key, AWS_S3_BUCKET)

# COMMAND ----------

dbutils.fs.mount(
    source="s3a://%s:%s@%s" %(access_key, encoded_secret_key, AWS_S3_BUCKET),
    mount_point="/mnt/bucket_s3_2",
    extra_configs={"fs.s3a.aws.credentials.provider": "com.databricks.backend.daemon.driver.AwsInstanceProfileCredentialsProvider"}
)


# COMMAND ----------

# MAGIC %fs ls '/mnt/bucket_s3_2'

# COMMAND ----------

crimes_data = spark.read.csv(header = True, inferSchema = True, path = "dbfs:/mnt/bucket_s3/Crimes_-_2022_20240930.csv")

display(crimes_data)

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *

df = spark.read.csv("dbfs:/FileStore/Crimes___2022_20240929.csv", header=True, inferSchema=True)
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("Case Number", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Block", StringType(), True),
    StructField("IUCR", StringType(), True),
    StructField("Primary Type", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Location Description", StringType(), True),
    StructField("Arrest", BooleanType(), True),
    StructField("Domestic", BooleanType(), True),
    StructField("Beat", IntegerType(), True),
    StructField("District", IntegerType(), True),
    StructField("Ward", IntegerType(), True),
    StructField("Community Area", IntegerType(), True),
    StructField("FBI Code", StringType(), True),
    StructField("X Coordinate", IntegerType(), True),
    StructField("Y Coordinate", IntegerType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Updated On", StringType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("Location", StringType(), True)
])

streaming_df = (
    spark.readStream
    .option("header", "true")
    .schema(schema)
    .csv("dbfs:/FileStore/Crimes___2022_20240929.csv")
)
display(streaming_df)


# COMMAND ----------

df_silver = df.drop("Latitude", "Longitude", "X Coordinate", "Y Coordinate")
df_silver = df_silver.na.drop()
display(df_silver)

# COMMAND ----------


silver_streaming_df = sdf.drop("Latitude", "Longitude", "X Coordinate", "Y Coordinate")

silver_streaming_df = silver_streaming_df.na.drop()


output_path = "dbfs:/FileStore/silver_streaming_output"  
query = (
    silver_streaming_df.writeStream
    .format("delta")  
    .outputMode("append")  
    .option("checkpointLocation", "dbfs:/FileStore/checkpoints/silver_streaming")  
    .start(output_path)
)

query.awaitTermination()


# COMMAND ----------

Gold_df_counts = df_silver.groupBy("Primary Type").count().alias("Crime Type")

display(Gold_df_counts)

# COMMAND ----------

arrest_counts_df = (
    df_silver.groupBy("Arrest")
    .count()
    .withColumnRenamed("count", "Crimes_reported")
    .withColumn("Arrest_Status", F.when(F.col("Arrest") == True, "Arrested").otherwise("Not Arrested"))
    .drop("Arrest")
)

display(arrest_counts_df)

# COMMAND ----------

crimes_per_district_df = (
    df_silver
    .groupBy("District")
    .count()
    .withColumnRenamed("count", "Total_Crimes_Committed")
    .orderBy("District")
)

display(crimes_per_district_df)

# COMMAND ----------

from pyspark.sql import functions as F

location_df = (
    df_silver
    .withColumn("Latitude", F.regexp_extract(F.col("Location"), r"\(([^,]+),", 1).cast("double"))
    .withColumn("Longitude", F.regexp_extract(F.col("Location"), r",\s*([^)]+)\)", 1).cast("double"))
)

grouped_location_df = (
    location_df
    .groupBy("Latitude", "Longitude")
    .count()
    .withColumnRenamed("count", "Crime_Count")
    .filter(F.col("Latitude").isNotNull() & F.col("Longitude").isNotNull())
)

display(grouped_location_df)

