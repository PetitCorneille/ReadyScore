from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Initialisation de Spark
spark = SparkSession.builder.appName("KafkaMongoLoansStream").getOrCreate()

# Définition du schéma JSON (correspondant au schéma MongoDB)
schema = StructType([
    StructField("productFamily", StringType()),
    StructField("status", StringType()),
    StructField("productSubCategory", StringType()), 
    StructField("pScore", DoubleType()),
    StructField("category", StringType()),
    StructField("segment", StringType()),
    StructField("numberSimCard", StringType()),
    StructField("refAmount", DoubleType()), 
    StructField("loan", DoubleType()), 
    StructField("operator", StringType()),
    StructField("insert_date", TimestampType()) 
])
# Lecture en streaming depuis Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "loans_topic") \
    .load()

# Transformer les données JSON en DataFrame
df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


'''La partie à rediger'''

# Afficher les données en streaming sur la console
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
