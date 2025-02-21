from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("MongoDB_Spark") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1") \
    .config("spark.driver.memory", "4G")\
    .config("spark.executor.memory", "4G")\
    .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017") \
    .getOrCreate()

df = spark.read.format("mongodb") \
          .option("database", "scoring") \
          .option("collection", "cash_allocated_data") \
          .load() #loans_repayment

df = spark.read.format("mongodb") \
          .option("database", "scoring") \
          .option("collection", "cash_allocated_data") \
          .load() #loans_repayment

country_codes = {
        "Nano_Loan": 15,
        "Macro_Loan": 15,
        "Advanced_Credit": 15,
        "Cash_Roller_Over": 30
    }
individual_period = {
        "Refund_Bonus": 2.5,
        "Normal": 2,
        "Reconducted": 1.75,
        "Penalty": 0.3
    }
corporates_period = {
        "Refund_Bonus": 10,
        "Normal": 4,
        "Reconducted": 3.5,
        "Penalty": 0.6
    }

