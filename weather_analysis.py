import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import min, max, avg


# spark-submit  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 weather_analysis.py
mongo_URI = "mongodb+srv://sluser:admin@cluster0.pxn7mv1.mongodb.net/?retryWrites=true&w=majority"

spark = SparkSession.builder.appName("UK Weather Analysis").getOrCreate()

schema = StructType([StructField("Year", IntegerType(), False),
                    StructField("Month", IntegerType(), False), StructField("T_Max", FloatType(), True), StructField("T_Min", FloatType(), True), StructField("AF", FloatType(), True), StructField("Rain", FloatType(), True), StructField("Sun", FloatType(), True), StructField("Station", StringType(), True)])

df = spark.readStream.schema(
    schema).csv("./weather_data")

filtered_df = df.na.drop().filter(df.Year >= 2000)

# Alternative method to use Spark SQL to obtain the station count
# filtered_df.createOrReplaceTempView("filtered_df")
# station_count = spark.sql(
#    "SELECT Station, COUNT(*) AS Station_Count FROM filtered_df GROUP BY Station")

station_count = filtered_df.groupby("Station").count()
station_count.writeStream.format("mongodb").option("spark.mongodb.connection.uri", mongo_URI).option(
    "spark.mongodb.database", "weather_statistics").option("spark.mongodb.collection", "Station_Count").option("checkpointLocation", "Station_Count").outputMode("complete").start()

group_fields = ["Station", "Year"]

for field in group_fields:
    data = filtered_df.groupby(field).agg(max("T_Max").alias("Max T_Max"), min("T_Max").alias(
        "Min T_Max"),  avg("T_Max").alias("AVG T_Max"), max("T_Min").alias("Max T_Min"), min("T_Min").alias("Min T_Min"), avg("T_Min").alias("AVG T_Min"), max("Rain").alias("Max Rain"), min("Rain").alias(
        "Min Rain"),  avg("Rain").alias("AVG Rain"), max("Sun").alias("Max Sun"), min("Sun").alias(
        "Min Sun"),  avg("Sun").alias("AVG Sun"))

    collection_name = field + "_Data"
    data.writeStream.format("mongodb").option("spark.mongodb.connection.uri", mongo_URI).option(
        "spark.mongodb.database", "weather_statistics").option("spark.mongodb.collection", collection_name).option("checkpointLocation", collection_name).outputMode("complete").start()

spark.streams.awaitAnyTermination()
