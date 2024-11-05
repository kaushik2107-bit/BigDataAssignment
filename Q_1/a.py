from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder.appName("Question1").getOrCreate() #type: ignore
    json_file_path = "iot_devices.json"
    df = spark.read.json(json_file_path)

    # 1_a Sort by location with count of readings
    location = df.groupBy("cn").count().orderBy(col("count").desc()) 
    location.show()
    output_path = "1_a.json"
    location.coalesce(1).write.format('json').save(output_path)

    # 1_b Most readings showw
    location_most = location.limit(1)
    location_most.show()
    output_path = "1_b.json"
    location_most.coalesce(1).write.format('json').save(output_path)

    # 1_c 
    start_timestamp = 1458444054122
    end_timestamp = 1458444060000

    co2_filter = df.filter(
        (col("c02_level") > 1400) & 
        (col("timestamp") >= start_timestamp) & 
        (col("timestamp") <= end_timestamp)
    ).groupBy("cn").count().filter(
        col("count") > 10
    ).orderBy(col("count").desc())
    co2_filter.show()
    output_path = "1_c.json"
    co2_filter.coalesce(1).write.format('json').save(output_path)

    spark.stop()

if __name__ == "__main__":
    main()