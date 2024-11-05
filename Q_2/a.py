from pyspark.sql import SparkSession
from pyspark.sql.functions import floor, year, current_date, col, countDistinct, explode, split

def main():
    spark = SparkSession.builder.appName("Question2").getOrCreate() #type:ignore
    csv_file_path = "medical_records.csv"
    df = spark.read.option("header", True).csv(csv_file_path)
    df = df.withColumn("date_of_birth", col("date_of_birth").cast("date"))

    # 2_a Common medical condition between age group
    age_df = df.withColumn("age", year(current_date()) - year(col("date_of_birth")))
    age_df = age_df.withColumn("age_group", (floor(col("age") / 10) * 10).cast("int"))
    age_df = age_df.withColumn("medical_condition", split("medical_conditions", ', '))
    age_df = age_df.withColumn("medical_condition", explode("medical_condition"))
    most_common_condition = age_df.groupBy("medical_condition").count().orderBy(col("count").desc()).first()["medical_condition"]
    print(f"Most common medical condition: {most_common_condition}")
    result = age_df.filter(col("medical_condition") == most_common_condition) \
               .groupBy("age_group", "medical_condition").count().orderBy("age_group")
    result.show(truncate=False)
    output_path = "2_a.json"
    result.coalesce(1).write.format('json').save(output_path)

    # 2_b Number of unique types of allergies
    allergy_df = df.withColumn("allergy", explode(split(col("allergies"), ", ")))
    unique_allergies_count = allergy_df.select(countDistinct("allergy"))
    unique_allergies_count.show()
    output_path = "2_b.json"
    unique_allergies_count.coalesce(1).write.format('json').save(output_path)

    # 2_c Number of persons having surname “Davis”
    davis_count = df.select(col("name")).filter(col("name").contains("Davis")).groupBy().count()
    davis_count.show()
    output_path = "2_c.json"
    davis_count.coalesce(1).write.format('json').save(output_path)

    # 2_d The number of Male and Female Patients which are allergic to hospitals
    hospital_allergic_df = df.filter(col("allergies").contains("hospital"))
    gender_count = hospital_allergic_df.groupBy("gender").count()
    gender_count.show()
    output_path = "2_d.json"
    gender_count.coalesce(1).write.format('json').save(output_path)


if __name__ == "__main__":
    main()