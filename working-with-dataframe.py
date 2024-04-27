# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

raw_fire_df = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

# COMMAND ----------

display(raw_fire_df)

# COMMAND ----------

renamed_fire_df = raw_fire_df \
    .withColumnRenamed("Call Number", "CallNumber") \
    .withColumnRenamed("Unit ID", "UnitID") \
    .withColumnRenamed("Incident Number", "IncidentNumber") \
    .withColumnRenamed("Call Date", "CallDate") \
    .withColumnRenamed("Watch Date", "WatchDate") \
    .withColumnRenamed("Call Final Disposition", "CallFinalDisposition") \
    .withColumnRenamed("Available DtTm", "AvailableDtTm") \
    .withColumnRenamed("Zipcode of Incident", "Zipcode") \
    .withColumnRenamed("Station Area", "StationArea") \
    .withColumnRenamed("Final Priority", "FinalPriority") \
    .withColumnRenamed("ALS Unit", "ALSUnit") \
    .withColumnRenamed("Call Type Group", "CallTypeGroup") \
    .withColumnRenamed("Unit sequence in call dispatch", "UnitSequenceInCallDispatch") \
    .withColumnRenamed("Fire Prevention District", "FirePreventionDistrict") \
    .withColumnRenamed("Supervisor District", "SupervisorDistrict")

# COMMAND ----------

display(renamed_fire_df)

# COMMAND ----------

renamed_fire_df.printSchema()

# COMMAND ----------

fire_df = renamed_fire_df \
    .withColumn("CallDate", to_date("CallDate", "MM/dd/yyyy")) \
    .withColumn("WatchDate", to_date("WatchDate", "MM/dd/yyyy")) \
    .withColumn("AvailableDtTm", to_timestamp("AvailableDtTm", "MM/dd/yyyy hh:mm:ss a")) \
    .withColumn("Delay", round("Delay", 2))    

# COMMAND ----------

display(fire_df)

# COMMAND ----------

fire_df.printSchema()

# COMMAND ----------

fire_df.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Q1. How many distinct types of calls were made to the Fire Department?
# MAGIC select count(distinct CallType) as distinct_call_type_count<br>
# MAGIC from fire_service_calls_tbl<br>
# MAGIC where CallType is not null<br>

# COMMAND ----------

fire_df.createOrReplaceTempView("fire_service_calls_view")
q1_sql_df = spark.sql("""
        select count(distinct CallType) as distinct_call_type_count
        from fire_service_calls_view
        where CallType is not null
        """)
display(q1_sql_df)

# COMMAND ----------

q1_df = fire_df.where("CallType is not null") \
            .select("CallType") \
            .distinct()
print(q1_df.count())   

# COMMAND ----------

# MAGIC %md
# MAGIC #####Q2. What were distinct types of calls made to the Fire Department?

# COMMAND ----------

display(q1_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q3. Find out all response for delayed times greater than 5 mins?

# COMMAND ----------

q3_df = fire_df.where("Delay > 5") \
            .select("CallNumber", "Delay") 
display(q3_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q4. What were the most common call types?

# COMMAND ----------

q4_df = fire_df.select("CallType") \
            .where("CallType is not null") \
            .groupBy("CallType") \
            .count() \
            .orderBy("count", ascending=False) \

display(q4_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q5. What zip codes accounted for most common calls?

# COMMAND ----------

q5_df = fire_df.where("CallType is not null") \
            .select("CallType", "ZipCode") \
            .groupBy("CallType", "ZipCode") \
            .count() \
            .orderBy("count", ascending=False) 

display(q5_df)
            
            

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q6. What San Francisco neighborhoods are in the zip codes 94102 and 94103?

# COMMAND ----------

q6_df = fire_df.where("City = 'San Francisco' and Zipcode in (94102,94103)") \
            .select("Neighborhood", "Zipcode")
display(q6_df)

# COMMAND ----------

result_df = (fire_df
             .agg(sum("NumAlarms").alias("TotalNumAlarms"),
                     avg("Delay").alias("AverageDelay"),
                     min("Delay").alias("MinimumDelay"),
                     max("Delay").alias("MaximumDelay"))
            )
display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q8. How many distinct years of data is in the data set?

# COMMAND ----------

year_df = (fire_df
           .select(year(col("callDate")).alias("year_num"))  # Extract year and alias as 'year_num'
           .distinct()  # Get distinct years
           .orderBy("year_num")  # Order by year
          )

# COMMAND ----------

display(year_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q9. What week of the year in 2018 had the most fire calls?

# COMMAND ----------

week_df = (fire_df
           .where(year(col("callDate")) == 2018)  # Correct filtering for the year 2018
           .select(weekofyear(col("callDate")).alias("week_year"))  # Extract week number and alias as 'week_year'
           .groupBy("week_year")  # Group by the extracted week number
           .count()  # Count the number of entries per week
           .orderBy("count", ascending=False)  # Order the results by count in descending order
          )
display(week_df)

# COMMAND ----------

# MAGIC  %md
# MAGIC ##### Q10. What neighborhoods in San Francisco had the worst response time in 2018?

# COMMAND ----------

q10_df = fire_df.where((col("City") == 'San Francisco') & (year(col("callDate")) == 2018)) \
            .select("Neighborhood", "Delay") \
            .orderBy("Delay", ascending=False)

display(q10_df)

# COMMAND ----------


