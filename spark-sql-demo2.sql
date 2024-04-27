-- Databricks notebook source
cache lazy table fire_service_calls_tbl_cache as
select * from demo_db.fire_service_calls_tbl

-- COMMAND ----------

select count(*) from demo_db.fire_service_calls_tbl

-- COMMAND ----------

select * from demo_db.fire_service_calls_tbl limit 100

-- COMMAND ----------

-- MAGIC  %md
-- MAGIC ##### Q1. How many distinct types of calls were made to the Fire Department?

-- COMMAND ----------

select count(distinct callType) as distinct_call_type_count
from demo_db.fire_service_calls_tbl
where callType is not null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q2. What were distinct types of calls made to the Fire Department?

-- COMMAND ----------

select distinct callType as distinct_call_type
from demo_db.fire_service_calls_tbl
where callType is not null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q3. Find out all response for delayed times greater than 5 mins?

-- COMMAND ----------

select  CallNumber, Delay
from demo_db.fire_service_calls_tbl
where Delay > 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q4. What were the most common call types?

-- COMMAND ----------

select CallType 
from demo_db.fire_service_calls_tbl
where callType is not null
group by CallType
order by count(CallType) desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q5. What zip codes accounted for most common calls?

-- COMMAND ----------

select callType, zipCode, count(*) as count
from demo_db.fire_service_calls_tbl
where callType is not null
group by callType, zipCode
order by count desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q6. What San Francisco neighborhoods are in the zip codes 94102 and 94103?

-- COMMAND ----------

select Neighborhood, Zipcode
from demo_db.fire_service_calls_tbl
where City = 'San Francisco'
  and Zipcode in (94102,94103) ;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Q7. What was the sum of all call alarms, average, min, and max of the call response times?

-- COMMAND ----------

SELECT sum(NumAlarms), avg(Delay), min(Delay), max(Delay)
FROM demo_db.fire_service_calls_tbl

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q8. How many distinct years of data is in the data set?

-- COMMAND ----------

select distinct year(to_date(callDate, "yyyy-MM-dd")) as year_num
from demo_db.fire_service_calls_tbl
order by year_num

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q9. What week of the year in 2018 had the most fire calls?

-- COMMAND ----------

select weekofyear(to_date(callDate, "yyyy-MM-dd")) week_year, count(*) as count
from demo_db.fire_service_calls_tbl
where year(to_date(callDate, "yyyy-MM-dd")) == 2018
group by week_year
order by count desc

-- COMMAND ----------

-- MAGIC  %md
-- MAGIC ##### Q10. What neighborhoods in San Francisco had the worst response time in 2018?

-- COMMAND ----------

select Neighborhood, Delay
from demo_db.fire_service_calls_tbl
where City = 'San Francisco'
  and year(to_date(callDate, "yyyy-MM-dd")) == 2018
order by Delay desc

-- COMMAND ----------


