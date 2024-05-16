import sys

from pyspark.sql import *

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("HelloSparkSQL") \
        .getOrCreate()

    logger = Log4j(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    surveyDF = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/sample.csv")

    surveyDF.createOrReplaceTempView("survey_tbl")
    # surveyDF.show()
    # countDF = spark.sql("select Country, count(1) as count from survey_tbl where Age<40 group by Country")
    countDF = spark.sql("""
                select Country, count(1) as count
                from survey_tbl
                where Age<40
                group by Country
                """)
    countDF.show()