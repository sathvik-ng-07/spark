import sys

from pyspark.sql import *

from lib.utils import get_spark_app_config, load_survey_df, count_by_country
from lib.logger import Log4j

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
                .config(conf = conf) \
                .getOrCreate()

    logger = Log4j(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    logger.info("Starting HelloSpark")

    survey_raw_df = load_survey_df(spark, sys.argv[1])
    partitioned_survey_df = survey_raw_df.repartition(2)
    count_df = count_by_country(partitioned_survey_df)

    logger.info(count_df.collect())

    input("Press Enter")

    # conf_out = spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())
    logger.info("Finished HelloSpark")