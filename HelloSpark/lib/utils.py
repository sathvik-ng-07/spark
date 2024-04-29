# Import the configparser module to read configuration files
import configparser

# Import SparkConf from pyspark to allow configuration of Spark settings
from pyspark import SparkConf

def load_survey_df(spark, data_file):
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(data_file)

def count_by_country(survey_df):
    return survey_df.filter("Age < 40") \
        .select("Age", "Gender", "Country", "state") \
        .groupBy("Country") \
        .count()


def get_spark_app_config():
    # Create a SparkConf object to hold configuration settings for the Spark application
    spark_conf = SparkConf()

    # Create a ConfigParser object to parse configuration files
    config = configparser.ConfigParser()

    # Read the configuration file named 'spark.conf'
    config.read("spark.conf")

    # Iterate over each configuration item in the section "SPARK_APP_CONFIGS" of the config file
    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        # For each configuration item, set it in the SparkConf object
        # 'key' is the name of the configuration property, 'val' is its value
        spark_conf.set(key, val)

    # Return the configured SparkConf object, ready to be used to configure a Spark session
    return spark_conf
