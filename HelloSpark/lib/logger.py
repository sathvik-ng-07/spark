class Log4j(object):
    """
    This class encapsulates the Log4j logging functionality within a PySpark application,
    allowing Python code to log messages via the Java-based Log4j logging system.
    """

    def __init__(self, spark):
        """
        Initializes a Log4j logger for a given Spark session.

        Args:
        spark (SparkSession): The SparkSession object from which to obtain the Spark context
                               and application name for logging purposes.

        The logger will be uniquely identified by a combination of a predefined root class
        and the specific Spark application name, enabling differentiated logging across
        multiple applications.
        """
        # Base package name to differentiate logs in a multi-application environment
        root_class = "demo.sathvikng.spark.examples"

        # Retrieve the Spark application's configuration
        conf = spark.sparkContext.getConf()

        # Obtain the application name from Spark's configuration to uniquely identify the logs
        app_name = conf.get("spark.app.name")

        # Access the JVM's Log4j logging system from the PySpark context
        log4j = spark._jvm.org.apache.log4j

        # Initialize the Log4j logger with a unique name combining root class and application name
        self.logger = log4j.LogManager.getLogger(root_class + "." + app_name)

    def warn(self, message):
        """
        Logs a warning message.

        Args:
        message (str): The message to log as a warning.
        """
        self.logger.warn(message)

    def info(self, message):
        """
        Logs an informational message.

        Args:
        message (str): The message to log as informational.
        """
        self.logger.info(message)

    def error(self, message):
        """
        Logs an error message.

        Args:
        message (str): The message to log as an error.
        """
        self.logger.error(message)

    def debug(self, message):
        """
        Logs a debug message.

        Args:
        message (str): The message to log for debugging purposes.
        """
        self.logger.debug(message)



