from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import (
    col, min as spark_min, max as spark_max, 
    sum as spark_sum, count, when, date_format, to_date
)
from pyspark.sql.types import IntegerType, StringType

def get_data_spark():
    # Initialize Spark session
    conf = SparkConf() \
    .setAppName("NBX Data Query and Processing") \
    .set("spark.master", "local[*]") \
    .set("spark.executor.memory", "2g") \
    .set("spark.driver.memory", "2g") \
    .set("spark.sql.session.timeZone", "UTC") \
    .set("spark.jars", "C:\\Users\\ngabom\\Documents\\BI-PLSQL\\EVA_keys\\presto-jdbc-0.287.jar")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    # Presto JDBC URL and properties (Update this with proper configuration)
    jdbc_url = "jdbc:presto://master01002.mtn.co.rw:8443/hive/rw"
    connection_properties = {
        "user": "",
        "password": "",
        "SSL": "true",
        "SSLTrustStorePath": "C:\\Users\\ngabom\\Documents\\keystore-2024-25.jks",  # Update with your truststore path
        "SSLTrustStorePassword": "enzoslR555$"  # Update with your truststore password
    }

    # SQL query
    query = """
        WITH filtered_data AS (
            SELECT 
                a_number, 
                b_number, 
                usage_type_descr_txt, 
                call_duration, 
                record_type,
                CAST(CONCAT(SUBSTR(date_key, 1, 4), '-', SUBSTR(date_key, 5, 2), '-', SUBSTR(date_key, 7, 2), ' ', 
                SUBSTR(date_key, 9, 2), ':', SUBSTR(date_key, 11, 2), ':', SUBSTR(date_key, 13, 2)) AS TIMESTAMP) AS connection_date,
                region, district, sector
            FROM hive.rw.lea_mart
            WHERE tbl_dt BETWEEN 20241001 AND 20241231
              AND msisdn_key IN (
                250788607516, 250788475166, 250788715455, 250788818194, 250786423630, 250788475607,
                250785357927, 250788645663, 250783181338, 250788559185, 250791452504
              )
              AND record_type = 'MO'
        )
        SELECT 
            a_number,
            b_number,
            MIN(connection_date) AS first_connection_date,
            MAX(connection_date) AS last_connection_date,
            SUM(CASE WHEN usage_type_descr_txt = 'VOICE' THEN call_duration ELSE 0 END) AS total_voice_call_duration,
            COUNT(CASE WHEN usage_type_descr_txt = 'SMS' THEN call_duration END) AS total_sms_call_duration,
            region, district, sector
        FROM filtered_data
        GROUP BY a_number, b_number, region, district, sector
        limit 4
    """

    # Read data using Spark's JDBC connector
    df = spark.read.format("jdbc").options(
        url=jdbc_url,
        dbtable=f"({query}) AS query_result",
        **connection_properties
    ).load()

    return df

def process_data_spark():
    # Fetch query results
    df = get_data_spark()

    # Declare the last day in the dataset as the current date
    day = "2024-12-31"

    # Convert connection_date to date type
    df = df.withColumn("last_connection_date", to_date(col("last_connection_date")))

    # Create Recency feature
    df = df.withColumn("Recency", (to_date(col(day)) - col("last_connection_date")).cast(IntegerType()))

    # Create Frequency feature
    frequency_df = df.groupBy("a_number", "b_number").count()
    df = df.join(frequency_df, on=["a_number", "b_number"], how="left")

    # Create Monetary feature
    sec_price = 0.7
    sms_price = 12
    df = df.withColumn(
        "Monetary", 
        (col("total_voice_call_duration") * sec_price) + (col("total_sms_call_duration") * sms_price)
    )

    return df

if __name__ == "__main__":
    processed_data = process_data_spark()
    processed_data.show(truncate=False)