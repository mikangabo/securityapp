from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

def read_sql():
    jdbc_path = r"C:\\Users\\ngabom\\Documents\\BI-PLSQL\\EVA_keys\\presto-jdbc-0.287.jar"
    truststore_path = r"C:\\Users\\ngabom\\Documents\\keystore-2024-25.jks"
    conf = SparkConf(loadDefaults=True) \
    .setAppName("NBX Data Query and Processing") \
    .set("spark.master", "local") \
    .set("spark.executor.memory", "2g") \
    .set("spark.driver.memory", "2g") \
    .set("spark.sql.session.timeZone", "UTC") \
    .set("spark.jars", jdbc_path)

    session= SparkSession.builder.config(conf=conf).getOrCreate()
    connection_properties = {
        "user": "",
        "password": "",
        "SSL": "true",
        "SSLTrustStorePath":  truststore_path,  # Update with your truststore path
        "SSLTrustStorePassword": "enzoslR555$"  # Update with your truststore password
    }
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
    df = session.read.format("jdbc").options(
    jdbc_url = "jdbc:presto://master01002.mtn.co.rw:8443/hive/rw",
    dbtable=f"({query}) AS query_result",
    user="",
    password="",
    partitionColumn="id",
    lowerBound=1,
    upperBound=1000,
    numPartitions=10,
    **connection_properties
    ).load()
    return df

if __name__== "__main__":
    df=read_sql()
    df.head()


