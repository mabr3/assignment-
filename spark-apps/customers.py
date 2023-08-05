import yaml
import logging
from schemas.customers import Companies, Customers
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, current_date, split, collect_list, col
from utils import check_tables
import logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('customer_file_stream')



def run():
    logger.info("Starting...")

    spark = SparkSession.builder \
        .appName('costumers_consumer') \
        .master("local[*]") \
        .config("spark.log.level", "WARN") \
        .getOrCreate()

    logger.info("Creating required tables...")
    table_schemas = yaml.safe_load_all(open('/home/iceberg/spark-apps/' + '/schemas/' + 'customers' + '.yaml','r'))
    tables_list = [i for i in table_schemas]
    check_tables(spark, tables_list)

    ssc = StreamingContext(spark.sparkContext, 60)

    def process_customer_csv(rdd):
        customer_schema = Customers().get_schema()
        if not rdd.isEmpty():
            df = spark.read.option('header', 'true').schema(customer_schema).csv(rdd)
            
            # Process customers and companies
            df_customers = df.select('customer_id', 'company_name').withColumn('ingest_date', current_date())
            df_customers.createOrReplaceTempView('customers_view')
            spark.sql("""
            MERGE INTO assignment_db.customers a
            USING (SELECT * FROM customers_view) b
            ON a.customer_id = b.customer_id
            WHEN MATCHED AND a.company_name != b.company_name THEN UPDATE SET a.company_name = b.company_name, a.ingest_date = b.ingest_date
            WHEN NOT MATCHED THEN INSERT *
            """)
            df_companies = df.withColumn("industry", explode(split(col("specialized_industries"), ";"))) \
                .select('company_name','industry')
            existing_data_df = spark.read.format('iceberg').load('assignment_db.companies') \
                .select('company_name', 'industry')
            
            df_union = existing_data_df \
                .union(df_companies) \
                .distinct() \
                .groupBy('company_name') \
                .agg(collect_list('industry').alias('industries')) \
                .select('company_name', 'industries')
            
            df_union \
                .withColumn('industry', explode('industries')) \
                .selectExpr('company_name','industry') \
                .withColumn('ingest_date', current_date())\
                .write \
                .mode('overwrite') \
                .format('iceberg') \
                .insertInto('assignment_db.companies')


    files_path = "s3a://demo-data/Customers/"
    lines = ssc.textFileStream(files_path)
    lines.foreachRDD(process_customer_csv)

    ssc.start()
    ssc.awaitTermination()

    


if __name__ == '__main__':
    run()
