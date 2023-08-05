import logging
import yaml
from pyspark.sql.functions import current_date, explode, from_json, split, col
from pyspark.sql import SparkSession
from utils import check_tables
from schemas.orders import Order, OrderLine
from schemas.customers import Customers, Companies
from schemas.products import Products
from schemas.industries import Industries
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('initial_data_loader')



def load_data(spark, entry, path, tables):

    schemas_dict = {'Customers': Customers().get_schema(),
        'Industries': Industries().get_schema(),
        'Products': Products().get_schema()}
    if entry[-3:] == 'csv':
        data_schema = schemas_dict[entry.split("/")[0]]
        logger.info(f"Loading {entry} with schema {data_schema}")
        df = spark.read.option('header', 'true').schema(data_schema).csv(path + entry)
        df = df.withColumn('ingest_date', current_date())
        if not 'Customers' in entry:
            df \
                .write \
                .format('iceberg') \
                .mode('append') \
                .insertInto(f"{tables[0]['table']['schema']}.{tables[0]['table']['name']}")
        else:
            df \
                .select('customer_id', 'company_name', 'ingest_date') \
                .write \
                .format('iceberg') \
                .mode('append') \
                .insertInto(f"{tables[0]['table']['schema']}.{tables[0]['table']['name']}")

            df \
                .select('company_name', 'specialized_industries') \
                .withColumn('industry', explode(split(col('specialized_industries'), ';'))) \
                .join(
                    df.filter(col('specialized_industries').isNull()) \
                        .selectExpr('company_name','specialized_industries as industry_'), on='company_name', how='outer') \
                .select('company_name', 'industry') \
                .distinct() \
                .withColumn('ingest_date', current_date()) \
                .write \
                .format('iceberg') \
                .mode('append') \
                .insertInto(f"{tables[1]['table']['schema']}.{tables[1]['table']['name']}")

        
    elif entry[-4:] == 'json':
        data_schema = Order().get_schema()
        logger.info(f"Loading {entry} with schema {data_schema}")
        df = spark.read.schema(data_schema).json(path + entry)

        df \
            .selectExpr("order_id", "customer_id", 'amount', "timestamp", "YEAR(timestamp) AS year", "MONTH(timestamp) AS month", "DAY(timestamp) AS day") \
            .write \
            .format('iceberg') \
            .mode('append') \
            .partitionBy('year', 'month', 'day') \
            .saveAsTable(f"{tables[0]['table']['schema']}.{tables[0]['table']['name']}")
        
        df \
            .withColumn('order_line', explode(df.order_lines)) \
            .selectExpr('order_id', 'order_line.product_id', 'order_line.volume', 'order_line.price', "timestamp", "YEAR(timestamp) AS year", "MONTH(timestamp) AS month", "DAY(timestamp) AS day") \
            .write \
            .format('iceberg') \
            .mode('append') \
            .partitionBy('year', 'month', 'day') \
            .saveAsTable(f"{tables[1]['table']['schema']}.{tables[1]['table']['name']}")

def run():
    path = 's3a://demo-data/'
    schemas_path = '/home/iceberg/spark-apps/schemas/'
    files_list = ['Customers/Customers.csv', 'Industries/Industries.csv', 'Orders/orders.json', 'Products/Products.csv']

    logger.info("Creating required tables...")

    spark = SparkSession.builder \
        .appName('inital_loader_app') \
        .master("local[*]") \
        .config("spark.log.level", "WARN") \
        .getOrCreate()

    for entry in files_list:
        table_schemas = yaml.safe_load_all(open(schemas_path + entry.split('/')[0].lower() + '.yaml'))
        tables_list = [i for i in table_schemas]
        check_tables(spark, tables_list, True)
        load_data(spark, entry, path, tables_list)



if __name__ == '__main__':
    run()