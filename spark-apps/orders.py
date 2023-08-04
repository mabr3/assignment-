import yaml
import os
import logging
from schemas.orders import Order, OrderLine
from utils import check_tables
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, explode
from yaml.loader import SafeLoader


logging.basicConfig(level=logging.WARN,
                    format='%(asctime)s - %(name)s - %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('orders_consumer')
logging.getLogger().setLevel(logging.WARNING)

def process_batch(df, epoch_id, topic):
    df.persist()
    store_raw(df, epoch_id, topic)
    store_silver(df, epoch_id)
    df.unpersist()


def store_raw(df, epoch_id, topic):
    raw_df = df.selectExpr('offset', 'value', 'timestamp.to_date() AS ingest_date')
    raw_df.write.option("compression", "gzip").partitionBy('ingest_date').mode('append').json(f"s3a://raw-data/{topic}/")

def store_silver(df, epoch_id):
    df_parsed = df \
        .selectExpr("CAST(value AS string)") \
        .withColumn('parsed_value', from_json('value', Order().get_schema())) \
        .selectExpr("parsed_value.*")
   
    # Store orders
    df_parsed_orders = df_parsed \
        .selectExpr("order_id", "customer_id", 'amount', "timestamp", "YEAR(timestamp) AS year", "MONTH(timestamp) AS month", "DAY(timestamp) AS day") \
        .write \
        .format('iceberg') \
        .mode('append') \
        .partitionBy("year", "month", "day") \
        .saveAsTable('assignment_db.orders')
    
    # Store order lines
    df_parsed_order_lines = df_parsed \
        .withColumn('order_line', explode(df_parsed.order_lines)) \
        .selectExpr('order_id',
                    'order_line.product_id',
                    'order_line.volume', 
                    'order_line.price',
                    'timestamp',
                    'YEAR(timestamp) AS year', 
                    'MONTH(timestamp) AS month', 
                    'DAY(timestamp) AS day') \
        .write \
        .format('iceberg') \
        .mode('append') \
        .partitionBy('year', 'month', 'day') \
        .saveAsTable('assignment_db.order_lines')

def run():
    logger.info("Starting...")

    spark = SparkSession.builder \
        .appName('orders_consumer') \
        .master("local[*]") \
        .config("spark.log.level", "WARN") \
        .getOrCreate()

    
    logger.info("Creating required tables...")

    kafka_topic = 'orders'
    kafka_host = 'kafka:9092'
    table_schemas = yaml.safe_load_all(open('/home/iceberg/spark-apps/' + '/schemas/' + kafka_topic + '.yaml','r'))
    tables_list = [i for i in table_schemas]
    check_tables(spark, tables_list)

    logger.info("Starting Kafka stream on {kafka_topic}, on topic {kafka_host}")

    stream_kafka_orders = (spark
                       .readStream
                       .format("kafka")
                       .option("subscribe", "orders")
                       .option("kafka.bootstrap.servers", "kafka:9092")
                       .option("startingOffsets", "earliest")
                       .option("failOnDataLoss", "true")
                       .option("group.id", "OrderConsumer")
                       .load())
    
    consumer_stream = stream_kafka_orders \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(store_silver) \
        .option("checkpointLocation", "s3a://checkpoints/") \
        .start()

    consumer_stream.awaitTermination()



if __name__ == '__main__':
    run()
