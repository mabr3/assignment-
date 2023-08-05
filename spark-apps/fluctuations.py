import yaml
import logging
import argparse
import datetime
from utils import check_tables
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, broadcast, split, col, desc, avg, sum as _sum, abs as _abs, row_number, lit
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('daily_aggregator')


def format_print_diff(row):
    industry = row['industry']
    value_24h = row['24h_value']
    value_30d = row['30d_avg']
    difference = row['diff']
    percent_variation = (difference / value_30d) * 100
    rank = row['rank']
    
    str_print = f"Rank: {rank} -> Industry: {industry:<15} | 24h_value: {value_24h:<15.2f} | 30d_avg: {value_30d:<15.2f} | Difference: {difference:<15.2f} | % variation: {percent_variation:.2f}%"
    
    return str_print


def run(agg_date):
    logger.info("Starting...")

    spark = SparkSession.builder \
        .appName('fluctuations_agg') \
        .master("local[*]") \
        .config("spark.log.level", "WARN") \
        .getOrCreate()

    
    logger.info("Creating required tables...")

    table_schemas = yaml.safe_load_all(open('/home/iceberg/spark-apps/' + '/schemas/' + 'fluctuations' + '.yaml','r'))
    tables_list = [i for i in table_schemas]
    check_tables(spark, tables_list)

    industries_dimension = spark \
            .read \
            .format('iceberg') \
            .load('assignment_db.industries')
    companies_dimension = spark \
            .read \
            .format('iceberg') \
            .load('assignment_db.companies')
    customers_dimension = spark \
        .read \
        .format('iceberg') \
        .load('assignment_db.customers') \
        .join(companies_dimension \
              .join(industries_dimension, on=['industry'], how='right')
              , on=['company_name'], how='right') \
        .select("customer_id", "industry")

    orders_with_industry = spark. \
        read \
        .format('iceberg') \
        .load('assignment_db.orders') \
        .join(broadcast(customers_dimension), 'customer_id')

    df_24h = orders_with_industry.filter(
            (col('year') == agg_date.year) &
            (col('month') == agg_date.month) &
            (col('day') == agg_date.day)) \
            .groupBy(col('industry')).agg(_sum('amount').alias("24h_value"))

    start_30d = agg_date - datetime.timedelta(days=31)
    end_30d = agg_date - datetime.timedelta(days=1)
    start_year = start_30d.year
    start_month = start_30d.month
    start_day = start_30d.day
    end_year = end_30d.year
    end_month = end_30d.month
    end_day = end_30d.day

    df_30d = orders_with_industry.filter(
        ((col("year")==start_year) & (col("month") == start_month) & (col("day") >= start_day)) |
        ((col("year")==end_year) & (col("month") > start_month) & (col("month") < end_month)) |
        ((col("year")==end_year) & (col("month") == end_month) & (col("day") <= end_day))) \
        .groupBy("year","month","day","industry") \
        .agg(_sum('amount').alias('amount_sum')) \
        .groupBy('industry') \
        .agg(avg('amount_sum').alias("30d_avg"))

    window_spec = Window.orderBy(_abs(col("diff")).desc())
    fluctuations_df = df_24h \
        .join(df_30d, 'industry') \
        .withColumn('diff', col('24h_value') - col('30d_avg')) \
        .withColumn('rank', row_number().over(window_spec)) \
        .filter(col('rank') <=3)
    
    logger.info("\t\tThese where the 3 major fluctuating industries:\n")
    fluctuations_df.rdd.foreach(lambda x: logger.info(format_print_diff(x)))
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    fluctuations_df \
        .withColumn('aggregation_date', lit(agg_date)) \
        .select('aggregation_date', 'industry', 'rank', '24h_value', '30d_avg', 'diff') \
        .write \
        .mode('overwrite') \
        .format('iceberg') \
        .insertInto('assignment_db.fluctuations')



if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Date to be aggregated. If left emppty will use previous day')
    parser.add_argument('--agg_date', type=str,required=False, help='Date in the following format: "YYYY-MM-DD"')
    args = parser.parse_args()

    date_str = args.agg_date
    if date_str:
        date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    else:
        date_obj = (datetime.datetime.now() - datetime.timedelta(days=1)).date()
    run(date_obj)