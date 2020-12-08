from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import *
import configparser
import os

config = configparser.ConfigParser()
config.read(os.path.expanduser("~/.aws/credentials"))
aws_section = 'default'

_AWS_KEY_ID = config.get(aws_section, "aws_access_key_id")
_AWS_SECRET_KEY = config.get(aws_section, "aws_secret_access_key")
_KAFKA_HOST = '127.0.0.1:9092'
_KAFKA_INPUT_TOPIC = 'bank-transaction'
_KAFKA_GROUP_ID = 'sp-count'
_S3_BUCKET = 's3a://odc-raw-data-ut-bucket/Arthur/delta_lake/'


def get_spark_session():
    conf = (
        SparkConf()
        .setAppName('{}-spark-streaming'.format(_KAFKA_INPUT_TOPIC))
        .set('spark.driver.maxResultSize', '0')
        .set('spark.executor.core', '1')
        .set('spark.executor.memory', '1g')
        .set(
            'spark.jars.packages',
            'org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,org.elasticsearch:elasticsearch-hadoop:7.5.0,io.delta:delta-core_2.11:0.6.1,org.apache.hadoop:hadoop-aws:2.7.3',
        )
        .set('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .set('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
    )

    sc = SparkSession.builder.config(conf=conf).getOrCreate()

    sc.sparkContext.setLogLevel('WARN')
    sc.sparkContext._conf.setAll(
        [('spark.delta.logStore.class', 'org.apache.spark.sql.delta.storage.S3SingleDriverLogStore')]
    )
    sc._jsc.hadoopConfiguration().set('fs.s3a.awsAccessKeyId', _AWS_KEY_ID)
    sc._jsc.hadoopConfiguration().set('fs.s3a.awsSecretAccessKey', _AWS_SECRET_KEY)
    sc._jsc.hadoopConfiguration().set('fs.s3a.impl', 'org.apache.hadoop.fs.s3native.NativeS3FileSystem')

    return sc


def set_schema():
    schema = (
        StructType()
        .add('account_no', StringType())
        .add('date', StringType())
        .add('transaction_details', StringType())
        .add('value_date', StringType())
        .add('withdrawal_amt', IntegerType())
        .add('balance_amt', IntegerType())
        .add('event_time', TimestampType())
    )

    return schema


if __name__ == '__main__':
    # [SPARK SESSION]
    print('Starting a spark session')
    sc = get_spark_session()

    # [Import deltalake module]
    from delta.tables import *

    print('Set kafka input schema')
    schema = set_schema()

    # [SOURCE]
    df_json = (
        sc.readStream.format('kafka')
        .option('kafka.bootstrap.servers', _KAFKA_HOST)
        .option('subscribe', _KAFKA_INPUT_TOPIC)
        .load()
    )

    df = (
        df_json.selectExpr('CAST(key AS STRING)', 'CAST(value AS STRING)')
        .select(from_json(col('value'), schema).alias('transaction_data'))
        .select('transaction_data.*')
    )

    # Write delta table to S3
    qry = (
        df.writeStream.format('delta')
        .outputMode('append')
        .option('kafka.bootstrap.servers', _KAFKA_HOST)
        .option('checkpointLocation', './tmp/checkpoint-{}'.format(_KAFKA_GROUP_ID))
        .start('{}/{}'.format(_S3_BUCKET, _KAFKA_INPUT_TOPIC))
    )

    qry.awaitTermination()