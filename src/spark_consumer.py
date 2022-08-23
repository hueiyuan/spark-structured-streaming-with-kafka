#!/usr/bin/env python3
import json
import argparse
import pendulum

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.avro.functions import from_avro
from delta.tables import DeltaTable

from project_libs.common import loadconfig
from project_libs.common import utils
from helpers.schema_registry_helper import SRClient

spark = SparkSession.builder \
    .appName('kafka-spark-consumer')\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .getOrCreate()

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger(__name__)

# udf 
def get_minute_parition(_processing_time, _minutes):
    time_range = list(range(0, 60, int(_processing_time.split(" ")[0])))
    i = 0
    while i < len(time_range):
        _minute = "{:02d}".format(time_range[i])
        if _minutes < time_range[i]:
            _minute = "{:02d}".format(time_range[i-1])
            break
        i += 1
    return _minute
udf_get_minute_parition = F.udf(get_minute_parition, StringType())

class IngestDataFromKafka:
    def __init__(self, 
                 environment_mode: str, 
                 config: dict):
        logger.info(f'===[INFO]==== env:{environment_mode}')
        self.config = config
        self.env = environment_mode
        data_tags_partition_dict = {data_tag: partition_list for data_tag, partition_list in config['data_tags'][self.env].items()}
        self._proj_name = self.config['project_name']
        self.schema_registry_user = utils.get_ssm_value(
            region=self.config['aws']['region_name'],
            ssm_name=self.config[self.env]['schmea_registry_ssm_usr'])
        self.schema_registry_pwd = utils.get_ssm_value(
            region=self.config['aws']['region_name'],
            ssm_name=self.config[self.env]['schmea_registry_ssm_pwd'])
        
        self.from_avro_options = {'mode': 'PERMISSIVE'}
        
        self.schema_registry_client = SRClient(
            schema_registry_url=self.config[self.env]['schema_registry_url'],
            schema_registry_user=self.schema_registry_user,
            schema_registry_pwd=self.schema_registry_pwd)

        self.msk_config = {
            'kafka.bootstrap.servers': self.config[self.env]['kafka_brokers'],
            'kafka.ssl.truststore.location': self.config['kafka']['truststore_path'],
            'kafka.security.protocol': 'SSL',
            'kafka.ssl.protocol': 'SSL',
            'assign': json.dumps(data_tags_partition_dict),
            'groupIdPrefix': self.config['streaming_setting']['group_id_prefix'],
            'minPartitions': self.config['streaming_setting']['min_partitions'],
            'maxOffsetsPerTrigger': self.config['streaming_setting']['qps_upperbound']*30*60,
            'startingOffsets': self.config['kafka']['offset_strategy'],
            'failOnDataLoss': True,
            'kafka.fetch.min.bytes': self.config['update_parameters_setting']['fetch_min_bytes'],
            'kafka.fetch.max.wait.ms': self.config['update_parameters_setting']['fetch_max_wait_ms']
        }

        self.aws_bucket = self.config['aws']['bucket_name']
        self.s3_delta_path = self.config['streaming_consumer']['delta_path']
        self.s3_data_monitor_path = self.config['streaming_consumer']['data_monitorin_path']
        self.parse_timestamp_format = "yyyy-MM-dd'T'HH:mm:ss"
        
    def __write_to_s3_foreachbatch(self, df, epoch_id):
        df.cache()

        for data_tag in self.config['data_tags'][self.env]:
            #start_process_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            start_process_datetime = pendulum.now('UTC')
            corresponding_delta_path = self.s3_delta_path.format(
                bucket=self.aws_bucket, 
                env=self.env, 
                topic=data_tag
            )
            
            corresponding_data_monitor_path = self.s3_data_monitor_path.format(
                bucket=self.aws_bucket, 
                env=self.env,
                dt=start_process_datetime.to_date_string(),
                topic=data_tag
            )
            correct_streaming_data_df = self.__data_processing(df, data_tag)
            correct_streaming_data_df.write \
                .format('delta') \
                .mode('append') \
                .partitionBy('dt', 'hour', 'minute') \
                .option('mergeSchema', 'true') \
                .save(corresponding_delta_path)
            
            
            # Monitoring
            self.__monitoring(
                _data_tag=data_tag,
                _start_time=start_process_datetime, 
                _delta_path=corresponding_delta_path,
                _monitor_path=corresponding_data_monitor_path
                )
            logger.info(f'===[INFO]==== data_tag : {data_tag} processe done.')
    
    def __data_processing(self, _df, _data_tag):
        streaming_input_df = _df.filter(F.col('topic') == _data_tag) \
            .select(
                'key', 'topic', 'partition', 'offset', 'timestamp', 'timestampType',
                from_avro(
                    F.expr('substring(value, 6)'), 
                    self.schema_registry_client.get_latest_version_schema(_data_tag),
                    self.from_avro_options).alias('raw_data'),
                F.col('value').alias('original_data')) \
            .withColumnRenamed('timestamp', 'kafka_ingest_time') \
            .repartition(self.config['streaming_setting']['num_files'])
        
        valid_df = streaming_input_df \
            .withColumn(
                'dt', F.date_format('kafka_ingest_time', 'yyyy-MM-dd')) \
            .withColumn(
                'hour', F.format_string("%02d", F.hour('kafka_ingest_time'))) \
            .withColumn(
                'minute', udf_get_minute_parition(
                    F.lit(self.config['streaming_setting']['processing_time']), F.minute('kafka_ingest_time')))

        streaming_preprocess_df = valid_df \
            .select('topic', 'partition', 'offset', 'raw_data.*', 'kafka_ingest_time', 'dt', 'hour', 'minute') \
            .withColumn(
                'client_timestamp', F.to_timestamp(F.from_unixtime(
                    F.unix_timestamp('extract_client_timestamp', self.parse_timestamp_format)))) \
            .withColumn(
                'client_dt', F.from_unixtime(F.unix_timestamp(
                    'extract_client_timestamp', self.parse_timestamp_format), 'yyyy-MM-dd'))
        
        data_df = streaming_preprocess_df.repartition(self.config['streaming_setting']['num_files'])
            
        return data_df 
    
    def __monitoring(self, 
                     _data_tag: str, 
                     _start_time: str,
                     _delta_path: str, 
                     _monitor_path: str):
        delta_table = DeltaTable.forPath(spark, _delta_path)
        last_operation_df = delta_table.history(1)  # get latest transaction log
        last_operation_df \
            .withColumnRenamed('timestamp', 'delta_time') \
            .withColumn('process_start_time', F.lit(_start_time.replace(microsecond=0))) \
            .withColumn('data_tag', F.lit(_data_tag)) \
            .withColumn('num_records', F.col('operationMetrics').getItem('numOutputRows').cast(IntegerType())) \
            .withColumn('dt', F.to_date(F.col('delta_time'))) \
            .select('data_tag', 'num_records', 'process_start_time', 'delta_time') \
            .write \
            .mode('append') \
            .parquet(_monitor_path)

    def run(self):
        logger.info('===[INFO]==== Start to run process')
        #spark.sql('set spark.sql.legacy.timeParserPolicy=LEGACY')
        
        checkpoint_path = self.config['streaming_consumer']['spark_checkpoint_path'].format(
            bucket=self.aws_bucket,
            env=self.env,
            project_name=self._proj_name
        )
        
        try:
            streaming_writer = (
                spark.readStream
                .format('kafka')
                .options(**self.msk_config)
                .load()
                .writeStream
                .trigger(processingTime=self.config['streaming_setting']['processing_time'])
                .foreachBatch(self.__write_to_s3_foreachbatch)
                .option('checkpointLocation', checkpoint_path)
                .start()
                .awaitTermination()
            )
        except Exception as e:
            logger.info('[ERROR] Error message:{}'.format(e))
            logger.exception(e)
            raise Exception(e)


def main_consumer(environment_mode: str):
    general_config = loadconfig.config(
        service_name='etl',
        config_folder_prefix='project_configs',
        config_prefix_name='general',
        platform='AWS')._load()

    topics_config = loadconfig.config(
        service_name='etl',
        config_folder_prefix='project_configs',
        config_prefix_name='topics',
        platform='AWS')._load()

    consumer_config = loadconfig.config(
        service_name='etl',
        config_folder_prefix='project_configs',
        config_prefix_name='consumer',
        platform='AWS')._load()

    config = {
        **general_config,
        **topics_config,
        **consumer_config
    }
    
    main = IngestDataFromKafka(
        environment_mode=environment_mode,
        config=config)
    
    main.run()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--environment-mode', action='store',
                        type=str, required=True, help='Which environment?')
    args = parser.parse_args()
    
    main_consumer(args.environment_mode)
