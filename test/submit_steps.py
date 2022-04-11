import boto3
import argparse

class GetSteps:
    def __init__(self, env) -> None:
        self.env = env
        self.source_code_path = "s3://{bucket_name}}/code_deploy"
        
    def steps(self):
        job_step = [
            {
                "Name": "Spark streaming testing",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--master", "yarn",
                        "--deploy-mode", "cluster",
                        "--conf", "spark.memory.fraction=0.8",
                        "--conf", "spark.memory.storageFraction=0.4",
                        "--conf", "spark.driver.cores=2",
                        "--conf", "spark.driver.memory=14g",
                        "--packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.spark:spark-avro_2.12:3.1.1,io.delta:delta-core_2.12:1.0.0,com.amazon.deequ:deequ:2.0.0-spark-3.1",
                        "--py-files", f"{self.source_code_path}/spark-consumer-1.0.0-py3.8.egg",
                        "--files",f"{self.source_code_path}/general_config.yaml,{self.source_code_path}/topics_config.yaml,{self.source_code_path}/consumer_config.yaml",
                        f"{self.source_code_path}/spark_consumer.py",
                        "--environment-mode", self.env
                    ]
                }
            }
        ]
        return job_step

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--environment-mode', action='store',
                        type=str, required=True, help='Which environment?')
    args = parser.parse_args()
        
    job_flow_id = "{your_job_flow_id}"

    env = args.environment_mode
    initial_steps = GetSteps(env)
    client = boto3.client('emr')
    client.add_job_flow_steps(JobFlowId=job_flow_id, Steps=initial_steps.steps())
    
