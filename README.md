# Spark Structured Streaming consume Kafka Template 
## Project Description
The repo jsut a template which consume kafka with spark structured streaming, and emulate to write to AWS S3 with delta lake.

## The Repo Basic Spec
```
[AWS]
* AWS S3: the bucket which save consumed data from kafka with delta lake.
* AWS SSM: save related username and password of the schema registry.
* AWS EMR: emulate our spark structured streaming is running on AWS EMR.

[Other Service]
* Kafka: can self-hosted or AWS MSK.
* Schema Registry: self-hosted schema registry service, which assist to do schema validation.
```

## The Folder Structure
```
|-- project_libs/
    |-- __init__.py
    |-- common/
        |-- __init__.py
        |-- loadconfig.py
        |-- utils.py
|-- project_configs/
    |-- general_config.yaml
    |-- topics_config.yaml
    |-- consumer_config.yaml
|-- helpers/
    |-- __init__.py
    |-- schema_registry.py
|-- src/
    |-- spark_consumer.py
|-- test/
    |-- submit_steps.py
|-- spark_bootstrap.sh
|-- setup.py
|-- README.md
```

* `project_libs/` folder:
    1. Includes general lib script, `loadconfig.py` is load yaml file, and `utils.py` is util script.
* `project_configs/` folder:
    1. general_config.yaml: general config yaml file.
    2. topics_config.yaml: topic name by environment, just for testing and emulate.
    3. consumer_config.yaml: the config setting about consume data from kafka.
* `helpers/` folder:
    1. schema_registry.py: schema registry client.
* `src/` folder:
    1. spark_consumer.py: main consume kafka script.
* `test/` folder:
    1. submit_steps.py: It used to add step to AWS EMR with boto3.    
* `spark_bootstrap.sh`:
    It used to bootstrap emr cluster script.
* `setup.py`:
    It used to setup as egg package, and can be used emr steps.

ps. All string include `{}` just as template in config file. When we use it, need to replace proper value to config file.
