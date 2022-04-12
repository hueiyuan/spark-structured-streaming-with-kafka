from confluent_kafka.schema_registry import SchemaRegistryClient

class SRClient:
    def __init__(self, schema_registry_url, schema_registry_user, schema_registry_pwd):
        config = dict()
        config['url'] = schema_registry_url
        config['basic.auth.user.info'] = '{usr}:{pwd}'.format(usr=schema_registry_user, pwd=schema_registry_pwd)
        self.sr_client = SchemaRegistryClient(config)

    def get_latest_version_schema(self, data_tag):
        data_tag = data_tag + '-value'
        schema_object = self.sr_client.get_latest_version(data_tag)
        return schema_object.schema.schema_str
