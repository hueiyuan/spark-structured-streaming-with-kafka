import argparse
import boto3

def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def get_ssm_value(region: str, 
                  ssm_name: str, 
                  is_decryption=True) -> str:
    client = boto3.client(service_name='ssm', region_name=region)
    response = client.get_parameter(
        Name=ssm_name,
        WithDecryption=is_decryption
    )

    corresponding_value = response['Parameter']['Value']

    return corresponding_value.replace('\"','')
  
