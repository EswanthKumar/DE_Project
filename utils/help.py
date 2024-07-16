import boto3
from config.config import Config
import snowflake.connector as sf


class Help:
    def boto3_connect():
        service_name = Config.aws_cre['service_name']
        region_name = Config.aws_cre['region_name']
        aws_access_key_id = Config.aws_cre['aws_access_key_id']
        aws_secret_access_key = Config.aws_cre['aws_secret_access_key']
        
        s3 = boto3.resource(
            service_name=service_name,
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key =aws_secret_access_key
            )
        return s3
        
    def snowflake_connect():
        user = Config.snow_cre['user']
        password = Config.snow_cre['password']
        account = Config.snow_cre['account']
        warehouse = Config.snow_cre['warehouse']
        database = Config.snow_cre['database']
        schema = Config.snow_cre['schema']
        role = Config.snow_cre['role']
        
        sf_connect = sf.connect(
            user = user,
            password = password,
            account = account,
            warehouse = warehouse,
            database = database,
            schema = schema,
            role = role
            )
        
        return sf_connect
        
        
           
        