import boto3
class Config:
    aws_cre = {
        'service_name' : 's3',
        'region_name' : 'ap-southeast-2'
    }
    
    snow_cre = {
        'user' : 'ESWANTH',
        'password' : 'Sollamata@12345',
        'account' : 'nn01356.ap-southeast-1',
        'warehouse' : 'NEW_WAREHOUSE',
        'database' : 'SNOWFLAKE_DB',
        'schema' : 'SNOWFLAKE_SC',
        'role' : 'ACCOUNTADMIN'
    }
    
    
