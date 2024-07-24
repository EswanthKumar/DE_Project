import os
import sys

current = os.path.dirname(os.path.realpath(__file__))
root_dic = os.path.abspath(os.path.join(current, '..'))
sys.path.append(root_dic)

from scripts.extract import extract_fn
from config.config import Config
from utils.help import Help
from io import StringIO
import pandas as pd
import pandasql as psql
from glob import glob
import numpy as np


def transformation():
    s3 = Help.boto3_connect()
    
    bucket_names = s3.list_buckets()
    
    if 'Buckets' in bucket_names:
        for bucket in bucket_names['Buckets']:
            print(bucket['Name'])
    
        bucket_name = 'prtde'
        
        response = s3.list_objects_v2(Bucket = bucket_name)
        # if record in response:
        #     # print(record)
            
        if 'Contents' in response:
            for data in response['Contents']:
                files = data['Key']
                print(files)
                
                # bucket la irukura files kulla irukuratha read pandrathuku
                obj = s3.get_object(Bucket=bucket_name, Key= files)
                csv_content = obj['Body'].read().decode('utf-8')
                # print(csv_content)
                
                csv_file = StringIO(csv_content)
                csv_file.seek(0)
                
                df = pd.read_csv(csv_file)
                # print(f' DataFame {df}')
                
                if 'actor.csv' == files:
                    if 'actor_number' not in df.columns:
                        df['actor_number'] = np.nan
                        df['fullname'] = df['first_name'] + ' ' + df['last_name']
                        transformations = df[['actor_id', 'fullname','actor_number']].head(50)
                        # print(transformations)
                        
                        trans_prtde = 'atrans'
                        csv_buffer = StringIO()
                        transformations.to_csv(csv_buffer, index=False)
                        s3.put_object(Bucket=trans_prtde, Key='transformed_actor.csv', Body=csv_buffer.getvalue())
                        print("Transformed actor.csv uploaded to S3 as transformed_actor.csv")
                        
                if 'address.csv' == files:
                    if 'address2' in df.columns:
                        df['address2'].fillna('NaN', inplace=True)
                        df['postal_code'].fillna('NaN', inplace=True)
                        df['phone'].fillna('NaN', inplace=True)
                        transformat = df[['address','postal_code']].head(50)
                        print(transformat)
                        
                        trans_prtde = 'atrans'
                        csv_buffer = StringIO()
                        transformat.to_csv(csv_buffer, index=False)
                        s3.put_object(Bucket=trans_prtde, Key='transformed_address.csv', Body=csv_buffer.getvalue())
                        print("Transformed address.csv uploaded to S3 as transformed_address.csv")
                                       
                        break
if __name__ == "__main__":
    transformation()
    