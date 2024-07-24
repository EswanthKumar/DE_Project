import os
import sys

current = os.path.dirname(os.path.realpath(__file__))
root_dic = os.path.abspath(os.path.join(current, '..'))
sys.path.append(root_dic)

from scripts.extract import extract_fn
from scripts.transformation import transformation
from config.config import Config
from utils.help import Help
from io import StringIO
import pandas as pd


def loan():
    try:
        
        sf = Help.snowflake_connect()
        
        cur = sf.cursor()
        
        cur.execute('''
                    CREATE OR REPLACE STAGE prtde_stage
                    URL = 's3://atrans/'
                    STORAGE_INTEGRATION = prtde_integration
                    ''')
        
        cur.execute('''
                    create or replace file format ff
                    type = csv
                    field_delimiter = ','
                    skip_header = 1
                    ''')
        
        cur.execute('LIST @prtde_stage')
        files = cur.fetchall()
        for file in files:
            print(file)
            
        cur.execute('''
                    COPY INTO transformed_actor
                    FROM @prtde_stage/transformed_actor.csv
                    FILE_FORMAT = ff
                    ''')
        
        cur.execute('''
                    COPY INTO transformed_address
                    FROM @prtde_stage/transformed_address.csv
                    FILE_FORMAT = ff
                    ''')
        print('successfully files upload from AmazonS3 to Snowflake')
        
    except Exception as e:
        print(e)
        
    finally:
        sf.commit()
        cur.close()
        sf.close()
        

if __name__ == "__main__":
    loan()