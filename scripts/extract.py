
import os
import sys

current = os.path.dirname(os.path.realpath(__file__))
root_dic = os.path.abspath(os.path.join(current, '..'))
sys.path.append(root_dic)

from config.config import Config
from utils.help import Help
from io import StringIO
import pandas as pd



def extract_fn():
    try:
        pg = Help.postgre_connect()
        s3 = Help.boto3_connect()
        print(s3)
        
        cur =pg.cursor()
        
        table_name = '''(select table_name from information_schema.tables where table_schema = 'public' ORDER BY table_name)
        '''
        cur.execute(f"SELECT * FROM {table_name}")
        
        tables = cur.fetchall()

        # Loop through each table and print its data
        for table in tables:
            table_name = table[0]
            # print(f"\nTable: {table_name}")

            # ella table laiyum seperate ah adukurathuku
            query = f"SELECT * FROM {table_name};"
            cur.execute(query)
        
            rows = cur.fetchall()
            
            colnames = [desc[0] for desc in cur.description] ## colum name matum thaniya adukurathuku
            
            df = pd.DataFrame(rows, columns=colnames)
            # print(df)
            
            buffer = StringIO()
            df.to_csv(buffer , index= False)
            buffer.seek(0)
            
            
            csv_file = f"{table_name}.csv"
            
            bucket_name = 'prtde'
            
            # with open(csv_file, 'rb') as f:
            #     s3.upload_fileobj(f,bucket_name,csv_file)
            upload = s3.put_object(Bucket=bucket_name, Key=csv_file, Body=buffer.getvalue(), ContentType='text/csv')
            print(f"successfully uploaded")

        
    except Exception as error:
        print(error)
        
    finally:
        pg.close()
        cur.close()

# If this script is the main script being run, call extract_fn
if __name__ == "__main__":
    extract_fn()