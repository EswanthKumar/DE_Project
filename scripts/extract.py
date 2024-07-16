import os
import sys

current = os.path.dirname(os.path.realpath(__file__))
root_dic = os.path.abspath(os.path.join(current,'..'))
sys.path.append(root_dic)

from config.config import Config
from utils.help import Help


def extract_fn():
    
    s3 =  Help.boto3_connect()
    print(s3)
    sf = Help.snowflake_connect()
    print(sf)

    
    
    
    
