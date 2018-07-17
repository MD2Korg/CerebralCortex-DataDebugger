from cerebralcortex.core.datatypes.datastream import DataStream                                                                                                                                                                            
from cerebralcortex.core.datatypes.datastream import DataPoint                                                                                                                                                                             
from cerebralcortex.core.datatypes.stream_types import StreamTypes 
from cerebralcortex.cerebralcortex import CerebralCortex                                                                                                                                                                                   
from cerebralcortex.core.util.spark_helper import get_or_create_sc                                                                                                                                                                         

from datetime import timedelta                                                                                                                                                                                                             
from datetime import datetime                                                                                                                                                                                                              

import time                                                                                                                                                                                                                                
import json                                                                                                                                                                                                                                
import uuid                                                                                                                                                                                                                                
import traceback                                                                                                                                                                                                                           
import base64                                                                                                                                                                                                                              
import pickle                                                                                                                                                                                                                              
import pytz   
import ast

from distutils.version import StrictVersion

CC = CerebralCortex("/cerebralcortex/code/config/cc_starwars_configuration.yml")

def get_latest_stream_id(user_id, stream_name):                                                                                                                                                                                  
    streamids = CC.get_stream_id(user_id, stream_name)  
    
    latest_stream_id = []                                                                                                                                                                                                              
    latest_stream_version = None                                                                                                                                                                                                       
    for stream in streamids:                                                                                                                                                                                                           
        stream_metadata = CC.get_stream_metadata(stream['identifier'])                                                                                                                                                            
        execution_context = stream_metadata[0]['execution_context']                                                                                                                                                                    
        execution_context = ast.literal_eval(execution_context)                                                                                                                                                                        
        stream_version = execution_context['algorithm']['version'] 
        print('X'*100)
        print(stream_version)
        try:                                                                                                                                                                                                                           
            stream_version = int(stream_version)                                                                                                                                                                                       
            stream_version = str(stream_version) + '.0'                                                                                                                                                                                
        except:                                                                                                                                                                                                                        
            pass                                                                                                                                                                                                                       
        stream_version = StrictVersion(stream_version)                                                                                                                                                                                 
        if not latest_stream_version:                                                                                                                                                                                                  
            latest_stream_id.append(stream)                                                                                                                                                                                            
            latest_stream_version = stream_version                                                                                                                                                                                     
        else:                                                                                                                                                                                                                          
            if stream_version > latest_stream_version:                                                                                                                                                                                 
                latest_stream_id = [stream]
            elif stream_version == latest_stream_version:                                                                                                                                                                              
                    latest_stream_id.append(stream)                                                                                                                                                                                        
    print('Yo',latest_stream_version)                                                                                                                                                                                                                                     
    return latest_stream_id

activity_stream_name = "ACTIVITY_TYPE--org.md2k.phonesensor--PHONE_data_quality"



date_format = '%Y%m%d'                                                                                                                                                                                                                 
                                                                                                                                                                                                                                           
start_date = '20171001'                                                                                                                                                                                                                
#start_date = '20180401'                                                                                                                                                                                                               
start_date = datetime.strptime(start_date, date_format)                                                                                                                                                                                
end_date = '20180530'                                                                                                                                                                                                                  
end_date = datetime.strptime(end_date, date_format)  

all_days = []                                                                                                                                                                                                                          
while True:                                                                                                                                                                                                                            
    all_days.append(start_date.strftime(date_format))                                                                                                                                                                                  
    start_date += timedelta(days = 1)                                                                                                                                                                                                  
    if start_date > end_date : break 
        
usr = 'febb3cef-56cc-4f40-b7c2-7c2663b0dc33'
    
stream_ids = get_latest_stream_id(usr, activity_stream_name)

print(stream_ids)
if not len(stream_ids):
    print('Y'*100,usr, strm)

strm_id = stream_ids[0]['identifier']
stream_dps_count = 0
stream_corrupt_dps_count = 0
for day in all_days:
    ds = CC.get_stream(strm_id, usr, day)
    if len(ds.data):
        dp = ds.data[0]
        num_day_dps = dp.sample[0]
        num_day_corrupt_dps = len(dp.sample[1])
        if num_day_corrupt_dps:
            print(dp.sample[1])
            break
            
