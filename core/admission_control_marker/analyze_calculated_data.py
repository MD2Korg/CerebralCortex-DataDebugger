# Copyright (c) 2018, MD2K Center of Excellence
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
import math

from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.datatypes.datastream import DataPoint
from cerebralcortex.core.datatypes.stream_types import StreamTypes
#from core.computefeature import ComputeFeatureBase

from random import shuffle
from datetime import timedelta
from datetime import datetime
import time
import os
import json
import uuid
import traceback
import base64
import pickle
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.util.spark_helper import get_or_create_sc
import pytz
import ast

from distutils.version import StrictVersion

MIN_ACCL_VAL = -5.0
MAX_ACCL_VAL = 5.0

def get_latest_stream_id(user_id, stream_name, CC):                                                                                                                                                                                  
    streamids = CC.get_stream_id(user_id, stream_name)  
    
    latest_stream_id = []                                                                                                                                                                                                              
    latest_stream_version = None                                                                                                                                                                                                       
    for stream in streamids:                                                                                                                                                                                                           
        stream_metadata = CC.get_stream_metadata(stream['identifier'])                                                                                                                                                            
        execution_context = stream_metadata[0]['execution_context']                                                                                                                                                                    
        execution_context = ast.literal_eval(execution_context)                                                                                                                                                                        
        stream_version = execution_context['algorithm']['version'] 
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
    return latest_stream_id


def get_corrupt_data_count(userid, all_days, cc_config_path):
    stream_names = []

    sms_stream_name = 'CU_SMS_LENGTH--edu.dartmouth.eureka_corrupt_data'
    stream_names.append(sms_stream_name)

    call_stream_name = 'CU_CALL_DURATION--edu.dartmouth.eureka_corrupt_data'
    stream_names.append(call_stream_name)

    proximity_stream_name = \
    'PROXIMITY--org.md2k.phonesensor--PHONE_corrupt_data'
    stream_names.append(proximity_stream_name)

    cu_appusage_stream_name = 'CU_APPUSAGE--edu.dartmouth.eureka_corrupt_data'
    stream_names.append(cu_appusage_stream_name)

    light_stream_name = \
    'AMBIENT_LIGHT--org.md2k.phonesensor--PHONE_corrupt_data'
    stream_names.append(light_stream_name)

    call_number_stream_name = \
    "CU_CALL_NUMBER--edu.dartmouth.eureka_corrupt_data"
    stream_names.append(call_number_stream_name)

    sms_number_stream_name = "CU_SMS_NUMBER--edu.dartmouth.eureka_corrupt_data"
    stream_names.append(sms_number_stream_name)

    activity_stream_name = \
    "ACTIVITY_TYPE--org.md2k.phonesensor--PHONE_corrupt_data"
    stream_names.append(activity_stream_name)

    call_type_stream_name = "CU_CALL_TYPE--edu.dartmouth.eureka_corrupt_data"
    stream_names.append(call_type_stream_name)

    sms_type_stream_name = "CU_SMS_TYPE--edu.dartmouth.eureka_corrupt_data" 
    stream_names.append(sms_type_stream_name)

    location_stream = 'LOCATION--org.md2k.phonesensor--PHONE_corrupt_data'
    stream_names.append(location_stream)

    geofence_list_stream = \
    'GEOFENCE--LIST--org.md2k.phonesensor--PHONE_corrupt_data'
    stream_names.append(geofence_list_stream)

    CC = CerebralCortex(cc_config_path)

    all_stream_quality = {}        

    count = 0
    started_time = datetime.now()
    userids = [userid]
    for usr in userids[:1]:
        print('processing %d of %d' % (count,len(userids)))
        count += 1

        output_per_day_dir = '/tmp/corruption_per_day/'
        if not os.path.exists(output_per_day_dir):
            os.mkdir(output_per_day_dir)
        buf_day = ''
        for strm in stream_names:
            if not strm in all_stream_quality:
                all_stream_quality[strm] = [0, 0, 0]
            
            stream_ids = get_latest_stream_id(usr, strm, CC)
                
            strm_id = stream_ids[0]['identifier']
            stream_dps_count = 0
            stream_corrupt_dps_count = 0
            stream_possible_accl_gyro_dps = 0

            for day in all_days:
                ds = CC.get_stream(strm_id, usr, day)
                if len(ds.data):
                    dp = ds.data[0]
                    num_day_dps = dp.sample[0]
                    num_day_corrupt_dps = len(dp.sample[1])
                    num_possible_accl_sample = 0
                    # check if the corrupted datapoints could be accl or gyro
                    # samples
                    if num_day_corrupt_dps:
                        for corrupt_dp in dp.sample[1]:
                            if type(corrupt_dp.sample) is list and len(corrupt_dp.sample) == 3:
                                try:
                                    if corrupt_dp.sample[0] >=  MIN_ACCL_VAL and corrupt_dp.sample[0] <= MAX_ACCL_VAL:
                                        if corrupt_dp.sample[1] >=  MIN_ACCL_VAL and corrupt_dp.sample[1] <= MAX_ACCL_VAL:
                                            if corrupt_dp.sample[2] >=  MIN_ACCL_VAL and corrupt_dp.sample[2] <= MAX_ACCL_VAL:
                                                num_possible_accl_sample += 1
                                except Exception as e:
                                    print(corrupt_dp)
                                    print(str(e))

                    buf_day += str(usr) + '\t' + str(strm) + '\t' + str(day) +'\t' +\
                                str(num_day_dps) + '\t' + str(num_day_corrupt_dps) + '\t' +\
                                str(num_possible_accl_sample) + '\n'

                    stream_dps_count += num_day_dps
                    stream_corrupt_dps_count += num_day_corrupt_dps
                    stream_possible_accl_gyro_dps += num_possible_accl_sample
                    
            #print('X'*50)
            #print(usr, strm, stream_dps_count, stream_corrupt_dps_count)
            all_stream_quality[strm][0] += stream_dps_count
            all_stream_quality[strm][1] += stream_corrupt_dps_count
            all_stream_quality[strm][2] += stream_possible_accl_gyro_dps
        print(all_stream_quality)
    
        output_dir = '/tmp/corruption_count/'
        if not os.path.exists(output_dir):
            os.mkdir(output_dir)
        file_name = usr + '.pickle'
        f = open(os.path.join(output_dir,file_name),'wb')
        pickle.dump(all_stream_quality, f)
        f.close()

        f = open(os.path.join(output_per_day_dir,file_name),'w')
        f.write(buf_day)
        f.close()

    return all_stream_quality

def main():
    date_format = '%Y%m%d'

    start_date = '20171001'
    #start_date = '20180401'
    start_date = datetime.strptime(start_date, date_format)
    end_date = '20180530'
    end_date = datetime.strptime(end_date, date_format)
    CC_CONFIG_FILEPATH="/cerebralcortex/code/config/cc_starwars_configuration.yml"

    all_days = []
    while True:
        all_days.append(start_date.strftime(date_format))
        start_date += timedelta(days = 1)
        if start_date > end_date : break


    userids = []
    f = open('users.txt','r')
    usrs = f.read()
    userids = usrs.split(',')
    userids = [x.strip() for x in userids]
    f.close()

    #userids = ['20940a76-976b-446e-b173-89237835ae6b']

    #  20180401 20940a76-976b-446e-b173-89237835ae6b

    print("Number of users ",len(userids))
    num_cores = 164


    useSpark = True

    if useSpark:
        spark_context = get_or_create_sc(type="sparkContext")

        rdd = spark_context.parallelize(userids, len(userids))
        try:
            results = rdd.map(
                lambda user: get_corrupt_data_count(user,
                                               all_days, 
                                               CC_CONFIG_FILEPATH))
            x = results.count()
            print(x)

            spark_context.stop()
        except Exception as e:
            print(e)
    else:
        get_corrupt_data_count(userids[0],
                                       all_days, 
                                       CC_CONFIG_FILEPATH)

if __name__ == '__main__':
    main()

