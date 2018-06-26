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

from urllib.request import urlopen
from bs4 import BeautifulSoup
from datetime import datetime
import numpy as np
from datetime import timedelta
import time
import json
import uuid
import copy
import traceback
from functools import lru_cache
import math
import base64
import pickle
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.util.spark_helper import get_or_create_sc
from window import window
from window import merge_consective_windows
import pytz

date_format = '%Y%m%d'

start_date = '20171001'
#start_date = '20180401'

start_date = datetime.strptime(start_date, date_format)

end_date = '20180530'
end_date = datetime.strptime(end_date, date_format)
CC_CONFIG_FILEPATH="/cerebralcortex/code/config/cc_starwars_configuration.yml"
# Below are the 'raw' ingested input streams that phone_features uses.
phone_input_streams = {}

call_stream_name = 'CU_CALL_DURATION--edu.dartmouth.eureka'
call_stream_admission_control = lambda x: (type(x) is float and x >= 0)
phone_input_streams[call_stream_name] = call_stream_admission_control

sms_stream_name = 'CU_SMS_LENGTH--edu.dartmouth.eureka'
sms_stream_admission_control = lambda x: (type(x) is float and x >= 0)
phone_input_streams[sms_stream_name] = sms_stream_admission_control

proximity_stream_name = 'PROXIMITY--org.md2k.phonesensor--PHONE'
proximity_stream_admission_control = lambda x: (type(x) is float and x >= 0)
phone_input_streams[proximity_stream_name] = proximity_stream_admission_control

cu_appusage_stream_name = 'CU_APPUSAGE--edu.dartmouth.eureka'
cu_appusage_admission_control = lambda x: type(x) is str
phone_input_streams[cu_appusage_stream_name] = cu_appusage_admission_control

light_stream_name = 'AMBIENT_LIGHT--org.md2k.phonesensor--PHONE'
light_stream_admission_control = lambda x: (type(x) is float and x >= 0)
phone_input_streams[light_stream_name] = light_stream_admission_control

appcategory_stream_name = "org.md2k.data_analysis.feature.phone.app_usage_category"
appcategory_stream_admission_control = lambda x: (type(x) is list and len(x) == 4)
phone_input_streams[appcategory_stream_name] = appcategory_stream_admission_control

call_number_stream_name = "CU_CALL_NUMBER--edu.dartmouth.eureka"
call_number_stream_admission_control = lambda x: (type(x) is str)
phone_input_streams[call_number_stream_name] = call_number_stream_admission_control

sms_number_stream_name = "CU_SMS_NUMBER--edu.dartmouth.eureka"
sms_number_stream_admission_control = lambda x: (type(x) is str)
phone_input_streams[sms_number_stream_name] = sms_number_stream_admission_control

activity_stream_name = "ACTIVITY_TYPE--org.md2k.phonesensor--PHONE"
activity_stream_admission_control = lambda x: (type(x) is list and len(x) == 2)
phone_input_streams[activity_stream_name] = activity_stream_admission_control

call_type_stream_name = "CU_CALL_TYPE--edu.dartmouth.eureka"
call_type_stream_admission_control = lambda x: (type(x) is float)
phone_input_streams[call_type_stream_name] = call_type_stream_admission_control

sms_type_stream_name = "CU_SMS_TYPE--edu.dartmouth.eureka"
sms_type_stream_admission_control = lambda x: (type(x) is float)
phone_input_streams[call_type_stream_name] = sms_type_stream_admission_control



class PhoneStreamsAnalyzer():
    """
    This class is responsible for computing features based on streams of data
    derived from the smartphone sensors.
    """

    def get_day_data(self, userid, day, stream_name, localtime=True):
        """
        Return the filtered list of DataPoints according to the admission control provided

        :param List(DataPoint) data: Input data list

        1 - Data is present and passes admission control
        0 - No data present
        -1 - Data is present and fails admission control
        """
        data = []
        stream_ids = self.CC.get_stream_id(userid, stream_name)
        for stream_id in stream_ids:
            if stream_id is not None:
                ds = self.CC.get_stream(stream_id['identifier'], user_id=userid, day=day, localtime=localtime)
                if ds is not None:
                    if ds.data is not None:
                        data += ds.data
        if len(stream_ids) > 1:
            data = sorted(data, key=lambda x: x.start_time)

        return data


    def analyze_all_users(self, userids, alldays, config_path):
        x = 0
        for usr in userids:
            print('Analyzing user %d %s' % (x,usr))
            self.analyze_user(usr, alldays, config_path) 
            x += 1

    def analyze_user(self, userid, alldays,config_path):
        self.CC = CerebralCortex(config_path)
        self.window_size = 3600

        for day in alldays:
            for phone_stream in phone_input_streams:
                current_date = datetime.strptime(day, date_format)
                day_data = self.get_day_data(userid, day, phone_stream)
                data_quality_analysis = []
                if len(day_data): 
                    corrupt_data = \
                                self.get_corrupt_data(day_data,
                                                             phone_input_streams[phone_stream])
                            
                    utc_offset = day_data[0].start_time.utcoffset().total_seconds() * 1000
                    dp = DataPoint(start_time=current_date,
                                           end_time=current_date + timedelta(days=1),
                                           offset=utc_offset,
                                           sample=[len(day_data), corrupt_data])
                    data_quality_analysis.append(dp)

                else:
                    next_day = current_date + timedelta(days=1)
                    utc_offset = 0
                    dp = DataPoint(start_time=current_date,
                                   end_time=next_day,
                                   offset=utc_offset,
                                   sample=[0, []])
                    data_quality_analysis.append(dp)
                
                # TODO - store the stream
                mf  = open('phone_input_streams_data_quality.json','r')
                metadata = mf.read()
                mf.close()
                metadata = json.loads(metadata)
                metadata_name = phone_stream + '_data_quality'
                output_stream_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, str(
                      metadata_name + userid + str(metadata))))
                input_streams = []
                input_stream_ids = self.CC.get_stream_id(userid, phone_stream)
                for inpstrm in input_stream_ids:
                    stream_info = {}
                    stream_info['name'] = phone_stream
                    stream_info['identifier'] = inpstrm['identifier']
                    input_streams.append(stream_info)

                metadata["execution_context"]["processing_module"]["input_streams"] = input_streams

                quality_ds = DataStream(identifier=output_stream_id, owner=userid, 
                        name=metadata_name, 
                        data_descriptor= metadata['data_descriptor'], 
                        execution_context=metadata['execution_context'], 
                        annotations= metadata['annotations'], 
                        stream_type=1,
                        data=data_quality_analysis) 
                try:
                    self.CC.save_stream(quality_ds)
                except Exception as e:
                    print(e)



    def get_corrupt_data(self, data,
                          admission_control = None):
        """
        Return the filtered list of DataPoints according to the admission control provided

        :param List(DataPoint) data: Input data list
        :param Callable[[Any], bool] admission_control: Admission control lambda function, which accepts the sample and
                returns a bool based on the data sample validity
        :return: Filtered list of DataPoints
        :rtype: List(DataPoint)
        """
        if admission_control is None:
            return []
        corrupt_data = []
        for d in data:
            if type(d.sample) is list:
                if len(d.sample) == 1:
                    if not admission_control(d.sample[0]):
                        corrupt_data.append(d)
                else:
                    corrupt_data.append(d)
            elif not admission_control(d.sample):
                corrupt_data.append(d)

        return corrupt_data




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

#userids = ['20940a76-976b-446e-b173-89237835ae6b']

#  20180401 20940a76-976b-446e-b173-89237835ae6b

print("Number of users ",len(userids))

psa = PhoneStreamsAnalyzer()
psa.analyze_all_users(userids, all_days, CC_CONFIG_FILEPATH)
