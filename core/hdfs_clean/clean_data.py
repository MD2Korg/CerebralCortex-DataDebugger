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

import pickle
import os
import gzip
from cerebralcortex.core.datatypes.datastream import DataStream

stream_adm_control = {} # total number of columns that are expected

stream_adm_control['CU_CALL_DURATION--edu.dartmouth.eureka'] = 3
stream_adm_control['CU_SMS_LENGTH--edu.dartmouth.eureka'] = 3
stream_adm_control['PROXIMITY--org.md2k.phonesensor--PHONE'] = 3
stream_adm_control['CU_APPUSAGE--edu.dartmouth.eureka'] = lambda x: type(x) is str
stream_adm_control['AMBIENT_LIGHT--org.md2k.phonesensor--PHONE'] = 3
stream_adm_control['CU_CALL_NUMBER--edu.dartmouth.eureka'] = 3
stream_adm_control['CU_SMS_NUMBER--edu.dartmouth.eureka'] = 3
stream_adm_control['ACTIVITY_TYPE--org.md2k.phonesensor--PHONE'] = 4
stream_adm_control['CU_CALL_TYPE--edu.dartmouth.eureka'] = 3
stream_adm_control['CU_SMS_TYPE--edu.dartmouth.eureka'] = 3
stream_adm_control['LOCATION--org.md2k.phonesensor--PHONE'] = 8
stream_adm_control['GEOFENCE--LIST--org.md2k.phonesensor--PHONE'] = 3



f = open('all_corrupt_files.txt','r')

for line in f:
    splits = line.split('/')
    splits = [x.strip() for x in splits]
    stream_name = splits[0]
    userid = splits[4]
    date = splits[5]
    streamid = splits[6]
    
    
    files_dir = '00ab666c-afb8-476e-9872-6472b4e66b68/b2e7bd24-0e33-3f2f-bc0a-96d6d689c6d1/'#FIXME
    filename = date + '.gz'
    file_path = os.path.join(files_dir, filename)

    if os.path.exists(file_path):
        clean_datapoints = []
        fp = gzip.open(file_path)
        contents = fp.read()
        fp.close()
        data_points = pickle.loads(contents)
        for dp in data_points:
            sample = dp.sample
            adm_controlf = stream_adm_control[stream_name]
            if adm_controlf(sample):
                clean_datapoints.append(dp)
            else:
                if isinstance(sample, list) and len(sample) == 1 and adm_controlf(sample[0]):
                    clean_datapoints.append(dp)

        print('corrupt len',len(data_points))
        print('cleaned len',len(clean_datapoints))
        new_fp = gzip.open(file_path, 'wb')
        new_fp.write(pickle.dumps(clean_datapoints))
        new_fp.close()
        # Test
        test_fp = gzip.open(file_path)
        cleaned_data = test_fp.read()
        print('Num cleaned data points', len(pickle.loads(cleaned_data)))

    break

f.close()
