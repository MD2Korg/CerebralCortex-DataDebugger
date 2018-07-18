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
import pyarrow as pa
import datetime
from cerebralcortex.core.datatypes.datastream import DataStream

stream_adm_control = {} # total number of columns that are expected

stream_adm_control['CU_CALL_DURATION--edu.dartmouth.eureka'] = lambda x: (type(x) is float and x >= 0)
stream_adm_control['CU_SMS_LENGTH--edu.dartmouth.eureka'] =  lambda x: (type(x) is float and x >= 0)
stream_adm_control['PROXIMITY--org.md2k.phonesensor--PHONE'] = lambda x: (type(x) is float and x >= 0)
stream_adm_control['CU_APPUSAGE--edu.dartmouth.eureka'] = lambda x: type(x) is str
stream_adm_control['AMBIENT_LIGHT--org.md2k.phonesensor--PHONE'] = lambda x: (type(x) is float and x >= 0)
stream_adm_control['CU_CALL_NUMBER--edu.dartmouth.eureka'] = lambda x: (type(x) is str)
stream_adm_control['CU_SMS_NUMBER--edu.dartmouth.eureka'] = lambda x: (type(x) is str)
stream_adm_control['ACTIVITY_TYPE--org.md2k.phonesensor--PHONE'] = lambda x:(type(x) is list and len(x) == 2) 
stream_adm_control['CU_CALL_TYPE--edu.dartmouth.eureka'] = lambda x: (type(x) is float) 
stream_adm_control['CU_SMS_TYPE--edu.dartmouth.eureka'] = lambda x: (type(x) is float)
stream_adm_control['LOCATION--org.md2k.phonesensor--PHONE'] = lambda x: ( isinstance(x, list) and len(x) == 6)
stream_adm_control['GEOFENCE--LIST--org.md2k.phonesensor--PHONE'] = lambda x: (isinstance(x, str) and '#' in x)


                                                                                                       
date_format = '%Y%m%d'
                                                               
f = open('all_corrupt_files.txt','r')

hdfs = pa.hdfs.connect('dagobah10dot.memphis.edu', 8020)

output_dir = '/cerebralcortex/anand/'

if not hdfs.exists(output_dir):
    hdfs.mkdir(output_dir)
    print('created outputdir', output_dir)

for line in f:
    uploaded_fp = gzip.open(line.split('\t')[1].strip())
    start_date = None
    end_date = None
    for updp_line in uploaded_fp:
        splts = updp_line.decode().split(',')
        tm_stamp = int(splts[0])
        if start_date is None:
            start_date = datetime.datetime.fromtimestamp(tm_stamp/1000)
        end_date = datetime.datetime.fromtimestamp(tm_stamp/1000)


    splits = line.split('/')
    splits = [x.strip() for x in splits]
    stream_name = splits[0]
    userid = splits[4]
    date = splits[5]
    streamid = splits[6]
    all_days = []
    while True:
        all_days.append(start_date)
        start_date += datetime.timedelta(days = 1)
        if start_date > end_date:
            break

    base_dir = '/cerebralcortex/data'
    files_dir = os.path.join(base_dir, userid)
    files_dir = os.path.join(files_dir, streamid)
    
    for corrupt_day in all_days:
        filename = corrupt_day.strftime(date_format) + '.gz'
        file_path = os.path.join(files_dir, filename)
        if hdfs.exists(file_path):
            clean_datapoints = []
            
            fp = hdfs.open(file_path)
            contents = fp.read()
            contents = gzip.decompress(contents)
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

            out_path = file_path.replace('/cerebralcortex/data',
                                         '/cerebralcortex/anand')

            new_fp = hdfs.open(out_path, 'wb')
            clean_contents = pickle.dumps(clean_datapoints)
            clean_contents = gzip.compress(clean_contents)
            new_fp.write(clean_contents)
            new_fp.close()
    break #FIXME

f.close()
