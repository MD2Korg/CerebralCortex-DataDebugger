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


import glob
import sys
import csv
from os import scandir
import os
from pprint import pprint
from collections import OrderedDict
from functools import reduce
import re
import gzip
import datetime
import json
from pprint import pprint
from multiprocessing import Pool
from cerebralcortex.cerebralcortex import CerebralCortex


UUID_re = re.compile("^([a-z0-9]+-){4}[a-z0-9]+$")
#directory=sys.argv[1]

stream_lengths = {} # total number of columns that are expected

stream_lengths['CU_CALL_DURATION--edu.dartmouth.eureka'] = 3
stream_lengths['CU_SMS_LENGTH--edu.dartmouth.eureka'] = 3
stream_lengths['PROXIMITY--org.md2k.phonesensor--PHONE'] = 3
stream_lengths['CU_APPUSAGE--edu.dartmouth.eureka'] = 3
stream_lengths['AMBIENT_LIGHT--org.md2k.phonesensor--PHONE'] = 3
stream_lengths['CU_CALL_NUMBER--edu.dartmouth.eureka'] = 3
stream_lengths['CU_SMS_NUMBER--edu.dartmouth.eureka'] = 3
stream_lengths['ACTIVITY_TYPE--org.md2k.phonesensor--PHONE'] = 4
stream_lengths['CU_CALL_TYPE--edu.dartmouth.eureka'] = 3
stream_lengths['CU_SMS_TYPE--edu.dartmouth.eureka'] = 3
stream_lengths['LOCATION--org.md2k.phonesensor--PHONE'] = 8
stream_lengths['GEOFENCE--LIST--org.md2k.phonesensor--PHONE'] = 3

corrupt_files_buf = ''
total_files = 0

def scan_dir_for_corrupt_files(upload_dir_path, stream_name):
    global corrupt_files_buf
    global total_files
    files = os.listdir(upload_dir_path)
    for gzf in files:
        if gzf[-3:] == '.gz':
            gzfilepath = os.path.join(upload_dir_path, gzf)
            total_files += 1
            try:
                gzfp = gzip.open(gzfilepath)
                for line in gzfp:
                    line_splits = len(line.decode().split(','))
                    if line_splits != stream_lengths[stream_name]:
                        corrupt_files_buf += stream_name
                        corrupt_files_buf += '\t'
                        corrupt_files_buf += gzfilepath
                        corrupt_files_buf += '\n'
                    
                    break
            except Exception as e:
                corrupt_files_buf += stream_name
                corrupt_files_buf += '\t'
                corrupt_files_buf += gzfilepath
                corrupt_files_buf += '\n'
                    



if __name__ == '__main__':
    CC_CONFIG_FILEPATH="/cerebralcortex/code/config/cc_starwars_configuration.yml"
    base_path = '/md2k/apiserver/data/'
    base_path2 = '/md2k2/apiserver/data/'
    CC = CerebralCortex(CC_CONFIG_FILEPATH)
    stream_id_map = {}


    '''
    participants = []
    for f in scandir(directory):
        if f.is_dir and UUID_re.match(f.name):
            participants.append(f.name)
    '''

    f = open('all_users_corruption.txt', 'r')
    skip_line = True

    for line in f:
        if skip_line:
            skip_line = False
            continue
        line = line.strip()
        splits = line.split('\t')

        userid = splits[0]
        stream_name = splits[1]
        stream_name = stream_name.replace('_corrupt_data','')
        stream_ids = None
        if userid in stream_id_map and stream_name in stream_id_map[userid]:
            stream_ids = stream_id_map[userid][stream_name]
        else:
            stream_id_tmp = CC.get_stream_id(userid, stream_name)
            stream_ids = [x['identifier'] for x in stream_id_tmp]
            if userid in stream_id_map:
                stream_id_map[userid][stream_name] = stream_ids
            else:
                stream_id_map[userid] = {}
                stream_id_map[userid][stream_name] = stream_ids

        date = splits[2]
        total_corrupt_dp = int(splits[4])

        if total_corrupt_dp:
            usr_dir_path = os.path.join(base_path, userid)
            day_path = os.path.join(usr_dir_path, date)

            for strmid in stream_ids:
                upload_dir_path = os.path.join(day_path,strmid)
                if os.path.exists(upload_dir_path):
                    scan_dir_for_corrupt_files(upload_dir_path, stream_name)

            usr_dir_path = os.path.join(base_path2, userid)
            day_path = os.path.join(usr_dir_path, date)
            for strmid in stream_ids:
                upload_dir_path = os.path.join(day_path,strmid)
                if os.path.exists(upload_dir_path):
                    scan_dir_for_corrupt_files(upload_dir_path, stream_name)

    corrf = open('all_corrupt_files.txt', 'w')
    corrf.write(corrupt_files_buf)
    corrf.close()

    print("DONE")
    print("Total files scanned ", total_files)
