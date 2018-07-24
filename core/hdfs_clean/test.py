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



date_format = '%Y%m%d'
                                                               
f = open('all_corrupt_files.txt','r')
processed_dirs = [] 
for line in f:
    file_path = line.split('\t')[1].strip()
    dir_path = file_path[:-len('56f32620-ca10-41b0-ac36-4b55a7a826cf.gz')]
    if dir_path not in processed_dirs:
        processed_dirs.append(dir_path)
    
f.close()
print(len(processed_dirs))



"""
hdfs = pa.hdfs.connect('dagobah10dot.memphis.edu', 8020)
base_dir = '/cerebralcortex/anand1/'

userids = hdfs.ls(base_dir)
total_files = 0

for user in userids:
    user_dir = os.path.join(base_dir, user)
    streamids = hdfs.ls(user_dir)
    for stream_id in streamids:
        stream_dir = os.path.join(user_dir, stream_id)
        clean_files = hdfs.ls(stream_dir)
        for cf in clean_files:
            if '.gz' in cf:
                total_files += 1


print('DONE', total_files)
hdfs.close()
"""
