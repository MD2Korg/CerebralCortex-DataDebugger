# Copyright (c) 2017, MD2K Center of Excellence
# - Nasir Ali <nasir.ali08@gmail.com>
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

import uuid
from collections import OrderedDict
from typing import List

import numpy as np

from cerebralcortex.cerebralcortex import CerebralCortex
from core.post_processing import get_execution_context, get_annotations
from core.post_processing import store
from core.util.window import merge_consective_windows, window
from core.util.helper_methods import generate_dd_stream_uuid
from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet


def sensor_failure_marker(all_streams, wrist: str, owner_id: uuid, CC: CerebralCortex, config: dict):
    """
    Label a window as packet-loss if received packets are less than the expected packets.
    All the labeled data (st, et, label) with its metadata are then stored in a datastore.
    :param stream_id:
    :param CC_obj:
    :param config:
    """
    marker_version = "0.0.1"

    key0 = "motionsense_hrv_"+wrist+"_attachment_marker"
    key1 = "motionsense_hrv_"+wrist+"_attachment_marker"
    key2 = "motionsense_hrv_accel_"+wrist
    key3 = "motionsense_hrv_gyro_"+wrist
    key4 = "motionsense_hrv_"+wrist+"_sensor_failure_marker"

    stream_name = all_streams[config["stream_names"][key0]]["name"]
    raw_stream_ids = all_streams[config["stream_names"][key1]]["stream_ids"]
    mshrv_accel_id = all_streams[config["stream_names"][key2]]["stream_ids"]
    mshrv_gyro_id = all_streams[config["stream_names"][key3]]["stream_ids"]
    dd_stream_name = config["stream_names"][key4]

    if config["stream_names"][key2] in all_streams:

        # using stream_id, data-diagnostic-stream-id, and owner id to generate a unique stream ID for battery-marker
        sensor_failure_stream_id = generate_dd_stream_uuid(dd_stream_name, marker_version, owner_id, "SENSOR FAILURE MARKER")
        input_streams = [{"owner_id": owner_id, "id": raw_stream_ids,
                          "name": stream_name}]
        output_stream = {"id": sensor_failure_stream_id, "name": dd_stream_name,
                         "algo_type": config["algo_type"]["sensor_failure"]}
        metadata = get_metadata(dd_stream_name, input_streams, config)


        if isinstance(raw_stream_ids, list):
            for raw_stream_id in raw_stream_ids:
                stream_days = CC.get_stream_days(raw_stream_id, sensor_failure_stream_id, CC)
                for day in stream_days:
                    try:
                        # load stream data to be diagnosed
                        attachment_marker_stream = CC.get_stream(raw_stream_id, day, data_type=DataSet.COMPLETE)
                        results = OrderedDict()
                        if attachment_marker_stream.data:
                            for marker_window in attachment_marker_stream.data:
                                if "MOTIONSENSE-ON-BODY" in marker_window.sample:
                                    mshrv_accel_stream = CC.get_stream(mshrv_accel_id, day,
                                                                           start_time=marker_window.start_time,
                                                                           end_time=marker_window.end_time, data_type=DataSet.ONLY_DATA)
                                    mshrv_gyro_stream = CC.get_stream(mshrv_gyro_id, day,
                                                                          start_time=marker_window.start_time,
                                                                          end_time=marker_window.end_time, data_type=DataSet.ONLY_DATA)

                                results_accel = process_windows(mshrv_accel_stream, config)
                                results_gyro = process_windows(mshrv_gyro_stream, config)

                                key = marker_window.start_time, marker_window.end_time

                                # if sensor failure period is more than 12 hours then mark it as a sensor failure
                                if results_accel > 0 and results_gyro < 1:
                                    sample = "MOTIONSENE-HRV-" + str(wrist) + "ACCELEROMETER-FAILURE"
                                    results[key].append(DataPoint(marker_window.start_time, marker_window.end_time, sample))
                                elif results_accel < 1 and results_gyro > 0:
                                    sample = "MOTIONSENE-HRV-" + str(wrist) + "GYRO-FAILURE"
                                    results[key].append(DataPoint(marker_window.start_time, marker_window.end_time, sample))

                            if len(results) > 0:
                                merged_windows = merge_consective_windows(results)
                                store(merged_windows, input_streams, output_stream, metadata, CC, config)
                    except Exception as e:
                        CC.logging.log("Error processing: owner-id: %s, stream-id: %s, Algo-name: %s, day: %s. Error: "
                                       % (str(owner_id), str(raw_stream_id), "sensor_failure_marker", str(day), str(e)))


def process_windows(windowed_data: List, config: dict) -> int:
    """

    :param windowed_data:
    :param config:
    :return:
    """
    total_failures = 0

    for data in windowed_data:
        dp = []
        try:
            sample = float(data.sample[0])
            dp.append(sample)
        except Exception as e:
            print(e)
    if dp:
        signal_var = np.var(dp)
        if signal_var < config["sensor_failure"]["threshold"]:
            total_failures += 1

    return total_failures


def get_metadata(dd_stream_name: str, input_streams: dict, config: dict) -> dict:
    """

    :param dd_stream_name:
    :param input_streams:
    :param config:
    :return:
    """
    input_param = {"window_size": "21600"}
    if dd_stream_name == config["stream_names"]["motionsense_hrv_right_sensor_failure_marker"] or dd_stream_name == \
            config["stream_names"]["motionsense_hrv_left_sensor_failure_marker"]:
        label = config["labels"]["motionsense_failure"]
    elif dd_stream_name == dd_stream_name == config["stream_names"]["phone_sensor_failure_marker"]:
        label = config["labels"]["phone_sensor_failure"]
    elif dd_stream_name == dd_stream_name == config["stream_names"]["autosense_sensor_failure_marker"]:
        label = config["labels"]["autosense_sensor_failure"]
    else:
        raise ValueError("Incorrect sensor type")

    data_descriptor = {"NAME": dd_stream_name, "DATA_TYPE": "int",
                       "DESCRIPTION": "sensor failure detection: " + str(label)}
    algo_description = config["description"]["sensor_failure"]
    method = 'cerebralcortex.data_processor.data_diagnostic.sensor_failure'
    ec = get_execution_context(dd_stream_name, input_param, input_streams, method,
                               algo_description, config)
    anno = get_annotations()
    return {"ec": ec, "dd": data_descriptor, "anno": anno}
