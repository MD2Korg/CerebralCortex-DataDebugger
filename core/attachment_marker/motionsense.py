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

from cerebralcortex.cerebralcortex import CerebralCortex
from core.post_processing import get_execution_context, get_annotations
from core.post_processing import store
from core.util.window import merge_consective_windows, window
from core.util.helper_methods import generate_dd_stream_uuid
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet


def attachment_marker(all_streams, wrist, owner_id: uuid, CC: CerebralCortex, config: dict):
    """
    Label sensor data as sensor-on-body, sensor-off-body, or improper-attachment.
    All the labeled data (st, et, label) with its metadata are then stored in a datastore

    """
    marker_version = "0.0.1"

    # TODO: quality streams could be multiple so find the one computed with CC
    # using stream_id, data-diagnostic-stream-id, and owner id to generate a unique stream ID for battery-marker

    key0 = "motionsense_hrv_led_quality_"+wrist
    key1 = "motionsense_hrv_"+wrist+"_attachment_marker"
    raw_stream_ids = all_streams[config["stream_names"][key0]]["stream_ids"]
    stream_name = all_streams[config["stream_names"][key0]]["name"]
    dd_stream_name = config["stream_names"][key1]

    if config["stream_names"][key0] in all_streams:

        attachment_marker_stream_id = generate_dd_stream_uuid(dd_stream_name, marker_version, owner_id, "ATTACHMENT MARKER")
        input_streams = [{"owner_id": owner_id, "id": raw_stream_ids, "name": stream_name}]
        output_stream = {"id": attachment_marker_stream_id, "name": dd_stream_name,
                         "algo_type": config["algo_type"]["attachment_marker"]}
        metadata = get_metadata(dd_stream_name, input_streams, config)

        if isinstance(raw_stream_ids, list):
            for raw_stream_id in raw_stream_ids:
                stream_days = CC.get_stream_days(raw_stream_id, attachment_marker_stream_id, CC)

                for day in stream_days:
                    try:
                        # load stream data to be diagnosed
                        raw_stream = CC.get_stream(raw_stream_id, day=day, data_type=DataSet.COMPLETE)

                        if len(raw_stream.data) > 0:
                            windowed_data = window(raw_stream.data, config['general']['window_size'], True)
                            results = process_windows(windowed_data, config)
                            merged_windows = merge_consective_windows(results)

                            store(merged_windows, input_streams, output_stream, metadata, CC, config)
                    except Exception as e:
                        CC.logging.log("Error processing: owner-id: %s, stream-id: %s, stream-name: %s, day: %s. Error: "
                                       %(str(owner_id), str(raw_stream_id), str(stream_name), str(day), str(e)))


def process_windows(windowed_data: OrderedDict, config: dict) -> OrderedDict:
    """
    :param windowed_data:
    :param config:
    :return:
    """
    results = OrderedDict()

    threshold_improper_attachment = config['attachment_marker']['motionsense_improper_attachment']
    threshold_onbody = config['attachment_marker']['motionsense_onbody']
    threshold_offbody = config['attachment_marker']['motionsense_offbody']

    label_improper_attachment = config['labels']['motionsense_improper_attachment']
    label_onbody = config['labels']['motionsense_onbody']
    label_offbody = config['labels']['motionsense_offbody']

    if windowed_data:
        for key, data in windowed_data.items():
            one_minute_window = 0
            for k in data:
                if k.sample == 0:
                    one_minute_window += 1
            if (one_minute_window / 20) > threshold_offbody and (
                one_minute_window / 20) < threshold_improper_attachment:
                results[key] = label_improper_attachment
            elif (one_minute_window / 20) > threshold_onbody:
                results[key] = label_onbody
            else:
                results[key] = label_offbody
        return results


def get_metadata(dd_stream_name: str, input_streams: dict, config: dict) -> dict:
    """
    :param generated_stream_id:
    :param dd_stream_name:
    :param input_streams:
    :param config:
    :return:
    """
    if dd_stream_name == config["stream_names"]["autosense_rip_attachment_marker"]:
        input_param = {"window_size": config["general"]["window_size"],
                       "onbody_threshold": config["attachment_marker"]["rip_on_body"],
                       "improper_attachment": config["attachment_marker"]["improper_attachment"]}
        data_descriptor = {"NAME": dd_stream_name, "DATA_TYPE": "int",
                           "DESCRIPTION": "Attachment labels: Improper attachment: " + str(
                               config["labels"]["rip_improper_attachment"]) + ", Offbody: " + str(
                               config["labels"]["rip_off_body"]) + ", Onbody: " + str(config["labels"]["rip_on_body"])}
    elif dd_stream_name == config["stream_names"]["autosense_ecg_attachment_marker"]:
        input_param = {"window_size": config["general"]["window_size"],
                       "ecg_vairance_threshold": config["attachment_marker"]["ecg_on_body"],
                       "improper_attachment": config["attachment_marker"]["improper_attachment"]}
        data_descriptor = {"NAME": dd_stream_name, "DATA_TYPE": "int",
                           "DESCRIPTION": "Attachment labels: Improper attachment: " + str(
                               config["labels"]["ecg_improper_attachment"]) + ", Offbody: " + str(
                               config["labels"]["ecg_off_body"]) + ", Onbody: " + str(config["labels"]["ecg_on_body"])}
    elif dd_stream_name == config["stream_names"]["motionsense_hrv_right_attachment_marker"] or dd_stream_name == \
            config["stream_names"]["motionsense_hrv_left_attachment_marker"]:
        input_param = {"window_size": config["general"]["window_size"],
                       "motionsense_improper_attachment_threshold": config["attachment_marker"][
                           "motionsense_improper_attachment"],
                       "motionsense_onbody_threshold": config["attachment_marker"]["motionsense_onbody"],
                       "motionsense_offbody_threshold": config["attachment_marker"]["motionsense_offbody"]
                       }
        data_descriptor = {"NAME": dd_stream_name, "DATA_TYPE": "int",
                           "DESCRIPTION": "Attachment labels: Improper attachment: " + str(
                               config["labels"]["motionsense_improper_attachment"]) + ", Offbody: " + str(
                               config["labels"]["motionsense_offbody"]) + ", Onbody: " + str(
                               config["labels"]["motionsense_onbody"])}
    else:
        raise ValueError("Incorrect sensor type")

    method = 'cerebralcortex.data_processor.data_diagnostic.attachment_marker'
    algo_description = config["description"]["attachment_marker"]
    ec = get_execution_context(dd_stream_name, input_param, input_streams, method,
                               algo_description, config)
    anno = get_annotations()
    return {"ec": ec, "dd": data_descriptor, "anno": anno}
