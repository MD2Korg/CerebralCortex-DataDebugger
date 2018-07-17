# Copyright (c) 2018, MD2K Center of Excellence
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
import argparse

from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.util.spark_helper import get_or_create_sc
from cerebralcortex.core.config_manager.config import Configuration

from core.app_availability_marker import mobile_app_availability_marker
from core.attachment_marker.motionsense import \
    attachment_marker as ms_attachment_marker
from core.battery_data_marker import battery_marker
from core.packet_loss_marker import packet_loss_marker
from core.sensor_availablity_marker.motionsense import \
    sensor_availability as ms_wd
from core.sensor_failure_marker.motionsense import sensor_failure_marker


def one_user_data(user_id: uuid, md_config, CC, spark_context):
    # get all streams for a participant
    """
    Diagnose one participant streams only
    :param user_id: list containing only one
    """
    if user_id:
        rdd = spark_context.parallelize([user_id])
        results = rdd.map(
            lambda user: diagnose_streams(user, CC, md_config))
        results.count()
    else:
        print("User id cannot be empty.")


def all_users_data(study_name: str, md_config, CC, spark_context):
    """
    Diagnose all participants' streams
    :param study_name:
    """
    # get all participants' name-ids
    all_users = CC.get_all_users(study_name)

    if all_users:
        rdd = spark_context.parallelize(all_users)
        results = rdd.map(
            lambda user: diagnose_streams(user["identifier"], CC, md_config))
        results.count()
    else:
        print(study_name, "- study has 0 users.")


def diagnose_streams(owner_id: uuid, CC: CerebralCortex, config: dict):
    """
    Contains pipeline execution of all the diagnosis algorithms
    :param owner_id:
    :param CC:
    :param config:
    """

    # get all the streams belong to a participant
    streams = CC.get_user_streams(owner_id)
    if streams and len(streams) > 0:

        # phone battery
        battery_marker(streams, owner_id, config["stream_names"]["phone_battery"], CC, config)
        # autosense battery
        battery_marker(streams, owner_id, config["stream_names"]["autosense_battery"], CC, config)

        # TODO: Motionsense battery values are not available.
        # TODO: Uncomment following code when the motionsense battery values are available
        #battery_marker(streams, owner_id, config["stream_names"]["motionsense_hrv_battery_right"], CC, config)
        #battery_marker(streams, owner_id, config["stream_names"]["motionsense_hrv_battery_left"], CC, config)

        # mobile phone availability marker
        mobile_app_availability_marker(streams, streams[config["stream_names"]["phone_battery"]]["name"], owner_id, CC, config)

        # Sensor failure
        sensor_failure_marker(streams, "right", owner_id,  CC, config)
        sensor_failure_marker(streams, "left", owner_id,  CC, config)

        # Motionsense (ms) wireless disconnection (wd)
        ms_wd(streams, "right" , owner_id , CC, config)
        ms_wd(streams, "left" , owner_id , CC, config)

        # Attachment marker
        ms_attachment_marker(streams,"right", owner_id , CC, config)
        ms_attachment_marker(streams,"left", owner_id , CC, config)

        # Packet-loss marker
        packet_loss_marker(streams,"right", "accel", owner_id, CC, config)
        packet_loss_marker(streams,"left", "accel", owner_id, CC, config)
        packet_loss_marker(streams,"right", "gyro", owner_id, CC, config)
        packet_loss_marker(streams,"left", "gyro", owner_id, CC, config)





if __name__ == '__main__':
    # create and load CerebralCortex object and configs
    parser = argparse.ArgumentParser(description='CerebralCortex Kafka Message Handler.')
    parser.add_argument("-cc", "--cc_config_filepath", help="Configuration file path", required=True)
    parser.add_argument("-mdc", "--mdebugger_config_filepath", help="mDebugger configuration file path", required=True)
    args = vars(parser.parse_args())

    CC = CerebralCortex(args["cc_config_filepath"])

    # load data diagnostic configs
    md_config = Configuration(args["mdebugger_config_filepath"]).config

    # get/create spark context
    spark_context = get_or_create_sc(type="sparkContext")

    # run for one participant
    # DiagnoseData().one_user_data(["cd7c2cd6-d0a3-4680-9ba2-0c59d0d0c684"], md_config, CC, spark_context)

    # run for all the participants in a study
    all_users_data("mperf", md_config, CC, spark_context)
