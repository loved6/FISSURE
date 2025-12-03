# from comms.FissureZMQNode import *
# from fissure.comms.constants import *
# from fissure_libutils import *
from typing import List

import binascii
import fissure.comms
import fissure.utils
import fissure.utils.library
from fissure.utils.common import PLUGIN_DIR
from fissure.utils import plugin
from fissure.utils import plugin_editor
import os
import time
import yaml
import asyncio
import socket
import shutil
from fissure.Listeners import (
    MeshtasticListener,
    FilesystemListener,
    ZMQSubscriberListener,
    WebsitePollerListener,
    SerialPortListener,
    TCPUDPListener,
    MQTTListener
)


""" HiprFisr Specific Callback Functions """

# DELAY_SHORT = 0.25  # seconds

async def findGPS_CoordinatesLT(component: object, tab_index=0, gps_source="", format=""):
    """
    Queries the remote sensor node for its GPS coordinates. 
    """
    # Forward the Message
    PARAMETERS = {
        "tab_index": tab_index,
        "gps_source": gps_source,
        "format": format
    }
    msg = {
        fissure.comms.MessageFields.SOURCE: component.identifierLT,
        fissure.comms.MessageFields.DESTINATION: tab_index,                
        fissure.comms.MessageFields.MESSAGE_NAME: "findGPS_CoordinatesLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    await component.sensor_nodes[int(tab_index)].listener.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def findGPS_CoordinatesResultsLT(component: object, tab_index=0, coordinates=""):
    """
    Forwards the GPS coordinate results message to the Dashboard.
    """
    PARAMETERS = {"tab_index": tab_index, "coordinates": coordinates}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "findGPS_CoordinatesResultsLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def recallInfoMeshtasticReturnLT(component: object, tab_index="", nickname="", location="", notes=""):
    """
    Returns the recalled sensor node settings to the Dashboard.
    """
    # Send the Message
    PARAMETERS = {
        "tab_index": tab_index,
        "nickname": nickname,
        "location": location,
        "notes": notes
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "recallInfoMeshtasticReturnLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def recallHardwareMeshtasticReturnLT(component: object, tsi={}):
    """
    Returns the recalled sensor node settings to the Dashboard.
    """
    print("MADE IT TO HIPRFISR CALLBACKS")
    print(tsi)
    
    # Send the Message
    PARAMETERS = {"tsi": tsi}  # To Do
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "recallHardwareMeshtasticReturnLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)  # To Do


async def recallInfoMeshtasticLT(component: object, tab_index=""):
    """
    Recalls information from the sensor node config file.
    """
    component.logger.info(f"Recalling settings for sensor node {tab_index}...")
    PARAMETERS = {"tab_index": tab_index}
    msg = {
        fissure.comms.MessageFields.SOURCE: component.identifierLT,
        fissure.comms.MessageFields.DESTINATION: tab_index,                
        fissure.comms.MessageFields.MESSAGE_NAME: "recallInfoMeshtasticLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.sensor_nodes[int(tab_index)].listener.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def recallHardwareMeshtasticLT(component: object, tab_index=""):
    """
    Recalls information from the sensor node config file.
    """
    component.logger.info(f"Recalling hardware settings for sensor node {tab_index}...")
    PARAMETERS = {"sensor_node_id": tab_index}
    msg = {
        fissure.comms.MessageFields.SOURCE: component.identifierLT,
        fissure.comms.MessageFields.DESTINATION: tab_index,                
        fissure.comms.MessageFields.MESSAGE_NAME: "recallHardwareMeshtasticLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.sensor_nodes[int(tab_index)].listener.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def recallStatusMeshtasticLT(component: object, tab_index=""):
    """
    Recalls information from the sensor node config file.
    """
    component.logger.info(f"Recalling settings for sensor node {tab_index}...")
    PARAMETERS = {"tab_index": tab_index}
    msg = {
        fissure.comms.MessageFields.SOURCE: component.identifierLT,
        fissure.comms.MessageFields.DESTINATION: tab_index,                
        fissure.comms.MessageFields.MESSAGE_NAME: "recallStatusMeshtasticLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.sensor_nodes[int(tab_index)].listener.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def recallStatusMeshtasticReturnLT(component: object, tab_index="", status=""):
    """
    Returns the recalled sensor node settings to the Dashboard.
    """
    # Send the Message
    PARAMETERS = {
        "tab_index": tab_index,
        "status": status,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "recallStatusMeshtasticReturnLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def scanHardwareLT(component: object, tab_index=0, hardware_list=[]):
    """
    Sends a message to a sensor node to scan for hardware information.
    """
    # Forward the Message
    PARAMETERS = {"tab_index": tab_index, "hardware_list": hardware_list}
    msg = {
        fissure.comms.MessageFields.SOURCE: component.identifierLT,
        fissure.comms.MessageFields.DESTINATION: tab_index,                
        fissure.comms.MessageFields.MESSAGE_NAME: "scanHardwareLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.sensor_nodes[int(tab_index)].listener.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def probeHardwareLT(component: object, tab_index, table_row_text):
    """
    Sends a message to a sensor node to probe select hardware.
    """
    # Forward the Message
    PARAMETERS = {"tab_index": tab_index, "table_row_text": table_row_text}
    msg = {
        fissure.comms.MessageFields.SOURCE: component.identifierLT,
        fissure.comms.MessageFields.DESTINATION: tab_index,                
        fissure.comms.MessageFields.MESSAGE_NAME: "probeHardwareLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.sensor_nodes[int(tab_index)].listener.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def hardwareProbeResultsLT(component: object, tab_index=0, output="", height_width=[]):
    """
    Forwards the hardware probe results message to the Dashboard.
    """
    # PARAMETERS = {"tab_index": tab_index, "output": eval(f'"{output}"'), "height_width": height_width}
    PARAMETERS = {"tab_index": tab_index, "output": output, "height_width": height_width}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "hardwareProbeResultsLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def hardwareScanResultsLT(component: object, tab_index=0, hardware_scan_results=[]):
    """
    Forwards the hardware scan results message to the Dashboard.
    """
    PARAMETERS = {"tab_index": tab_index, "hardware_scan_results": hardware_scan_results}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "hardwareScanResultsLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def guessHardwareLT(component: object, tab_index=0, table_row=0, table_row_text=[], guess_index=0):
    """
    Sends a message to a sensor node to guess details for select hardware.
    """
    # Forward the Message
    PARAMETERS = {
        "tab_index": tab_index,
        "table_row": table_row,
        "table_row_text": table_row_text,
        "guess_index": guess_index,
    }
    msg = {
        fissure.comms.MessageFields.SOURCE: component.identifierLT,
        fissure.comms.MessageFields.DESTINATION: tab_index,                
        fissure.comms.MessageFields.MESSAGE_NAME: "guessHardwareLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.sensor_nodes[int(tab_index)].listener.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def hardwareGuessResultsLT(component: object, results=[]):
    """
    Forwards sensnor node hardware guess results from HIPRFISR to Dashboard.
    """
    PARAMETERS = {
        "tab_index": results[0],
        "table_row": results[1],
        "hardware_type": results[2],
        "scan_results": results[3],
        "new_guess_index": results[4],
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "hardwareGuessResultsLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def autorunPlaylistExecuteLT(component: object, sensor_node_id=0, playlist_filename=""):
    """
    Signals to sensor node to start autorun playlist already located on the sensor node.
    """
    # Send Message
    PARAMETERS = {"sensor_node_id": sensor_node_id, "playlist_filename": playlist_filename}
    msg = {
        fissure.comms.MessageFields.SOURCE: component.identifierLT,
        fissure.comms.MessageFields.DESTINATION: sensor_node_id,                
        fissure.comms.MessageFields.MESSAGE_NAME: "autorunPlaylistExecuteLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.sensor_nodes[sensor_node_id].listener.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def autorunPlaylistStopLT(component: object, sensor_node_id=0):
    """
    Signals to sensor node to stop autorun playlist.
    """
    # Send Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.SOURCE: component.identifierLT,
        fissure.comms.MessageFields.DESTINATION: sensor_node_id,                
        fissure.comms.MessageFields.MESSAGE_NAME: "autorunPlaylistStopLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.sensor_nodes[sensor_node_id].listener.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)
    

async def alertReturnLT(component: object, sensor_node_id=0, alert_text=""):
    """
    Forwards alertReturn Message to the Dashboard.
    """
    # Forward to Dashboard
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "alert_text": alert_text,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "alertReturnLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)
    
    
async def takPlotLT(component: object, msg=[]):
    """
    Forwards the GPS coordinate results message to Tak.
    """
    uid = str(msg[0])
    lat = float(msg[1])
    lon = float(msg[2])
    alt = float(msg[3])
    time = str(msg[4])
    remarks = str(msg[5])
    type = str(msg[6]) if len(msg) > 6 else "a-f-G-U-H"

    time = time.replace(" ", "T")
    
    prefix = component.settings['callsign_prefix']
    callsign = component.nodes[uid].get('callsign', f"{prefix}-{uid[:8]}")

    await component.send_cot(uid, callsign, lat, lon, alt, time, remarks, type)
    

async def exploitLT(component: object, msg=[]):
    """"
    Forwards the necesarry information to the proper exploit flow graph.
    """
    PARAMETERS = {
        "sensor_node_id": str(msg[0]),
        "protocol": str(msg[1]),
        "modulation": str(msg[2]),
        "hardware": str(msg[3]),
        "type": str(msg[4]),
        "attack": str(msg[5]),
        "variables": str(msg[6]),
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "exploitReturnLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)

    
async def takPlotGpsUpdateLT(component: object, msg=[]):
    """
    Forwards the GPS coordinate results message to TAK.
    """
    uid = str(msg[0])
    lat = float(msg[1])
    lon = float(msg[2])
    alt = float(msg[3])
    time = str(msg[4])
    remarks = str(msg[5])

    time = time.replace(" ", "T")

    max_history = 5
    
    prefix = component.settings['callsign_prefix']
    callsign = component.nodes[uid].get('callsign', f"{prefix}-{uid[:8]}")

    await component.sensor_node_tracker.send_cot_gps_update(uid, callsign, lat, lon, alt, time, remarks, max_history)


async def gpsBeaconEnableMeshtasticLT(component: object, sensor_node_id: str):
    """
    Signals to sensor node to enable the GPS TAK beacon.
    """
    # Send Message
    PARAMETERS = {}
    msg = {
        fissure.comms.MessageFields.SOURCE: component.identifierLT,
        fissure.comms.MessageFields.DESTINATION: sensor_node_id,                
        fissure.comms.MessageFields.MESSAGE_NAME: "gpsBeaconEnableMeshtasticLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.sensor_nodes[int(sensor_node_id)].listener.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def gpsBeaconDisableMeshtasticLT(component: object, sensor_node_id: str):
    """
    Signals to sensor node to disable the GPS TAK beacon.
    """
    # Send Message
    PARAMETERS = {}
    msg = {
        fissure.comms.MessageFields.SOURCE: component.identifierLT,
        fissure.comms.MessageFields.DESTINATION: sensor_node_id,                
        fissure.comms.MessageFields.MESSAGE_NAME: "gpsBeaconDisableMeshtasticLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.sensor_nodes[int(sensor_node_id)].listener.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def rebootMeshtasticLT(component: object, sensor_node_id: str):
    """
    Signals sensor node to reboot the computer.
    """
    # Send Message
    PARAMETERS = {}
    msg = {
        fissure.comms.MessageFields.SOURCE: component.identifierLT,
        fissure.comms.MessageFields.DESTINATION: sensor_node_id,                
        fissure.comms.MessageFields.MESSAGE_NAME: "rebootMeshtasticLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.sensor_nodes[int(sensor_node_id)].listener.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def uptimeMeshtasticLT(component: object, sensor_node_id: str):
    """
    Signals sensor node to retrieve the uptime of the computer.
    """
    # Send Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.SOURCE: component.identifierLT,
        fissure.comms.MessageFields.DESTINATION: sensor_node_id,                
        fissure.comms.MessageFields.MESSAGE_NAME: "uptimeMeshtasticLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.sensor_nodes[int(sensor_node_id)].listener.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def uptimeMeshtasticReturnLT(component: object, sensor_node_id: str, uptime: str):
    """
    Forwards uptimeMeshtasticReturnLT message to the Dashboard.
    """
    # Forward to Dashboard
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "uptime": uptime,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "uptimeMeshtasticReturnLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def memoryMeshtasticLT(component: object, sensor_node_id: str):
    """
    Signals sensor node to retrieve the memory usage of the computer.
    """
    # Send Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.SOURCE: component.identifierLT,
        fissure.comms.MessageFields.DESTINATION: sensor_node_id,                
        fissure.comms.MessageFields.MESSAGE_NAME: "memoryMeshtasticLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.sensor_nodes[int(sensor_node_id)].listener.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def memoryMeshtasticReturnLT(component: object, sensor_node_id: str, memory: list):
    """
    Forwards memoryMeshtasticReturnLT message to the Dashboard.
    """
    # Forward to Dashboard
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "memory": memory,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "memoryMeshtasticReturnLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def diskMeshtasticLT(component: object, sensor_node_id: str):
    """
    Signals sensor node to retrieve the disk usage of the computer.
    """
    # Send Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.SOURCE: component.identifierLT,
        fissure.comms.MessageFields.DESTINATION: sensor_node_id,                
        fissure.comms.MessageFields.MESSAGE_NAME: "diskMeshtasticLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.sensor_nodes[int(sensor_node_id)].listener.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def diskMeshtasticReturnLT(component: object, sensor_node_id: str, disk: dict):
    """
    Forwards diskMeshtasticReturnLT message to the Dashboard.
    """
    # Forward to Dashboard
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "disk": disk,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "diskMeshtasticReturnLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def cpuMeshtasticLT(component: object, sensor_node_id: str):
    """
    Signals sensor node to retrieve the CPU percentage of the computer.
    """
    # Send Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.SOURCE: component.identifierLT,
        fissure.comms.MessageFields.DESTINATION: sensor_node_id,                
        fissure.comms.MessageFields.MESSAGE_NAME: "cpuMeshtasticLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.sensor_nodes[int(sensor_node_id)].listener.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def cpuMeshtasticReturnLT(component: object, sensor_node_id: str, cpu: str):
    """
    Forwards cpuMeshtasticReturnLT message to the Dashboard.
    """
    # Forward to Dashboard
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "cpu": cpu,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "cpuMeshtasticReturnLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def processesMeshtasticLT(component: object, sensor_node_id: str):
    """
    Signals sensor node to retrieve the processes on the computer.
    """
    # Send Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.SOURCE: component.identifierLT,
        fissure.comms.MessageFields.DESTINATION: sensor_node_id,                
        fissure.comms.MessageFields.MESSAGE_NAME: "processesMeshtasticLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.sensor_nodes[int(sensor_node_id)].listener.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def processesMeshtasticReturnLT(component: object, sensor_node_id: str, processes: str):
    """
    Forwards processesMeshtasticReturnLT message to the Dashboard.
    """
    # Forward to Dashboard
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "processes": processes,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "processesMeshtasticReturnLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def ifconfigMeshtasticLT(component: object, sensor_node_id: str):
    """
    Signals sensor node to retrieve the ifconfig output on the computer.
    """
    # Send Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.SOURCE: component.identifierLT,
        fissure.comms.MessageFields.DESTINATION: sensor_node_id,                
        fissure.comms.MessageFields.MESSAGE_NAME: "ifconfigMeshtasticLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.sensor_nodes[int(sensor_node_id)].listener.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def ifconfigMeshtasticReturnLT(component: object, sensor_node_id: str, ifconfig: str):
    """
    Forwards ifconfigMeshtasticReturnLT message to the Dashboard.
    """
    # Forward to Dashboard
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "ifconfig": ifconfig,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "ifconfigMeshtasticReturnLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def iwconfigMeshtasticLT(component: object, sensor_node_id: str):
    """
    Signals sensor node to retrieve the iwconfig output on the computer.
    """
    # Send Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.SOURCE: component.identifierLT,
        fissure.comms.MessageFields.DESTINATION: sensor_node_id,                
        fissure.comms.MessageFields.MESSAGE_NAME: "iwconfigMeshtasticLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.sensor_nodes[int(sensor_node_id)].listener.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def iwconfigMeshtasticReturnLT(component: object, sensor_node_id: str, iwconfig: str):
    """
    Forwards iwconfigMeshtasticReturnLT message to the Dashboard.
    """
    # Forward to Dashboard
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "iwconfig": iwconfig,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "iwconfigMeshtasticReturnLT",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg) 