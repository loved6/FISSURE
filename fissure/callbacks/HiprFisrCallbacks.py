# from comms.FissureZMQNode import *
# from fissure.comms.constants import *
# from fissure_libutils import *
import pytak
from typing import List
import xml.etree.ElementTree as ET

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
import tempfile
import zipfile
import importlib
import traceback
import zmq
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

DELAY_SHORT = 0.25  # seconds


##########################################################################
########################### For HIPRFISR #################################
##########################################################################

async def retrieveDatabaseCache(component: object, refresh_frontend_widgets=False):
    """
    Retrieves a copy of important database tables needed for operating the Dashboard.
    """
    # Retrieve the Database Cache
    database_return = fissure.utils.library.cacheTableData(source="Dashboard")

    # Return to the Dashboard (or other component?)
    PARAMETERS = {
        "database_return": database_return, 
        "refresh_frontend_widgets": refresh_frontend_widgets
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "retrieveDatabaseCacheReturn",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def addToLibrary(
    component: object,
    protocol_name="",
    packet_name="",
    packet_data="",
    soi_data="",
    modulation_type="",
    demodulation_fg_data="",
    attack="",
    dissector="",
):
    """
    Adds new data to the library.
    """
    # Maintain a Connection to the Database
    conn = fissure.utils.library.openDatabaseConnection()

    try:  
        # Check Protocol
        protocol_exists = False
        all_protocols = fissure.utils.library.getProtocolNamesDirect(conn)
        for protocol in all_protocols:
            # Existing Protocol
            if protocol == protocol_name:
                protocol_exists = True

                # Add New Modulation Type
                if len(modulation_type) > 0:
                    get_modulations = fissure.utils.library.getModulationTypesDirect(conn, protocol)
                    if modulation_type not in get_modulations:
                        fissure.utils.library.addModulationType(conn, protocol, modulation_type)

                # Add New Packet Type
                if len(packet_data) > 0:
                    all_new_fields = {}
                    for n in range(0, len(packet_data)):
                        field_name = packet_data[n][0]
                        field_length = packet_data[n][1]
                        field_default = packet_data[n][2]
                        field_order = n + 1
                        is_crc = packet_data[n][3]
                        crc_range = packet_data[n][4]

                        field_to_add = fissure.utils.library.newField(
                            field_name, field_default, field_length, field_order, is_crc, crc_range
                        )
                        all_new_fields.update(field_to_add)

                    sort_order = len(fissure.utils.library.getPacketTypesDirect(conn, protocol)) + 1  # Makes the packet appear on the bottom of any list
                    fissure.utils.library.addPacketType(conn, protocol, packet_name, {"Port": None, "Filename": None}, all_new_fields, sort_order)

                # Add Dissector
                if len(dissector) > 0:
                    fissure.utils.library.addDissector(conn, protocol, packet_name, dissector[0], dissector[1])

                # Add SOI Data
                if len(soi_data) > 0:
                    fissure.utils.library.addSOI(
                        conn, 
                        protocol, 
                        soi_data["soi_name"], 
                        soi_data["center_frequency"], 
                        soi_data["start_frequency"], 
                        soi_data["end_frequency"], 
                        soi_data["bandwidth"], 
                        soi_data["continuous"], 
                        soi_data["modulation"],
                        soi_data["notes"]
                    )

                # Add Demodulation Flow Graph
                if len(demodulation_fg_data) > 0:
                    if (
                        (len(demodulation_fg_data["modulation_type"]) > 0)
                        and (len(demodulation_fg_data["hardware"]) > 0)
                        and (len(demodulation_fg_data["filename"]) > 0)
                        and (len(demodulation_fg_data["output_type"]) > 0)
                    ):
                        fissure.utils.library.addDemodulationFlowGraph(
                            conn,
                            protocol_name,
                            demodulation_fg_data["modulation_type"],
                            demodulation_fg_data["hardware"],
                            demodulation_fg_data["filename"],
                            demodulation_fg_data["output_type"],
                            fissure.utils.get_library_version()
                        )

                # Add Attack
                if len(attack) > 0:
                    fissure.utils.library.addAttack(
                        conn, 
                        protocol_name, 
                        attack["attack_name"], 
                        attack["modulation_type"],
                        attack["hardware"],
                        attack["attack_type"],
                        attack["filename"],
                        attack["category_name"],
                        fissure.utils.get_library_version()
                    )

        # New Protocol
        if not protocol_exists:
            print("Protocol not found")
            # make_new_protocol = fissure.utils.library.newProtocol(protocolname=protocol_name)
            fissure.utils.library.addProtocol(conn, protocol_name, None, None)

        # Send Message to PD to Update Library
        await retrieveDatabaseCachePD(component)
        
        # Send Message to Dashboard to Update Library
        await retrieveDatabaseCache(component, True)

    except:
        component.logger.error("Failure adding data to the FISSURE library.")

    finally:
        conn.close()


async def removeFromLibrary(
    component: object,
    table_name = "",
    row_id = "",
    delete_files = False
):
    """
    Removes a row from the library/database table.
    """
    # Maintain a Connection to the Database
    conn = fissure.utils.library.openDatabaseConnection()

    try:
        # Remove the Row/Files
        if (len(table_name) > 0) and (len(row_id) > 0):
            fissure.utils.library.removeFromTable(conn, table_name, row_id, delete_files, component.os_info)

        # Send Message to PD to Update Library
        await retrieveDatabaseCachePD(component)

        # Send Message to Dashboard to Update Library
        await retrieveDatabaseCache(component, True)
 
    except:
        component.logger.error("Failure removing data from the FISSURE library.")

    finally:
        conn.close()


async def shutdown(component: object, identifiers: List[str]):
    """
    Process `shutdown` commands

    :param identifier: Identifier of the fissure component to shutdown
    :type identifier: str
    """
    component.logger.info(f"received shutdown command for [{', '.join(identifiers)}]")
    for identifier in identifiers:
        if identifier == component.identifier:
            # component.shutdown = True  # before or after?

            # forward 'Shutdown' command to PD and TSI)
            msg = {
                fissure.comms.MessageFields.IDENTIFIER: component.identifier,
                fissure.comms.MessageFields.MESSAGE_NAME: "shutdown",
                fissure.comms.MessageFields.PARAMETERS: {
                    fissure.comms.Parameters.IDENTIFIERS: [fissure.comms.Identifiers.PD, fissure.comms.Identifiers.TSI]
                },
            }
            await component.backend_router.send_msg(
                fissure.comms.MessageTypes.COMMANDS, msg, target_ids=[component.pd_id, component.tsi_id]
            )

            # pd_running = True
            # tsi_running = True
            # while pd_running or tsi_running:
            #     msg = await component.backend_router.recv_msg()

            #     if msg is not None:
            #         msg_type = msg.get(fissure.comms.MessageFields.TYPE)
            #         msg_name = msg.get(fissure.comms.MessageFields.MESSAGE_NAME)
            #         sender = msg.get(fissure.comms.MessageFields.IDENTIFIER)
            #         if msg_type == fissure.comms.MessageTypes.STATUS and msg_name == "Shutting Down":
            #             if sender == fissure.comms.Identifiers.PD:
            #                 pd_running = False
            #             if sender == fissure.comms.Identifiers.TSI:
            #                 tsi_running = False
            component.shutdown = True
        else:
            # forward 'Shutdown' command to specified fissure component(s)
            pass


async def disconnect(component: object):
    ack = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "Disconnect OK",
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.STATUS, ack)
    component.logger.debug("Dashboard Disconnecting")
    component.dashboard_connected = False
    component.session_active = False
    component.heartbeats.update({fissure.comms.Identifiers.DASHBOARD: None})
    component.connect_loop = True


async def clearWidebandList(component: object):
    """Clears the Wideband List"""
    component.logger.debug("Executing Callback: Clear Wideband List")
    component.wideband_list = []


async def enableDisableListener(component: object, listener_type="", listener_name="", parameters={}):
    """
    Creates a listener if it does not exist and then toggles its enable/disable status.
    """
    loop = asyncio.get_running_loop()

    if listener_type == "Meshtastic":
        ListenerClass = MeshtasticListener
    elif listener_type == "Filesystem":
        ListenerClass = FilesystemListener
    elif listener_type == "ZMQ SUB":
        ListenerClass = ZMQSubscriberListener
    elif listener_type == "Website Poller":
        ListenerClass = WebsitePollerListener
    elif listener_type == "Serial Port":
        ListenerClass = SerialPortListener
    elif listener_type == "TCP/UDP":
        ListenerClass = TCPUDPListener
    elif listener_type == "MQTT":
        ListenerClass = MQTTListener        
    else:
        component.logger.error(f"Unknown listener type '{listener_type}'")
        return

    if listener_name not in component.alert_listeners:
        # Create a new Listener if it does not exist
        listener = ListenerClass(
            component, 
            listener_name, 
            parameters, 
            loop, 
            alert_callback=alertReturn
        )
        component.alert_listeners[listener_name] = listener
        component.logger.info(f"Created new {listener_type} Listener: {listener_name}")
    else:
        listener = component.alert_listeners[listener_name]

    # Toggle enable/disable status
    if listener.is_active():
        listener.disable()
        component.logger.info(f"Listener '{listener_name}' disabled.")
        status = "Disabled"
    else:
        listener.enable()
        component.logger.info(f"Listener '{listener_name}' enabled.")
        status = "Enabled"

    # Send Status to Dashboard
    PARAMETERS = {
        "listener_name": listener_name,
        "status": status,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "enableDisableListenerReturn",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def deleteListener(component: object, listener_name=""):
    """
    Deletes an existing listener.
    """
    # Delete the Listener
    if listener_name in component.alert_listeners:
        # Stop and delete the listener
        listener = component.alert_listeners[listener_name]
        listener.disable()
        del component.alert_listeners[listener_name]
        component.logger.info(f"Listener '{listener_name}' deleted and stopped.")
    else:
        component.logger.error(f"No listener found with name '{listener_name}' to delete.")

    # Update Dashboard
    PARAMETERS = {"listener_name": listener_name}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "deleteListenerReturn",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def pingIP(component: object, sensor_node_id: str):
    """
    Pings the sensor node IP from the HIPRFISR and returns the results to the Dashboard.
    """
    # Obtain IP address from sensor node ID
    for addr in component.sensor_nodes[int(sensor_node_id)].listener.connections:  # Will there ever be multiple connections?
        ip = addr.address

    try:
        proc = await asyncio.create_subprocess_shell(
            f"ping -c 1 -W 2 {ip}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()
        result = stdout.decode().strip() or stderr.decode().strip()
    except Exception as e:
        result = f"Error running ping: {e}"

    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "ping": result,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "pingIP_Return",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


##########################################################################
###################### To Multiple Components ############################
##########################################################################

async def updateFISSURE_Configuration(component: object, settings_dict={}):
    """Reload fissure_config.yaml after changes."""
    # Load settings from Fissure Config YAML
    component.settings = settings_dict #fissure.utils.get_fissure_config()  # Stick with Dashboard computer and someday look into storing on HIPRFISR computer.

    # Update TSI/PD/Other Components
    PARAMETERS = {"settings_dict": settings_dict}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "updateFISSURE_Configuration",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.backend_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS, msg, target_ids=[component.pd_id, component.tsi_id]
    )


async def updateLoggingLevels(component: object, new_console_level="", new_file_level=""):
    """
    Update the logging levels on the HIPRFISR and forward to all components.
    """
    # Update New Levels for the HIPRFISR
    await component.updateLoggingLevels(new_console_level, new_file_level)

##########################################################################
##################### From Multiple Components ##########################
##########################################################################



##########################################################################
############################## To PD ####################################
##########################################################################


async def startPD(component: object, sensor_node_id=0):
    """Sends a message to PD and sensor node to start processing on any incoming bits."""
    # Forward Message to PD
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "startPD",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.backend_router.send_msg(fissure.comms.MessageTypes.COMMANDS, msg, target_ids=[component.pd_id])

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def stopPD(component: object, sensor_node_id=0):
    """
    Signals to PD and sensor node to stop protocol discovery.
    """
    # Forward Message to PD
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "stopPD",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.backend_router.send_msg(fissure.comms.MessageTypes.COMMANDS, msg, target_ids=[component.pd_id])

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def pdBitsReturn(component: object, bits_message=""):
    """
    Forwards bits captured at the sensor node to the protocol discovery circular buffer.
    """
    # Forward Message to PD
    PARAMETERS = {"bits_message": bits_message}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "pdBitsReturn",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.backend_router.send_msg(fissure.comms.MessageTypes.COMMANDS, msg, target_ids=[component.pd_id])


async def searchLibraryForFlowGraphs(
    component: object, soi_data=[], hardware=""
):  # Future: keep this to wherever the database will be: hiprfisr?
    """
    Queries protocol discovery to look in its version of the library to recommend flow graphs for the SOI.
    """
    # Forward Message to PD
    PARAMETERS = {"soi_data": soi_data, "hardware": hardware}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "searchLibraryForFlowGraphs",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.backend_router.send_msg(fissure.comms.MessageTypes.COMMANDS, msg, target_ids=[component.pd_id])


async def findPreambles(component: object, window_min=0, window_max=0, ranking=0, std_deviations=0):
    """Sends message to PD to search the buffer for preambles."""
    # Send Message to PD
    PARAMETERS = {
        "window_min": window_min,
        "window_max": window_max,
        "ranking": ranking,
        "std_deviations": std_deviations,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "findPreambles",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.backend_router.send_msg(fissure.comms.MessageTypes.COMMANDS, msg, target_ids=[component.pd_id])


async def searchLibrary(component: object, soi_data="", field_data=""):
    """
    Sends message to PD to search library.yaml from SOI data and field values.
    """
    # Send Message to PD
    PARAMETERS = {"soi_data": soi_data, "field_data": field_data}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "searchLibrary",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.backend_router.send_msg(fissure.comms.MessageTypes.COMMANDS, msg, target_ids=[component.pd_id])


async def sliceByPreamble(component: object, preamble="", first_n=0, estimated_length=0):
    """Sends message to PD to slice the data by a single preamble."""
    # Send Message to PD
    PARAMETERS = {"preamble": preamble, "first_n": first_n, "estimated_length": estimated_length}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "sliceByPreamble",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.backend_router.send_msg(fissure.comms.MessageTypes.COMMANDS, msg, target_ids=[component.pd_id])


async def setBufferSize(component: object, min_buffer_size=0, max_buffer_size=0):
    """
    Sends message to PD with the new sizes for the protocol discovery buffer.
    """
    # Send Message to PD
    PARAMETERS = {"min_buffer_size": min_buffer_size, "max_buffer_size": max_buffer_size}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "setBufferSize",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.backend_router.send_msg(fissure.comms.MessageTypes.COMMANDS, msg, target_ids=[component.pd_id])


async def clearPD_Buffer(component: object):
    """
    Sends a message to Protocol Discovery to clear its buffer.
    """
    # Send Message to PD
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "clearPD_Buffer",
    }
    await component.backend_router.send_msg(fissure.comms.MessageTypes.COMMANDS, msg, target_ids=[component.pd_id])


async def findEntropy(component: object, message_length=0, preamble=""):
    """
    Sends a message to Protocol Discovery to find the entropy for the bit positions of fixed-length messages.
    """
    # Send Message to PD
    PARAMETERS = {"message_length": message_length, "preamble": preamble}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "findEntropy",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.backend_router.send_msg(fissure.comms.MessageTypes.COMMANDS, msg, target_ids=[component.pd_id])


async def findEntropyReturn(component: object, ents=[]):
    """
    Forwards the findEntropy results to the Dashboard.
    """
    # Send Message to PD
    PARAMETERS = {"ents": ents}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "findEntropyReturn",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def addPubSocket(component: object, ip_address="", port=0):
    """
    Signals to Protocol Discovery to add an additional ZMQ PUB for reading bits.
    """
    # Send Message to PD
    PARAMETERS = {"ip_address": ip_address, "port": port}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "addPubSocket",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.backend_router.send_msg(fissure.comms.MessageTypes.COMMANDS, msg, target_ids=[component.pd_id])


async def removePubSocket(component: object, address=""):
    """
    Signals to Protocol Discovery to remove a ZMQ PUB.
    """
    # Send Message to PD
    PARAMETERS = {"address": address}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "removePubSocket",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.backend_router.send_msg(fissure.comms.MessageTypes.COMMANDS, msg, target_ids=[component.pd_id])


##########################################################################
################################ From PD ################################
##########################################################################

async def retrieveDatabaseCachePD(component: object):
    """
    Retrieves a copy of important database tables needed for operating Protocol Discovery.
    """
    # Retrieve the Database Cache
    database_return = fissure.utils.library.cacheTableData(source="Protocol Discovery")

    # Return to the Dashboard (or other component?)
    PARAMETERS = {
        "database_return": database_return, 
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "retrieveDatabaseCacheReturnPD",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.backend_router.send_msg(fissure.comms.MessageTypes.COMMANDS, msg, target_ids=[component.pd_id])


async def findPreamblesReturn(component: object, slice_medians, candidate_preambles, min_std_dev_max_length_preambles):
    """
    Sends potential preambles found in the circular buffer to the Dashboard.
    """
    PARAMETERS = {
        "slice_medians": slice_medians,
        "candidate_preambles": candidate_preambles,
        "min_std_dev_max_length_preambles": min_std_dev_max_length_preambles,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "findPreamblesReturn",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def searchLibraryReturn(component: object, message=[]):
    """
    Forwards the search results to the Dashboard.
    """
    # Send Message to PD
    PARAMETERS = {"message": message}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "searchLibraryReturn",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def demodFG_LibrarySearchReturn(component: object, flow_graphs=[]):
    """."""
    # Forward Message to Dashboard
    PARAMETERS = {"flow_graphs": flow_graphs}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "demodFG_LibrarySearchReturn",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def bufferSizeReturn(component: object, buffer_size=0):
    """
    Forwards the size of the PD circular buffer to the Dashboard.
    """
    # Forward Message to Dashboard
    PARAMETERS = {"buffer_size": buffer_size}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "bufferSizeReturn",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def sliceByPreambleReturn(component: object, packet_lengths=[], packet_dict={}):
    """
    Forwards the slice results to the Dashboard.
    """
    # Forward Message to Dashboard
    PARAMETERS = {"packet_lengths": packet_lengths, "packet_dict": packet_dict}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "sliceByPreambleReturn",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def foundPreambles(component: object, parameters={}):
    """."""
    # Forward Message to Dashboard
    PARAMETERS = {"parameters": parameters}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "foundPreambles",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def foundPreamblesInLibrary(component: object, parameters={}):
    """."""
    # Forward Message to Dashboard
    PARAMETERS = {"parameters": parameters}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "foundPreamblesInLibrary",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


# ############################ To TSI ####################################


async def addBlacklist(component: object, start_frequency=0, end_frequency=0):
    """
    Forwards Add Blacklist message to TSI.
    """
    # Send Message to TSI
    PARAMETERS = {"start_frequency": start_frequency, "end_frequency": end_frequency}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "addBlacklist",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.backend_router.send_msg(fissure.comms.MessageTypes.COMMANDS, msg, target_ids=[component.tsi_id])


async def removeBlacklist(component: object, start_frequency=0, end_frequency=0):
    """
    Forwards Remove Blacklist message to TSI.
    """
    # Send Message to TSI
    PARAMETERS = {"start_frequency": start_frequency, "end_frequency": end_frequency}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "removeBlacklist",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.backend_router.send_msg(fissure.comms.MessageTypes.COMMANDS, msg, target_ids=[component.tsi_id])


async def startTSI_FE(component: object, common_parameter_names=[], common_parameter_values=[]):
    """
    Signals to TSI to start TSI feature extractor.
    """
    # Forward Message to TSI
    PARAMETERS = {"common_parameter_names": common_parameter_names, "common_parameter_values": common_parameter_values}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "startTSI_FE",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.backend_router.send_msg(fissure.comms.MessageTypes.COMMANDS, msg, target_ids=[component.tsi_id])


async def stopTSI_FE(component: object):
    """
    Signals to TSI to stop TSI feature extractor.
    """
    # Forward Message to TSI
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "stopTSI_FE",
    }
    await component.backend_router.send_msg(fissure.comms.MessageTypes.COMMANDS, msg, target_ids=[component.tsi_id])


async def startTSI_Conditioner(
    component: object,
    sensor_node_id=0,
    common_parameter_names=[],
    common_parameter_values=[],
    method_parameter_names=[],
    method_parameter_values=[],
    method_filepath=""
):
    """
    Signals to TSI to start TSI Conditioner.
    """
    # Forward Message to TSI
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "common_parameter_names": common_parameter_names,
        "common_parameter_values": common_parameter_values,
        "method_parameter_names": method_parameter_names,
        "method_parameter_values": method_parameter_values,
        "method_filepath": method_filepath
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "startTSI_Conditioner",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.backend_router.send_msg(fissure.comms.MessageTypes.COMMANDS, msg, target_ids=[component.tsi_id])


async def stopTSI_Conditioner(component, sensor_node_id=0):
    """
    Signals to TSI to stop TSI conditioner.
    """
    # Forward Message to TSI
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "stopTSI_Conditioner",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.backend_router.send_msg(fissure.comms.MessageTypes.COMMANDS, msg, target_ids=[component.tsi_id])


# ############################# From TSI #################################


async def conditionerProgressBarReturn(component: object, progress=0, file_index=0):
    """."""
    # Forward Message to Dashboard
    PARAMETERS = {"progress": progress, "file_index": file_index}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "conditionerProgressBarReturn",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def tsiConditionerFinished(component: object, table_strings=[]):
    """."""
    # Forward Message to Dashboard
    PARAMETERS = {"table_strings": table_strings}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "tsiConditionerFinished",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def feProgressBarReturn(component: object, progress=0, file_index=0):
    """."""
    # Forward Message to Dashboard
    PARAMETERS = {"progress": progress, "file_index": file_index}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "feProgressBarReturn",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def tsiFE_Finished(component: object, table_strings=[]):
    """."""
    # Forward Message to Dashboard
    PARAMETERS = {"table_strings": table_strings}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "tsiFE_Finished",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


##########################################################################
############################ To Sensor Node ##############################
##########################################################################


async def scanHardware(component: object, tab_index=0, hardware_list=[]):
    """
    Sends a message to a sensor node to scan for hardware information.
    """
    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(tab_index)
    if identity is None:
        return

    # Build message
    PARAMETERS = {"hardware_list": hardware_list}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "scanHardware",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def probeHardware(component: object, tab_index, table_row_text):
    """
    Sends a message to a sensor node to probe select hardware.
    """
    # Forward the Message
    PARAMETERS = {"tab_index": tab_index, "table_row_text": table_row_text}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "probeHardware",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(tab_index)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def guessHardware(component: object, tab_index=0, table_row=0, table_row_text=[], guess_index=0):
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
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "guessHardware",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(tab_index)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def transferSensorNodeFile(
    component: object, sensor_node_id=0, local_file="", remote_folder="", refresh_file_list=False
):
    """
    Loads a local file and transfers the data to a remote sensor node.
    """
    # Construct Filepath
    remote_filepath = remote_folder + "/" + local_file.split("/")[-1]

    # Load File
    if os.path.isfile(local_file):
        # Read the File
        try:
            with open(local_file, "rb") as f:
                get_data = f.read()
            get_data = binascii.hexlify(get_data)
            get_data = get_data.decode("utf-8").upper()
        except:
            component.logger.error("Error reading file")
            return
    else:
        component.logger.error("Invalid local filepath")
        return

    # Send Message
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "local_file_data": get_data,
        "remote_filepath": remote_filepath,
        "refresh_file_list": refresh_file_list,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "transferSensorNodeFile",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def deleteArchiveReplayFiles(component: object, sensor_node_id=0):
    """
    Deletes all the files in the Archive_Replay folder on the sensor node ahead of file transfer for replay.
    """
    # Send Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "deleteArchiveReplayFiles",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def refreshSensorNodeFiles(component: object, sensor_node_id=0, sensor_node_folder=""):
    """
    Signals to sensor node to return file details for a specified folder.
    """
    # Send Message
    PARAMETERS = {"sensor_node_id": sensor_node_id, "sensor_node_folder": sensor_node_folder}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "refreshSensorNodeFiles",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def deleteSensorNodeFile(component: object, sensor_node_id=0, sensor_node_file=""):
    """
    Signals to sensor node to delete a file or folder for a specified file path.
    """
    # Send Message
    PARAMETERS = {"sensor_node_id": sensor_node_id, "sensor_node_file": sensor_node_file}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "deleteSensorNodeFile",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def downloadSensorNodeFile(component: object, sensor_node_id=0, sensor_node_file="", download_folder=""):
    """
    Signals to sensor node to transfer a copy of a file or folder for saving it to a specified file path.
    """
    # Send Message
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "sensor_node_file": sensor_node_file,
        "download_folder": download_folder,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "downloadSensorNodeFile",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def overwriteDefaultAutorunPlaylist(component: object, sensor_node_id=0, playlist_dict={}):
    """Signals to sensor node to overwrite the default autorun playlist."""
    # Send Message
    PARAMETERS = {"sensor_node_id": sensor_node_id, "playlist_dict": playlist_dict}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "overwriteDefaultAutorunPlaylist",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def autorunPlaylistStart(component: object, sensor_node_id=0, playlist_dict={}, trigger_values=[]):
    """Signals to sensor node to start autorun playlist."""
    # Send Message
    PARAMETERS = {"sensor_node_id": sensor_node_id, "playlist_dict": playlist_dict, "trigger_values": trigger_values}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "autorunPlaylistStart",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def autorunPlaylistExecute(component: object, sensor_node_id=0, playlist_filename=""):
    """Signals to sensor node to start autorun playlist already located on the sensor node."""
    # Send Message
    PARAMETERS = {"sensor_node_id": sensor_node_id, "playlist_filename": playlist_filename}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "autorunPlaylistExecute",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def autorunPlaylistStop(component: object, sensor_node_id=0):
    """Signals to sensor node to stop autorun playlist."""
    # Send Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "autorunPlaylistStop",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def physicalFuzzingStart(
    component: object,
    sensor_node_id=0,
    fuzzing_variables=[],
    fuzzing_type="",
    fuzzing_min=0,
    fuzzing_max=0,
    fuzzing_update_period=0,
    fuzzing_seed_step=0,
):
    """Command for starting physical fuzzing on a running flow graph."""
    # Send Message to Sensor Node
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "fuzzing_variables": fuzzing_variables,
        "fuzzing_type": fuzzing_type,
        "fuzzing_min": fuzzing_min,
        "fuzzing_max": fuzzing_max,
        "fuzzing_update_period": fuzzing_update_period,
        "fuzzing_seed_step": fuzzing_seed_step,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "physicalFuzzingStart",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def physicalFuzzingStop(component: object, sensor_node_id=0):
    """Sends message to Sensor Node to stop the physical fuzzing thread being performed on a running flow graph."""
    # Send Message to sensor_node,PD
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "physicalFuzzingStop",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def multiStageAttackStart(
    component: object,
    sensor_node_id=0,
    filenames=[],
    variable_names=[],
    variable_values=[],
    durations=[],
    repeat=False,
    file_types=[],
    autorun_index=0,
    trigger_values=[]
):
    """
    Sends message to Sensor Node/PD to start multi-stage attack.
    """
    # Send Message to Sensor Node
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "filenames": filenames,
        "variable_names": variable_names,
        "variable_values": variable_values,
        "durations": durations,
        "repeat": repeat,
        "file_types": file_types,
        "autorun_index": autorun_index,
        "trigger_values": trigger_values
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "multiStageAttackStart",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def multiStageAttackStop(component: object, sensor_node_id=0, autorun_index=0):
    """
    Sends message to Sensor Node/PD to stop multi-stage attack.
    """
    # Send Message to Sensor Node
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        # "parameter": parameter,
        "autorun_index": autorun_index,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "multiStageAttackStop",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def archivePlaylistStart(
    component: object,
    sensor_node_id=0,
    flow_graph="",
    filenames=[],
    frequencies=[],
    sample_rates=[],
    formats=[],
    channels=[],
    gains=[],
    durations=[],
    repeat=False,
    ip_address="",
    serial="",
    trigger_values=[]
):
    """
    Sends message to Sensor Node to start the archive playlist.
    """
    # Send Message to Sensor Node
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "flow_graph": flow_graph,
        "filenames": filenames,
        "frequencies": frequencies,
        "sample_rates": sample_rates,
        "formats": formats,
        "channels": channels,
        "gains": gains,
        "durations": durations,
        "repeat": repeat,
        "ip_address": ip_address,
        "serial": serial,
        "trigger_values": trigger_values
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "archivePlaylistStart",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def archivePlaylistStop(component: object, sensor_node_id=0):
    """
    Sends message to Sensor Node to stop the archive playlist.
    """
    # Send Message to Sensor Node
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "archivePlaylistStop",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def attackFlowGraphStop(component: object, sensor_node_id=0, parameter="", autorun_index=0):
    """
    Sends message to Sensor Node to stop a running attack flow graph.
    """
    # Send Message to Sensor Node
    PARAMETERS = {"sensor_node_id": sensor_node_id, "parameter": parameter, "autorun_index": autorun_index}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "attackFlowGraphStop",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def attackFlowGraphStart(
    component: object,
    sensor_node_id=0,
    flow_graph_filepath="",
    variable_names=[],
    variable_values=[],
    file_type="",
    run_with_sudo=False,
    autorun_index=0,
    trigger_values=[]
):
    """Command for loading an attack."""
    # Send Message to Sensor Node
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "flow_graph_filepath": flow_graph_filepath,
        "variable_names": variable_names,
        "variable_values": variable_values,
        "file_type": file_type,
        "run_with_sudo": run_with_sudo,
        "autorun_index": autorun_index,
        "trigger_values": trigger_values
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "attackFlowGraphStart",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def iqFlowGraphStart(
    component: object, sensor_node_id=0, flow_graph_filepath="", variable_names=[], variable_values=[], file_type=""
):
    """
    Command for loading an IQ flow graph.
    """
    # Send Message to Sensor Node
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "flow_graph_filepath": flow_graph_filepath,
        "variable_names": variable_names,
        "variable_values": variable_values,
        "file_type": file_type,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "iqFlowGraphStart",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def iqFlowGraphStop(component: object, sensor_node_id=0, parameter=""):
    """
    Sends message to Sensor Node to stop a running attack flow graph.
    """
    # Send Message to Sensor Node,PD
    # PARAMETERS = {"sensor_node_id": sensor_node_id, "parameter": parameter}
    PARAMETERS = {"parameter": parameter}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "iqFlowGraphStop",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def inspectionFlowGraphStart(
    component: object, sensor_node_id=0, flow_graph_filepath="", variable_names=[], variable_values=[], file_type=""
):
    """
    Command for starting an inspection flow graph.
    """
    # Send Message to Sensor Node
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "flow_graph_filepath": flow_graph_filepath,
        "variable_names": variable_names,
        "variable_values": variable_values,
        "file_type": file_type,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "inspectionFlowGraphStart",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def inspectionFlowGraphStop(component: object, sensor_node_id=0, parameter=""):
    """
    Command for stopping an inspection flow graph.
    """
    # Send Message to Sensor Node,PD
    PARAMETERS = {"parameter": parameter}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "inspectionFlowGraphStop",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def snifferFlowGraphStart(
    component: object, sensor_node_id=0, flow_graph_filepath="", variable_names=[], variable_values=[]
):
    """
    Starts a sniffer flow graph.
    """
    # Send Message to Sensor Node
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "flow_graph_filepath": flow_graph_filepath,
        "variable_names": variable_names,
        "variable_values": variable_values,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "snifferFlowGraphStart",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def snifferFlowGraphStop(component: object, sensor_node_id=0, parameter=""):
    """
    Stops a sniffer flow graph
    """
    # Send Message to Sensor Node,PD
    PARAMETERS = {"sensor_node_id": sensor_node_id, "parameter": parameter}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "snifferFlowGraphStop",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def startScapy(component: object, sensor_node_id=0, interface="", interval=0, loop=False, operating_system=""):
    """
    Signals to Sensor Node to start Scapy.
    """
    # Send Message
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "interface": interface,
        "interval": interval,
        "loop": loop,
        "operating_system": operating_system,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "startScapy",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def stopScapy(component: object, sensor_node_id=0):
    """Signals to Sensor Node to stop Scapy."""
    # Send Message
    # PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "stopScapy",  # ,
        # fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )    


async def setVariable(component: object, sensor_node_id=0, flow_graph="", variable="", value=""):
    """
    Sends a message to Sensor Node to change the variable of the running flow graph.
    """
    # Send Message to Sensor Node
    PARAMETERS = {"sensor_node_id": sensor_node_id, "flow_graph": flow_graph, "variable": variable, "value": value}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "setVariable",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def protocolDiscoveryFG_Start(
    component: object, sensor_node_id=0, flow_graph_filepath="", variable_names=[], variable_values=[]
):
    """
    Sends message to Sensor Node to run a flow graph.
    """
    # Send Message to Sensor Node
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "flow_graph_filepath": flow_graph_filepath,
        "variable_names": variable_names,
        "variable_values": variable_values,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "protocolDiscoveryFG_Start",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )    


async def protocolDiscoveryFG_Stop(component: object, sensor_node_id=0):
    """
    Sends message to Sensor Node to stop a running flow graph.
    """
    # Send Message to Sensor Node
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "protocolDiscoveryFG_Stop",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def updateConfiguration(
    component: object, sensor_node_id=0, start_frequency=0, end_frequency=0, step_size=0, dwell_time=0, detector_port=0
):
    """Forwards the Update Configuration message to TSI."""
    # Forward Message to Sensor Node
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "start_frequency": start_frequency,
        "end_frequency": end_frequency,
        "step_size": step_size,
        "dwell_time": dwell_time,
        "detector_port": detector_port,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "updateConfiguration",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    # component.tsi_hiprfisr_server.sendmsg(
    #     'Commands',
    #     Identifier='HIPRFISR',
    #     MessageName='Update Configuration',
    #     Parameters=[start_frequency, end_frequency, step_size, dwell_time]
    # )
    # component.backend_router.send_msg(
    #     fissure.comms.MessageTypes.COMMANDS,
    #     target_ids=[component.tsi_id],
    #     msg
    # )  # Future?

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )    


async def startTSI_Detector(component: object, sensor_node_id=0, detector="", variable_names=[], variable_values=[], detector_port=0):
    """
    Signals to sensor node to start TSI detector.
    """
    # Forward Message to Sensor Node
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "detector": detector,
        "variable_names": variable_names,
        "variable_values": variable_values,
        "detector_port": detector_port,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "startTSI_Detector",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    # component.tsi_hiprfisr_server.sendmsg(
    #     'Commands',
    #     Identifier='HIPRFISR',MessageName='Start TSI Detector', Parameters=[detector,variable_names,variable_values]
    # )
    # component.backend_router.send_msg(
    #     fissure.comms.MessageTypes.COMMANDS, target_ids=[component.tsi_id], msg
    # )  # Future?

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def stopTSI_Detector(component: object, sensor_node_id=0):
    """
    Signals to sensor node to stop TSI detector.
    """
    # Forward Message to Sensor Node
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "stopTSI_Detector",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    # component.tsi_hiprfisr_server.sendmsg('Commands', Identifier='HIPRFISR', MessageName='Stop TSI Detector')
    # component.backend_router.send_msg(
    #     fissure.comms.MessageTypes.COMMANDS, msg, target_ids=[component.tsi_id]
    # )  # Future?

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )    


async def terminateSensorNode(component: object, sensor_node_id):
    """
    Stops sensor_node.py for local operations.
    """
    sensor_node_id = int(sensor_node_id)

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return

    # Send to Sensor Node
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "terminateSensorNode",
    }
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def nodeRefreshIP(component: object, dashboard_node_index):
    """
    """
    # For testing:
    # component.nodes = {
    #     "UUID-123": {
    #         "uuid": "UUID-123",
    #         "ip": "192.168.1.50",
    #         "settings": {"nickname": "Node A"},
    #         "network_type": "IP",
    #         "last_seen": time.time(),
    #         "connected": True
    #     },
    #     "UUID-456": {
    #         "uuid": "UUID-456",
    #         "ip": "10.0.0.15",
    #         "settings": {"nickname": "Node B"},
    #         "network_type": "Meshtastic",
    #         "last_seen": time.time() - 12.3,
    #         "connected": False
    #     }
    # }

    # -----------------------------------------------------
    # Notify dashboard
    # -----------------------------------------------------
    PARAMETERS = {
        "dashboard_node_index": dashboard_node_index,
        "nodes": component.nodes
    }

    msg_to_dashboard = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "nodeRefreshIP_Return",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    if component.dashboard_connected:
        await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg_to_dashboard)


async def nodeSelectIP(component: object, dashboard_node_index, node_uuid):
    """
    """
    # Lookup identity for this UUID
    node_entry = component.nodes.get(node_uuid)
    if not node_entry:
        component.logger.error(f"[HIPRFISR] No such node_uuid {node_uuid}")
        return

    identity = node_entry["identity"]

    # Send Message to Node
    PARAMETERS = {
        "dashboard_node_index": dashboard_node_index,
        "node_uuid": node_uuid
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "nodeSelectIP",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.sensor_node_router.send_msg(fissure.comms.MessageTypes.COMMANDS, msg, target_ids=[identity])


async def nodeReconnectIP(component: object, dashboard_node_index):
    """
    """
    # Resolve UUID + identity
    node_uuid, identity = component.resolve_sensor_node_identity(dashboard_node_index)
    if not node_uuid:
        return  # Helper already logged the problem

    # Send Message to Node
    PARAMETERS = {
        "dashboard_node_index": dashboard_node_index,
        "node_uuid": node_uuid
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "nodeSelectIP",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.sensor_node_router.send_msg(fissure.comms.MessageTypes.COMMANDS, msg, target_ids=[identity])


async def disconnectFromSensorNode(component, dashboard_index=0, delete_node=False):
    """
    Dashboard callback: request disconnection of a Sensor Node.
    """
    dashboard_index = int(dashboard_index)

    # 1) Resolve UUID + identity
    uuid, identity = component.resolve_sensor_node_identity(dashboard_index)
    if not uuid:
        return  # Helper already logged the problem

    # 2) Mark node as disconnected internally
    node = component.nodes.get(uuid)
    if node:
        node["connected"] = False

    # 3) Notify Dashboard immediately
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "componentDisconnected",
        fissure.comms.MessageFields.PARAMETERS: {"component_name": dashboard_index},
    }
    if component.dashboard_connected:
        await component.dashboard_socket.send_msg(
            fissure.comms.MessageTypes.COMMANDS,
            msg
        )

    # 4) Notify Sensor Node (if still reachable)
    try:
        msg2 = {
            fissure.comms.MessageFields.IDENTIFIER: component.identifier,
            fissure.comms.MessageFields.MESSAGE_NAME: "hiprfisrDisconnecting",
            fissure.comms.MessageFields.PARAMETERS: {},
        }

        await component.sensor_node_router.send_msg(
            fissure.comms.MessageTypes.COMMANDS,
            msg2,
            target_ids=[identity]
        )
    except Exception:
        pass

    # 5) Delete node entirely from HIPRFISR (optional)
    if delete_node:

        # Stop Local Sensor Node Program
        if uuid == component.local_node_uuid:
            msg3 = {
                fissure.comms.MessageFields.IDENTIFIER: component.identifier,
                fissure.comms.MessageFields.MESSAGE_NAME: "terminateSensorNode",
                fissure.comms.MessageFields.PARAMETERS: {},
            }
            await component.sensor_node_router.send_msg(
                fissure.comms.MessageTypes.COMMANDS,
                msg3,
                target_ids=[identity]
            )

            # Remove UUID from HIPRFISR registry for local only
            try:
                del component.nodes[uuid]
                component.logger.info(f"[DELETE] Removed node entry for UUID: {uuid}")
            except KeyError:
                component.logger.warning(f"[DELETE] UUID {uuid} not found in component.nodes")

        # Clear heartbeat tracking
        hb = component.heartbeats.get(fissure.comms.Identifiers.SENSOR_NODE, {})

        if isinstance(hb, list):
            for i, entry in enumerate(hb):
                if isinstance(entry, dict):
                    # Heartbeat stored using identity, not UUID
                    if entry.get("uuid") == identity:
                        hb[i] = None
                        break

        elif isinstance(hb, dict):
            # If keyed by identity
            hb.pop(identity, None)
            # If keyed by UUID
            hb.pop(uuid, None)

        # Remove UUID from dashboard slot map
        component.dashboard_node_map[dashboard_index] = None

        # Shift remaining slots left (GUI ONLY)
        for i in range(dashboard_index, len(component.dashboard_node_map) - 1):
            component.dashboard_node_map[i] = component.dashboard_node_map[i + 1]

        component.dashboard_node_map[-1] = None

        # --------------------------------------
        # E) Cleanup complete
        # --------------------------------------
        component.logger.info(f"[DELETE] Node {uuid} fully removed from HIPRFISR")


async def disconnectFromMeshtastic(component: object, sensor_node_id=0):
    """
    Ends connections to local serial connection to Meshatastic.
    """
    sensor_node_id = int(sensor_node_id)

    # Ensure the sensor node exists
    if component.sensor_nodes[sensor_node_id] is None:
        component.logger.warning(f"Sensor node {sensor_node_id} is not connected.")
        return

    # Disconnect
    try:
        await component.sensor_nodes[sensor_node_id].listener.disconnect()
    except Exception as e:
        component.logger.error(f"Error disconnecting local serial connection for Sensor Node {sensor_node_id}: {e}")        

    # Set the node to None to fully remove it
    component.sensor_nodes[sensor_node_id] = None
    component.logger.info(f"Local serial connection for Sensor Node {sensor_node_id} successfully disconnected.")

    # Notify the Dashboard
    PARAMETERS = {"component_name": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "componentDisconnected",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def connectToSensorNodeMeshtastic(component: object, sensor_node_id, serial_port, serial_baud_rate):
    """
    Connects the HIPRFISR to a local serial connection for a device using Meshtastic.
    """
    sensor_node_id = int(sensor_node_id)
    context = component
    name=f"{fissure.comms.Identifiers.HIPRFISR}::sensor_node"

    # Initialize Meshtastic connection
    component.logger.info(f"Connecting to sensor node {sensor_node_id} via Meshtastic on {serial_port}...")

    try:
        if component.sensor_nodes[sensor_node_id] is None:
            await component.reset_sensor_node_listener(
                sensor_node_id, "Meshtastic", serial_port=serial_port, name=name, context=context
            )
        component.logger.info(f"Connected to local serial port for communicating with Sensor node {sensor_node_id} via Meshtastic.")

        # Send Connected Messages
        PARAMETERS = {"component_name": sensor_node_id}
        msg = {
            fissure.comms.MessageFields.IDENTIFIER: component.identifier,
            fissure.comms.MessageFields.MESSAGE_NAME: "componentConnectedSerial",
            fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
        }
        await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)
        
    except Exception as e:
        component.logger.error(f"Failed to connect to sensor node {sensor_node_id} via Meshtastic: {e}")


async def findGPS_Coordinates(component: object, tab_index=0, gps_source="", format=""):
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
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "findGPS_Coordinates",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(tab_index)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def gpsBeaconEnableDisableIP(component: object, sensor_node_id: str):
    """
    Enables/disables the GPS TAK beacon at the sensor node.
    """
    # Send Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "gpsBeaconEnableDisableIP",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def gpsBeaconRefreshIP(component: object, sensor_node_id: str):
    """
    Retrieves the GPS TAK beacon state from the sensor node.
    """
    # Send Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "gpsBeaconRefreshIP",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def rebootIP(component: object, sensor_node_id=0):
    """
    Forwards the message to reboot the sensor node computer.
    """
    # Send the Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "rebootIP",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def uptimeIP(component: object, sensor_node_id: str):
    """
    Forwards the message to retrieve the uptime of the sensor node computer.
    """
    # Send the Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "uptimeIP",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )    


async def memoryIP(component: object, sensor_node_id: str):
    """
    Forwards the message to retrieve the memory usage of the sensor node computer.
    """
    # Send the Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "memoryIP",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def diskIP(component: object, sensor_node_id: str):
    """
    Forwards the message to retrieve the disk usage of the sensor node computer.
    """
    # Send the Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "diskIP",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )

    
async def cpuIP(component: object, sensor_node_id: str):
    """
    Forwards the message to retrieve the CPU percentage of the sensor node computer.
    """
    # Send the Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "cpuIP",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )

    
async def processesIP(component: object, sensor_node_id: str):
    """
    Forwards the message to retrieve the processes on the sensor node computer.
    """
    # Send the Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "processesIP",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )    

    
async def ifconfigIP(component: object, sensor_node_id: str):
    """
    Forwards the message to retrieve the ifconfig output on the sensor node computer.
    """
    # Send the Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "ifconfigIP",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def iwconfigIP(component: object, sensor_node_id: str):
    """
    Forwards the message to retrieve the iwconfig output on the sensor node computer.
    """
    # Send the Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "iwconfigIP",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )    


##########################################################################
########################### From Sensor Node #############################
##########################################################################

async def refreshSensorNodeFilesResults(
    component: object, sensor_node_id=0, filepaths=[], file_sizes=[], file_types=[], modified_dates=[]
):
    """
    Forwards the refresh sensor node files results to the Dashboard.
    """
    # Send the Message
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "filepaths": filepaths,
        "file_sizes": file_sizes,
        "file_types": file_types,
        "modified_dates": modified_dates,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "refreshSensorNodeFilesResults",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def autorunPlaylistStarted(component: object, sensor_node_id=0):
    """
    Forwards the autorun playlist started message to the Dashboard.
    """
    # Send the Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "autorunPlaylistStarted",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def autorunPlaylistFinished(component: object, sensor_node_id=0):
    """
    Forwards the autorun playlist finished message to the Dashboard.
    """
    # Send the Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "autorunPlaylistFinished",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def flowGraphError(component: object, sensor_node_id=0, error=""):
    """
    Forwards the flow graph error message to the Dashboard.
    """
    # Send the Message
    PARAMETERS = {"sensor_node_id": sensor_node_id, "error": error}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "flowGraphError",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def detectorFlowGraphError(component: object, sensor_node_id=0, error=""):
    """
    Forwards the detector flow graph error message to the Dashboard.
    """
    # Send the Message
    PARAMETERS = {"sensor_node_id": sensor_node_id, "error": error}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "detectorFlowGraphError",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def archivePlaylistFinished(component: object, sensor_node_id=0):
    """
    Forwards the Archive playlist finished message to the Dashboard.
    """
    # Send the Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "archivePlaylistFinished",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def archivePlaylistPosition(component: object, sensor_node_id=0, position=0):
    """
    Forwards the Archive playlist position to the Dashboard.
    """
    # Send the Message
    PARAMETERS = {"sensor_node_id": sensor_node_id, "position": position}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "archivePlaylistPosition",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def multiStageAttackFinished(component: object, sensor_node_id=0):
    """
    Forwards the multi-stage attack finished message to the Dashboard.
    """
    # Send the Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "multiStageAttackFinished",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def flowGraphFinishedSniffer(component: object, sensor_node_id=0, category=""):
    """
    Forwards the flow graph finished sniffer message to the Dashboard.
    """
    # Send the Message
    PARAMETERS = {"sensor_node_id": sensor_node_id, "category": category}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "flowGraphFinishedSniffer",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def flowGraphFinishedIQ_Inspection(component: object, sensor_node_id=0):
    """
    Forwards the flow graph finished IQ inspection message to the Dashboard.
    """
    # Send the Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "flowGraphFinishedIQ_Inspection",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def flowGraphFinishedIQ_Playback(component: object, sensor_node_id=0):
    """
    Forwards the flow graph finished IQ playback message to the Dashboard.
    """
    # Send the Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "flowGraphFinishedIQ_Playback",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def flowGraphFinishedIQ(component: object, sensor_node_id=0):
    """
    Forwards the flow graph finished IQ message to the Dashboard.
    """
    # Send the Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "flowGraphFinishedIQ",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def flowGraphFinished(component: object, sensor_node_id=0, category=""):
    """
    Forwards the flow graph finished message to the Dashboard.
    """
    # Send the Message
    PARAMETERS = {"sensor_node_id": sensor_node_id, "category": category}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "flowGraphFinished",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def flowGraphStartedSniffer(component: object, sensor_node_id=0, category=""):
    """
    Forwards the flow graph started IQ sniffer message to the Dashboard.
    """
    # Send the Message
    PARAMETERS = {"sensor_node_id": sensor_node_id, "category": category}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "flowGraphStartedSniffer",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def flowGraphStartedIQ_Inspection(component: object, sensor_node_id=0):
    """
    Forwards the flow graph started IQ inspection message to the Dashboard.
    """
    # Send the Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "flowGraphStartedIQ_Inspection",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def flowGraphStartedIQ_Playback(component: object, sensor_node_id=0):
    """
    Forwards the flow graph started IQ playback message to the Dasbhoard.
    """
    # Send the Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "flowGraphStartedIQ_Playback",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def flowGraphStartedIQ(component: object, sensor_node_id=0):
    """
    Forwards the flow graph started IQ message to the Dashboard.
    """
    # Send the Message
    PARAMETERS = {"sensor_node_id": sensor_node_id}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "flowGraphStartedIQ",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def flowGraphStarted(component: object, sensor_node_id=0, category=""):
    """
    Forwards the flow graph started message to the Dashboard.
    """
    # Send the Message
    PARAMETERS = {"sensor_node_id": sensor_node_id, "category": category}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "flowGraphStarted",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def recallSettingsReturn(component: object, dashboard_node_index, uuid, settings_dict):
    """
    Returns the recalled sensor node settings to the Dashboard.
    """
    # Send the Message
    PARAMETERS = {"settings_dict": settings_dict}
    msg1 = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "recallSettingsReturn",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg1)

    # Send First Connected Message
    component.dashboard_node_map[int(dashboard_node_index)] = uuid
    component.nodes[uuid]["connected"] = True
    PARAMETERS = {"component_name": dashboard_node_index}
    msg2 = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "componentConnected",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg2)


async def hardwareProbeResults(component: object, tab_index=0, output="", height_width=[]):
    """
    Forwards the hardware probe results message to the Dashboard.
    """
    # PARAMETERS = {"tab_index": tab_index, "output": eval(f'"{output}"'), "height_width": height_width}
    PARAMETERS = {"tab_index": tab_index, "output": output, "height_width": height_width}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "hardwareProbeResults",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def hardwareScanResults(component: object, uuid="", hardware_scan_results=[]):
    """
    Forwards the hardware scan results message to the Dashboard.
    """
    # Resolve Dashboard Hardware Tab
    node, tab_index = component.resolve_dashboard_target(uuid)
    if tab_index is None:
        return
    
    # Send Message
    PARAMETERS = {"tab_index": tab_index, "hardware_scan_results": hardware_scan_results}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "hardwareScanResults",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def hardwareGuessResults(
    component: object, tab_index=0, table_row=0, hardware_type="", scan_results="", new_guess_index=0
):
    """
    Forwards sensnor node hardware guess results from HIPRFISR to Dashboard.
    """
    PARAMETERS = {
        "tab_index": tab_index,
        "table_row": table_row,
        "hardware_type": hardware_type,
        "scan_results": scan_results,
        "new_guess_index": new_guess_index,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "hardwareGuessResults",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def bandID_Return(component: object, sensor_node_id=0, band_id=0, frequency=0):
    """
    Forwards the band ID return message for TSI detectors to the Dashboard.
    """
    PARAMETERS = {"sensor_node_id": sensor_node_id, "band_id": band_id, "frequency": frequency}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "bandID_Return",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def detectorReturn(component: object, frequency_value=0, power_value=0, time_value=0.0):
    """
    Forwards the TSI Detector return message with signals of interest to the Dashboard.
    """
    # Send the Message
    PARAMETERS = {"frequency_value": frequency_value, "power_value": power_value, "time_value": time_value}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "detectorReturn",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def saveFile(component: object, sensor_node_id=0, operation="", filepath="", data=""):
    """
    Saves a file from a remote sensor node to the local HIPRFISR computer.
    """
    # Save File and Send Message to Dashboard
    if operation == "IQ":
        # Save
        if len(filepath) > 0:
            with open(filepath, "wb") as file:
                file.write(binascii.a2b_hex(data))

        # Send Message to Dashboard
        PARAMETERS = {"sensor_node_id": sensor_node_id}
        msg = {
            fissure.comms.MessageFields.IDENTIFIER: component.identifier,
            fissure.comms.MessageFields.MESSAGE_NAME: "flowGraphFinishedIQ",
            fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
        }
        await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)

    elif operation == "Download":
        # Save
        if len(filepath) > 0:
            with open(filepath, "wb") as file:
                file.write(binascii.a2b_hex(data))

            # Send Message to Dashboard
            PARAMETERS = {"sensor_node_id": sensor_node_id}
            msg = {
                fissure.comms.MessageFields.IDENTIFIER: component.identifier,
                fissure.comms.MessageFields.MESSAGE_NAME: "fileDownloaded",
                fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def findGPS_CoordinatesResults(component: object, tab_index=0, coordinates=""):
    """
    Forwards the GPS coordinate results message to the Dashboard.
    """
    PARAMETERS = {"tab_index": tab_index, "coordinates": coordinates}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "findGPS_CoordinatesResults",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def alertReturn(component: object, sensor_node_id=0, alert_text=""):
    """
    Forwards alertReturn Message to the Dashboard.
    """
    component.logger.info(alert_text)  # TODO: Provide cleaned up console text for alerts

    # Forward to Dashboard
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "alert_text": alert_text,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "alertReturn",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def takPlot(component: object, uid: str, lat: float, lon: float, alt: float, time: str, remarks: str, type: str):
    """
    Forwards the GPS coordinate results message to TAK.
    """
    prefix = component.settings['callsign_prefix']
    callsign = component.nodes[uid].get('callsign', f"{prefix}-{uid[:8]}")
    await component.send_cot(uid, callsign, lat, lon, alt, time, remarks, type)


async def takPlotGpsUpdate(component: object, uid: str, lat: float, lon: float, alt: float, time: str, remarks: str):
    """
    Forwards the sensor node GPS coordinates message to TAK.
    """
    time = time.replace(" ", "T")
    max_history = 5

    prefix = component.settings['callsign_prefix']
    callsign = component.nodes[uid].get('callsign', f"{prefix}-{uid[:8]}")

    await component.sensor_node_tracker.send_cot_gps_update(uid, callsign, lat, lon, alt, time, remarks, max_history)


async def exploit(component: object, sensor_node_id: str, protocol:str, modulation:str, hardware:str, type:str, attack:str, variables:str):
    """"
    Forwards the necesarry information to the proper exploit flow graph.
    """
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "protocol": protocol,
        "modulation": modulation,
        "hardware": hardware,
        "type": type,
        "attack": attack,
        "variables": variables,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "exploitReturn",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def snreport(component: object, sensor_node_id:str, text:str):
    """"
    Forwards the necesarry information to the proper exploit flow graph.
    """
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "text": text,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "snreport",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def gpsBeaconEnableDisableIP_Return(component: object, sensor_node_id:str, gps_tak_beacon_status: bool):
    """
    Forwards GPS TAK beacon state to the Dashboard.
    """
    # Forward to Dashboard
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "gps_tak_beacon_status": gps_tak_beacon_status,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "gpsBeaconEnableDisableIP_Return",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def uptimeIP_Return(component: object, sensor_node_id:str, uptime: str):
    """
    Forwards the uptimeIP_Return message to the Dashboard.
    """
    # Forward to Dashboard
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "uptime": uptime,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "uptimeIP_Return",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def memoryIP_Return(component: object, sensor_node_id:str, memory: str):
    """
    Forwards the memoryIP_Return message to the Dashboard.
    """
    # Forward to Dashboard
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "memory": memory,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "memoryIP_Return",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def diskIP_Return(component: object, sensor_node_id:str, disk: str):
    """
    Forwards the diskIP_Return message to the Dashboard.
    """
    # Forward to Dashboard
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "disk": disk,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "diskIP_Return",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def cpuIP_Return(component: object, sensor_node_id:str, cpu: str):
    """
    Forwards the cpuIP_Return message to the Dashboard.
    """
    # Forward to Dashboard
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "cpu": cpu,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "cpuIP_Return",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def processesIP_Return(component: object, sensor_node_id:str, processes: str):
    """
    Forwards the processesIP_Return message to the Dashboard.
    """
    # Forward to Dashboard
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "processes": processes,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "processesIP_Return",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def ifconfigIP_Return(component: object, sensor_node_id:str, ifconfig: str):
    """
    Forwards the ifconfigIP_Return message to the Dashboard.
    """
    # Forward to Dashboard
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "ifconfig": ifconfig,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "ifconfigIP_Return",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def iwconfigIP_Return(component: object, sensor_node_id:str, iwconfig: str):
    """
    Forwards the iwconfigIP_Return message to the Dashboard.
    """
    # Forward to Dashboard
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "iwconfig": iwconfig,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "iwconfigIP_Return",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg) 


##########################################################################
####################### Outdated/Incomplete/Unused #######################
##########################################################################


async def setTargetSOI(
    component: object, frequency=0, modulation="", bandwidth=0, continuous=False, start_frequency=0, end_frequency=0
):
    """The Dashboard has selected a target SOI to examine. This SOI will be looked up in the
    library to find a best-fit flow graph.
    """
    # Save the SOI
    component.settings["target_SOI"] = str(
        [frequency, modulation, bandwidth, continuous, start_frequency, end_frequency]
    )

    # System has now been Manually Triggered
    component.soi_manually_triggered = True

    component.process_sois = True


async def setSOI_SelectionMode(component: object, mode=0):
    """Sets the SOI selection mode for deciding when to load flow graphs."""
    component.settings["SOI_trigger_mode"] = int(mode)


async def setProcessSOIs(component: object, enabled=False, priorities=None, filters=None, parameters=None):
    """Enables/Disables SOI Check in the main event loop."""
    # Assign to Variables
    if enabled is True:
        component.process_sois = True
        component.soi_priorities = priorities
        component.soi_filters = filters
        component.soi_parameters = parameters
    elif enabled is False:
        component.process_sois = False


async def setAutoStartPD(component: object, value=False):
    """
    Controls whether Protocol Discovery will begin immediately when a target signal is selected.
    """
    if value is True:
        component.auto_start_pd = True
    elif value is False:
        component.auto_start_pd = False


async def clear_SOI_List(component: object):
    """Clears the SOI List"""
    component.logger.debug("Executing Callback: Clear SOI List")
    component.soi_list = []


async def setHeartbeatInterval(component: object, interval=0):
    """Saves the settings changes made in the Dashboard to the HIPRFISR."""
    component.settings["heartbeat_interval"] = str(int(interval))

    # Send Change to TSI
    # component.tsi_hiprfisr_server.sendmsg(
    #     "Commands", Identifier="HIPRFISR", MessageName="Set Heartbeat Interval", Parameters=interval
    # )


async def SOI_Check(component: object, trigger_mode=""):
    """The methods for deciding when to examine SOIs"""
    returned_SOI = None

    # Manual Selection
    if trigger_mode == 0:
        component.logger.info("TRIGGER MODE 0")
        if component.soi_manually_triggered is True:
            component.process_sois = False
            component.soi_manually_triggered = False
            component.logger.info("SOI Triggered Manually: New Target Selected")
            returned_SOI = component.settings["target_SOI"]

            # Search Library for Flow Graphs
            # ~ searchLibraryForFlowGraphs(
            #     [
            #         returned_SOI[0],
            #         returned_SOI[1],
            #         returned_SOI[2],
            #         returned_SOI[3],
            #         returned_SOI[4],
            #         returned_SOI[5],
            #         0,
            #         0,
            #         0,
            #         0
            #     ]
            # )
            component.searchLibraryForFlowGraphs(
                ["", returned_SOI[1], "", "", "", "", 0, 0, 0, 0], None
            )  # Modulation Only

    # Time Elapsed
    elif trigger_mode == 1:
        component.logger.info("TRIGGER MODE 1")
        current_time = time.time()
        if (current_time - float(component.settings["SOI_trigger_time"])) > float(
            component.settings["SOI_trigger_timeout"]
        ):  # SOI_trigger_time should not be in YAML
            component.settings["SOI_trigger_time"] = str(current_time)
            component.logger.info("SOI Timeout: Selecting New Target")

            if len(component.soi_list) > 0:  # If the SOI list is not empty
                # Choose SOI from the current list
                returned_SOI = component.SOI_AutoSelect(
                    component.soi_list, component.soi_priorities, component.soi_filters
                )  # What happens if nothing is returned?

                # Send Message to Dashboard to Check Radio Button
                PARAMETERS = {"returned_soi": returned_SOI}
                msg = {
                    fissure.comms.MessageFields.IDENTIFIER: component.identifier,
                    fissure.comms.MessageFields.MESSAGE_NAME: "SOI Chosen",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
                }
                await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)

    # SOI quantity reached
    elif trigger_mode == 2:
        if len(component.soi_list) >= int(component.settings["SOI_quantity_limit"]):
            component.process_sois = False
            component.logger.info("SOI Quantity Reached: Selecting New Target")
            # Choose SOI from the current list
            returned_SOI = component.SOI_AutoSelect(
                component.soi_list, component.soi_priorities, component.soi_filters
            )  # What happens if nothing is returned?

            # Send Message to Dashboard to Check Radio Button
            PARAMETERS = {"returned_soi": returned_SOI}
            msg = {
                fissure.comms.MessageFields.IDENTIFIER: component.identifier,
                fissure.comms.MessageFields.MESSAGE_NAME: "SOI Chosen",
                fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)

    return returned_SOI


async def ignoreSOIs(component: object, dashboard_soi_blacklist=[]):
    """
    Copies the Dashboard's blacklisted items to the HIPRFISR. These items will be removed from the HIPRFISR SOI list.
    """
    # Copy the Dashboard Blacklist
    component.soi_blacklist = dashboard_soi_blacklist

    # Remove Blacklisted SOIs from SOI List
    for soi in dashboard_soi_blacklist:
        for x in reversed(range(0, len(component.soi_list))):
            if soi == component.soi_list[x][1] + "," + component.soi_list[x][0]:
                del component.soi_list[x]


async def checkPlugin(component: object, sensor_node_id: int):
    """Check Status of Plugins on Sensor Node

    Parameters
    ----------
    component : object
        Component
    sensor_node_id : int, optional
        Sensor node ID
    """
    # Get plugin names
    plugin_names = plugin.get_local_plugin_names()

    if sensor_node_id > -1:
        # Forward message to sensor node
        PARAMETERS = {
            "sensor_node_id": sensor_node_id,
            "plugin_names": plugin_names
        }
        msg = {
            fissure.comms.MessageFields.IDENTIFIER: component.identifier,
            fissure.comms.MessageFields.MESSAGE_NAME: "checkPlugin",
            fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
        }

        # Resolve Identity
        uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
        if identity is None:
            return
        
        # Send through ROUTER
        await component.sensor_node_router.send_msg(
            fissure.comms.MessageTypes.COMMANDS,
            msg,
            target_ids=[identity]
        )

    # Sensor node is local to HIPRFISR
    else:
        # Get status; deployed is N/A as it is local
        status = {}
        run_db_install = False
        uninstall_plugins = []
        for plugin_name in plugin_names:
            # Check if installed
            installed = plugin.installed(plugin_name)

            # Create status entry
            status[plugin_name] = {
                'deployed': 'N/A',
                'installed': installed
            }

            if installed and (not plugin_name in component.local_plugins):
                # Plugin is installed on sensor node but not registered in HIPRFISR; register for installation
                run_db_install = True
                await registerPlugin(component, -1, plugin_name)

            elif (not installed) and (plugin_name in component.local_plugins):
                # Plugin is not installed on sensor node but is registered in hipfisr; deregister and remove from database
                uninstall_plugins += [plugin_name]
                await deregisterPlugin(component, -1, plugin_name)

        if len(uninstall_plugins) > 0:
            # Uninstall plugins from database
            await uninstallPluginsDatabase(component, -1, uninstall_plugins)
        if run_db_install:
            # Install plugins to database
            await installPluginsDatabase(component, -1, True)

        # return status
        PARAMETERS = {
            "sensor_node_id": sensor_node_id,
            "plugin_status": status,
        }
        msg = {
            fissure.comms.MessageFields.IDENTIFIER: component.identifier,
            fissure.comms.MessageFields.MESSAGE_NAME: "checkSensorNodePluginResults",
            fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
        }
        await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def checkSensorNodePluginResults(component: object, sensor_node_id: int, plugin_status: dict):
    """Handle checkPlugin Response from Sensor Node

    Parameters
    ----------
    component : object
        Component
    sensor_node_id : int, optional
        Sensor node ID
    plugin_status : dict
        Status (values) of plugins (keys)
    """
    # Align database to sensor node
    run_db_install = False
    uninstall_plugins = []
    for plugin_name in plugin_status.keys():
        installed = plugin_status.get(plugin_name).get('installed')
        if installed and (not plugin_name in component.sensor_nodes[sensor_node_id].plugins):
            # Plugin is installed on sensor node but not registered in hipfisr; register for installation
            run_db_install = True
            await registerPlugin(component, sensor_node_id, plugin_name)

        elif (not installed) and (plugin_name in component.sensor_nodes[sensor_node_id].plugins):
            # Plugin is not installed on sensor node but is registered in hipfisr; deregister and remove from database
            uninstall_plugins += [plugin_name]
            await deregisterPlugin(component, sensor_node_id, plugin_name)

    if len(uninstall_plugins) > 0:
        # Uninstall plugins from database
        await uninstallPluginsDatabase(component, sensor_node_id, uninstall_plugins)
    if run_db_install:
        # Install plugins to database
        await installPluginsDatabase(component, sensor_node_id, True)

    # Forward results to Dashboard
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "plugin_status": plugin_status,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "checkSensorNodePluginResults",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def savePlugin(component: object, plugin_name: str, plugin_data: str):
    """Save Plugin to Local HIPRFISR

    Parameters
    ----------
    component : object
        Component
    plugin_name : str
        Plugin name
    plugin_data : str
        Plugin data in hex format (compressed as a zip file)
    """
    # Decode hex data
    plugin_data = binascii.a2b_hex(plugin_data)

    # Save file
    pathname = os.path.join(fissure.utils.PLUGIN_DIR, plugin_name + '.zip')
    with open(pathname, "wb") as f:
        f.write(plugin_data)

    # Create a path for the plugin to be extracted to
    extract_path = os.path.join(fissure.utils.PLUGIN_DIR, plugin_name)
    if os.path.exists(extract_path):
        copy_num = 1
        base_name = plugin_name
        while os.path.exists(extract_path):
            extract_path = os.path.join(fissure.utils.PLUGIN_DIR, f"{base_name} (Copy {copy_num})")
            copy_num += 1
    os.makedirs(extract_path, exist_ok=True)

    # Extract the zip file to the plugin directory
    with zipfile.ZipFile(pathname, "r") as zip_ref:
        zip_ref.extractall(extract_path)

    # Remove the zip file
    os.remove(pathname)


async def sendPlugin(component: object, plugin_name: str) -> None:
    """Send Plugin to Dashboard

    Parameters
    ----------
    component : object
        Component
    plugin_name : str
        Plugin name
    """
    plugin_path = os.path.join(fissure.utils.PLUGIN_DIR, plugin_name)
    if not os.path.exists(plugin_path):
        component.logger.error(f"Plugin {plugin_name} does not exist in {fissure.utils.PLUGIN_DIR}")
        return

    # Create a temporary zip file for the plugin directory
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as temp_zip:
        with zipfile.ZipFile(temp_zip.name, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(plugin_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, plugin_path)
                    zipf.write(file_path, arcname)
        with open(temp_zip.name, "rb") as f:
            zip_data = f.read()
        hex_zip_data = binascii.hexlify(zip_data).decode("utf-8").upper()

    # Send the plugin to the dashboard
    PARAMETERS = {
        "plugin_name": plugin_name,
        "plugin_data": hex_zip_data,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "savePlugin",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)

async def transferPlugins(component: object, sensor_node_id: int, plugin_names: List[str], install: bool=False):
    """Send Plugin to Sensor Node

    Parameters
    ----------
    component : object
        Component
    sensor_node_id : int
        Sensor node ID
    plugin_names : str
        Plugin names with file extension or no extension if folder
    install : bool
        Install plugin after transfer
    """
    plugins = []
    for plugin_name in plugin_names:
        pathname = os.path.join(fissure.utils.PLUGIN_DIR, plugin_name)
        if os.path.exists(pathname):
            zip_filename = None

            if not os.path.isfile(pathname):
                # zip directory
                shutil.make_archive(pathname, "zip", pathname)
                zip_filename = filename = pathname + ".zip"
                plugin_name += '.zip'
            else:
                filename = pathname

            # Read file
            try:
                with open(filename, "rb") as f:
                    plugin_data = f.read()
                plugin_data = binascii.hexlify(plugin_data)
                plugin_data = plugin_data.decode("utf-8").upper()
            except:
                component.logger.error("Error reading file: " + str(filename))
                return
            
            # Append to plugin data
            plugins += [(plugin_name, plugin_data)]

            # Delete the .zip File
            if not zip_filename is None:
                os.system('rm "' + zip_filename + '"')

    # Send File
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "plugins": plugins,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
        fissure.comms.MessageFields.MESSAGE_NAME: "transferPlugins" if not install else "transferPluginsInstall",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def installPlugins(component: object, sensor_node_id: int, plugin_names: List[str]):
    """Install Plugins to Sensor Node

    Parameters
    ----------
    component : object
        Component
    sensor_node_id : int
        Sensor node ID
    plugin_names : List[str]
        Plugin names
    """
    if sensor_node_id > -1:
        # Install through sensor node
        PARAMETERS = {
            "sensor_node_id": sensor_node_id,
            "plugin_names": plugin_names,
        }
        msg = {
            fissure.comms.MessageFields.IDENTIFIER: component.identifier,
            fissure.comms.MessageFields.MESSAGE_NAME: "installPlugins",
            fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
        }

        # Resolve Identity
        uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
        if identity is None:
            return
        
        # Send through ROUTER
        await component.sensor_node_router.send_msg(
            fissure.comms.MessageTypes.COMMANDS,
            msg,
            target_ids=[identity]
        )        

    else:
        # Install Locally
        for plugin_name in plugin_names:
            # Run Installation
            plugin.install(plugin_name)

            # Register in HIPRFISR
            await registerPlugin(component, -1, plugin_name)

        # Install to Database
        await installPluginsDatabase(component, -1, True)


async def registerPlugin(component: object, sensor_node_id: int, plugin_name: str):
    """Register Plugin in HIPRFISR Sensor Node Plugin List

    Parameters
    ----------
    component : object
        Component
    sensor_node_id : int
        Sensor node ID
    plugin_name : str
        Plugin name
    """
    if sensor_node_id > -1:
        if not plugin_name in component.sensor_nodes[sensor_node_id].plugins:
            # Add plugin to list for sensor node
            component.sensor_nodes[sensor_node_id].plugins += [plugin_name]
    else:
        if not plugin_name in component.local_plugins:
            # Add plugin to list for hiprfisr
            component.local_plugins += [plugin_name]


async def deregisterPlugin(component: object, sensor_node_id: int, plugin_name: str):
    """Remove Plugin from HIPRFISR Sensor Node Plugin List

    Parameters
    ----------
    component : object
        Component
    sensor_node_id : int
        Sensor node ID
    plugin_name : str
        Plugin name
    """
    if sensor_node_id > -1:
        if plugin_name in component.sensor_nodes[sensor_node_id].plugins:
            # Remove plugin from list for sensor node
            component.sensor_nodes[sensor_node_id].plugins.remove(plugin_name)
    else:
        if plugin_name in component.local_plugins:
            # Remove plugin from list for hiprfisr
            component.local_plugins.remove(plugin_name)


async def installPluginsDatabase(component: object, sensor_node_id: int, refresh_frontend_widgets: bool=True):
    """Install Plugin in Database

    Parameters
    ----------
    component : object
        Component
    sensor_node_id : int
        Sensor node ID
    refresh_frontend_widgets : bool, optional
        Update dashboard UI widgets after installation, by default True
    """
    # Get registered active plugins list
    if sensor_node_id > -1:
        plugins = component.sensor_nodes[sensor_node_id].plugins
    else:
        plugins = component.local_plugins

    # Install plugins to database
    plugin.modify_database(component.logger, plugins, 'add')

    # Update database cache and dashboard
    await retrieveDatabaseCache(component, refresh_frontend_widgets)

    # Update plugin table list
    await checkPlugin(component, sensor_node_id)


async def uninstallPluginsDatabase(component: object, sensor_node_id: int, plugin_names: List[str]):
    """Uninstall Plugin from Database

    Parameters
    ----------
    component : object
        Component
    sensor_node_id : int
        Sensor node ID
    plugin_names : List[str]
        Plugin names
    """
    # Uninstall plugins from database
    plugin.modify_database(component.logger, plugin_names, 'remove')

    # Update database cache and dashboard
    await retrieveDatabaseCache(component, True)

    # Update plugin table\list
    await checkPlugin(component, sensor_node_id)


async def requestPluginsTransferInstall(component: object, sensor_node_id: int, plugin_names: str):
    """Sensor Node Request for Plugin Transfer then Install

    Parameters
    ----------
    component : object
        Component
    sensor_node_id : int
        Sensor node ID
    plugin_names : str
        Plugin name
    """
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "plugin_names": plugin_names
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "requestPluginsTransferInstall",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def uninstallPlugins(component: object, sensor_node_id: int, plugin_names: str):
    """Uninstall Plugins from Sensor Node

    Parameters
    ----------
    component : object
        Component
    sensor_node_id : int
        Sensor node ID
    plugin_names : str
        Plugin names
    """
    if sensor_node_id > -1:
        # Uninstall through sensor node
        PARAMETERS = {
            "sensor_node_id": sensor_node_id,
            "plugin_names": plugin_names,
        }
        msg = {
            fissure.comms.MessageFields.IDENTIFIER: component.identifier,
            fissure.comms.MessageFields.MESSAGE_NAME: "uninstallPlugins",
            fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
        }

        # Resolve Identity
        uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
        if identity is None:
            return
        
        # Send through ROUTER
        await component.sensor_node_router.send_msg(
            fissure.comms.MessageTypes.COMMANDS,
            msg,
            target_ids=[identity]
        )
    
    else:
        # Uninstall locally
        for plugin_name in plugin_names:
            # run uninstallation
            plugin.uninstall(plugin_name)

        # Update database and dashboard
        await uninstallPluginsDatabase(component, -1, plugin_names)


async def removePlugin(component: object, sensor_node_id: int, plugin_name: str):
    """Remove Plugin from Sensor Node

    **WARNING**: This will remove the plugin from the sensor node file system

    Parameters
    ----------
    component : object
        Component
    sensor_node_id : int
        Sensor node ID
    plugin_name : str
        Plugin name
    """
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "plugin_name": plugin_name,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "removePlugin",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )    
    

async def requestPluginNamesHiprfisr(component: object):
    """Handle Request for Plugin Names

    Parameters
    ----------
    component : object
        Component
    """
    PARAMETERS = {
        "plugin_names": plugin.get_local_plugin_names(),
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "responsePluginNamesHiprfisr",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def openPluginHiprfisr(component: object, plugin_name: str):
    """Handle Request for Plugin Names

    Parameters
    ----------
    component : object
        Component
    """
    # Read the Files
    component.openPluginEditor(plugin_name)

    # Return the Table Data
    PARAMETERS = {
        "plugin_name": component.plugin_editor.name,
        "table_data_json": component.plugin_editor.table_data,
        "install_files": component.plugin_editor.install_files,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "responsePluginTableData",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def closePluginHiprfisr(component: object):
    """Handle Request for Plugin Names

    Parameters
    ----------
    component : object
        Component
    """
    component.plugin_editor = None


async def pluginDelete(component: object, plugin_name: str, delete_from_library: bool):
    """
    Deletes a plugin folder and library/database files (optional).

    Parameters
    ----------
    component : object
        Component
    delete_from_library : bool
        Delete from database in addition to plugin folder
    """
    component.openPluginEditor(plugin_name)
    component.plugin_editor.deletePlugin(plugin_name, delete_from_library, component.os_info)
    component.plugin_editor = None

    # Update the Combobox of Plugins
    await requestPluginNamesHiprfisr(component)

    # Send Message to Dashboard to Update Library
    await retrieveDatabaseCache(component, True)


async def pluginApplyChanges(component: object, table_data_json: dict, supporting_files_data_json: dict):
    """Handle Request for Plugin Names

    Parameters
    ----------
    component : object
        Component
    table_data_json : dict
        Tables data from Plugin Editor tab
    supporting_files_data_json : dict
        Supporting Files data from Plugin Editor tab
    """
    component.plugin_editor.applyChanges(table_data_json, supporting_files_data_json, component.os_info)

    # Send Message to Dashboard to Update Library
    await retrieveDatabaseCache(component, True)
    

async def pluginAddProtocolHiprfisr(component: object, protocol_name: str):
    """Handle Request to Add Protocol Plugin Names

    Parameters
    ----------
    component : object
        Component
    """
    #component.pluginAddProtocolHiprfisr(protocol_name)

    # run add and get returned parameters
    PARAMETERS = {
        "plugin_name": component.plugin_editor.name,
        "protocol_name": protocol_name,
        "parameters": component.pluginAddProtocolHiprfisr(protocol_name)
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "responsePluginProtocolParameters",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def pluginSetProtocolParameters(component: object, protocol_name: str, parameters: dict):
    component.plugin_editor.edit_protocol(protocol_name, parameters.get('data_rates'), parameters.get('median_packet_lengths'))

    # run add and get returned parameters
    PARAMETERS = {
        "plugin_name": component.plugin_editor.name,
        "protocol_name": protocol_name,
        "parameters": component.pluginAddProtocolHiprfisr(protocol_name)
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "responsePluginProtocolParameters",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def pluginAddProtocolModType(component: object, protocol_name: str, mod_type: str):
    component.plugin_editor.add_mod_type(protocol_name, mod_type)

    # run add and get returned parameters
    PARAMETERS = {
        "plugin_name": component.plugin_editor.name,
        "protocol_name": protocol_name,
        "parameters": component.plugin_editor.get_protocol_parameters(protocol_name)
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "responsePluginProtocolParameters",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def pluginRemoveProtocolModTypes(component: object, protocol_name: str, mod_types: str):
    component.plugin_editor.remove_mod_types(protocol_name, mod_types)

    # run add and get returned parameters
    PARAMETERS = {
        "plugin_name": component.plugin_editor.name,
        "protocol_name": protocol_name,
        "parameters": component.plugin_editor.get_protocol_parameters(protocol_name)
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "responsePluginProtocolParameters",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def pluginEditProtocolPktTypes(component: object, protocol_name: str, pkt_types: List[List[str]]):
    component.plugin_editor.edit_pkt_types(protocol_name, pkt_types)

    # run add and get returned parameters
    PARAMETERS = {
        "plugin_name": component.plugin_editor.name,
        "protocol_name": protocol_name,
        "parameters": component.plugin_editor.get_protocol_parameters(protocol_name)
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "responsePluginProtocolParameters",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def plugin_get_operation_parameters(component: object, plugin: str, operation: str):
    """Get parameters for a plugin operation.

    Parameters
    ----------
    component : object
        Component
    plugin : str
        Plugin name
    operation : str
        Script relative path within the plugin's install_file directory
    """
    # get the plugin path
    plugin_path = os.path.join(fissure.utils.PLUGIN_DIR, plugin)
    if not os.path.exists(plugin_path):
        component.logger.error(f"Plugin {plugin} does not exist in {fissure.utils.PLUGIN_DIR}")
        return

    # get the operation path
    operation_path = os.path.join(plugin_path, "install_files", operation)
    if not os.path.exists(operation_path):
        component.logger.error(f"Operation {operation} does not exist in plugin {plugin}")
        return
    
    # import and run the get_arguments function from the operation script
    try:
        spec = importlib.util.spec_from_file_location("operation_module", operation_path)
        operation_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(operation_module)
        OperationMain = getattr(operation_module, "OperationMain", None)
        if OperationMain is not None:
            if callable(OperationMain.get_arguments):
                parameters = OperationMain.get_arguments()
            else:
                component.logger.error(f"OperationMain.get_arguments function not found in {operation}")
                return
        else:
            component.logger.error(f"OperationMain class not found in {operation}")
            return
        if callable(OperationMain.get_resources):
            resources = OperationMain.get_resources()
        else:
            component.logger.warning(f"OperationMain.get_resources function not found in {operation}, resources will not be included")
            resources = {}
        if callable(OperationMain.get_interfaces):
            interfaces = OperationMain.get_interfaces()
        else:
            component.logger.warning(f"OperationMain.get_interfaces function not found in {operation}, interfaces will not be included")
            interfaces = {}
    except Exception as e:
        component.logger.error(f"Error importing operation script {operation}: {e}")
        return

    # cast all values to strings (ensures json serializability)
    for key, value in parameters.items():
        for subkey, subvalue in value.items():
            parameters[key][subkey] = str(subvalue)

    # send the plugin operation parameters to the dashboard
    PARAMETERS = {
        "plugin": plugin,
        "operation": operation,
        "parameters": parameters,
        "resources": resources,
        "interfaces": interfaces
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "responsePluginOperationParameters",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def plugin_get_operations(component: object, plugin: str):
    """Get operations for a plugin.

    Parameters
    ----------
    component : object
        Component
    plugin : str
        Plugin name
    """
    # get the plugin path
    plugin_path = os.path.join(fissure.utils.PLUGIN_DIR, plugin)
    if not os.path.exists(plugin_path):
        component.logger.error(f"Plugin {plugin} does not exist in {fissure.utils.PLUGIN_DIR}")
        return
    
    # get the install_files path
    install_files_path = os.path.join(plugin_path, "install_files")
    if not os.path.exists(install_files_path):
        component.logger.error(f"Plugin {plugin} does not have an install_files directory")
        return
    
    # get the list of operations (python scripts) in the install_files directory
    operations = []
    for filename in os.listdir(install_files_path):
        if filename.endswith(".py"):
            operations += [filename]

    # send the plugin operations to the dashboard
    PARAMETERS = {
        "plugin": plugin,
        "operations": operations,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "responsePluginOperations",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


async def run_plugin_operation(component: object, sensor_node_id: int, plugin: str, operation: str, parameters: dict = {}):
    """Run a plugin operation on the sensor node.

    Parameters
    ----------
    component : object
        Component
    sensor_node_id : int
        Sensor node ID
    plugin : str
        Plugin name
    operation : str
        Script relative path within the plugin's install_file directory
    parameters : dict, optional
        Additional parameters for the operation, by default {}
    """
    PARAMETERS = {
        "plugin": plugin,
        "operation": operation,
        "parameters": parameters,
        "sensor_node_id": sensor_node_id,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "run_plugin_operation",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )    


async def stop_plugin_operation(component: object, sensor_node_id: int, operation_id: str):
    """Stop a running plugin operation on the sensor node.

    Parameters
    ----------
    component : object
        Component
    sensor_node_id : int
        Sensor node ID
    operation_id : str
        Unique identifier for the operation to stop
    """
    PARAMETERS = {
        "operation_id": operation_id,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "stop_plugin_operation",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def stop_all_plugin_operations(component: object, tak_uid: str, sensor_node_id: int=0):
    """Stop all running plugin operations on the sensor node.

    Parameters
    ----------
    component : object
        Component
    sensor_node_id : int
        Sensor node ID
    """
    component.logger.info(f"Stopping all plugin operations on sensor node {sensor_node_id}")
    PARAMETERS = {}
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "stop_all_plugin_operations",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    # uuid, identity = component.resolve_sensor_node_identity(sensor_node_id)
    identity = component.nodes[tak_uid].get("identity", None)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def sendPluginNamesTak(component: object, tak_uid: str, sensor_node_id: int=0):
    """Request Sensor Node plugin names for TAK

    Parameters
    ----------
    component : object
        Component
    tak_uid : str
        TAK unique identifier
    sensor_node_id : int, optional
        Sensor node ID, by default 0
    """
    PARAMETERS = {
        "tak_uid": tak_uid,
        "sensor_node_id": sensor_node_id,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "sendPluginNamesTak",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    identity = component.nodes[tak_uid].get("identity", None)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def sendPluginNamesTakResults(component: object, tak_uid: str, sensor_node_id: int, plugin_names: List[str]):
    """Handle Sensor Node plugin names for TAK

    Parameters
    ----------
    component : object
        Component
    tak_uid : str
        TAK unique identifier
    sensor_node_id : int
        Sensor node ID
    plugin_names : List[str]
        Plugin names
    """
    component.logger.debug(f"Preparing to send TAK plugin names for TAK UID: {tak_uid}")
    try:
        msg = pytak.gen_cot_xml(uid=tak_uid, stale=300)

        # Ensure msg is an Element; if pytak returned a string, try to parse it
        if not isinstance(msg, ET.Element):
            try:
                msg = ET.fromstring(msg)
            except Exception:
                component.logger.error("pytak.gen_cot_xml did not return a valid XML Element")
                return

        # Find existing <detail> or create one if missing
        detail = msg.find("detail")
        if detail is None:
            detail = ET.SubElement(msg, "detail")

        # add <remarks> as a child element (not an attribute)
        remarks = ET.SubElement(detail, "remarks")
        # prepare remarks with plugin names
        remarks.text = "FTN plugins: " + ", ".join(plugin_names)

        # Convert to bytes
        msg_bytes = ET.tostring(msg, encoding='utf-8')

        # send the message
        component.logger.debug("Sending TAK plugin names message: " + msg_bytes.decode('utf-8'))
        component.clitool.tx_queue.put_nowait(msg_bytes)
    except Exception as e:
        component.logger.error(f"Failed to send TAK plugin names: {e}")
        tb = traceback.format_exc()
        component.logger.debug(tb)

async def sendPluginActionNamesTak(component: object, tak_uid: str, plugin_name: str, sensor_node_id: int=0):
    """Request Sensor Node plugin action names for TAK

    Parameters
    ----------
    component : object
        Component
    tak_uid : str
        TAK unique identifier
    plugin_name : str
        Plugin name
    sensor_node_id : int, optional
        Sensor node ID, by default 0
    """
    PARAMETERS = {
        "tak_uid": tak_uid,
        "plugin_name": plugin_name,
        "sensor_node_id": sensor_node_id,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "sendPluginActionNamesTak",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    identity = component.nodes[tak_uid].get("identity", None)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )


async def sendPluginActionNamesTakResults(component: object, tak_uid: str, sensor_node_id: int, plugin_name: str, action_names: List[str]):
    """Handle Sensor Node plugin action names for TAK

    Parameters
    ----------
    component : object
        Component
    tak_uid : str
        TAK unique identifier
    sensor_node_id : int
        Sensor node ID
    plugin_name : str
        Plugin name
    action_names : List[str]
        Plugin action names
    """
    component.logger.debug(f"Preparing to send TAK plugin action names for TAK UID: {tak_uid}")
    try:
        msg = pytak.gen_cot_xml(uid=tak_uid, stale=300)

        # Ensure msg is an Element; if pytak returned a string, try to parse it
        if not isinstance(msg, ET.Element):
            try:
                msg = ET.fromstring(msg)
            except Exception:
                component.logger.error("pytak.gen_cot_xml did not return a valid XML Element")
                return

        # Find existing <detail> or create one if missing
        detail = msg.find("detail")
        if detail is None:
            detail = ET.SubElement(msg, "detail")

        # add <remarks> as a child element (not an attribute)
        remarks = ET.SubElement(detail, "remarks")
        # prepare remarks with plugin action names
        remarks.text = f"FTN plugin actions: {plugin_name}: " + ", ".join(action_names)

        # Convert to bytes
        msg_bytes = ET.tostring(msg, encoding='utf-8')

        # send the message
        component.logger.debug("Sending TAK plugin action names message: " + msg_bytes.decode('utf-8'))
        component.clitool.tx_queue.put_nowait(msg_bytes)
    except Exception as e:
        component.logger.error(f"Failed to send TAK plugin action names: {e}")
        tb = traceback.format_exc()
        component.logger.debug(tb)


async def sendPluginActionTak(component: object, tak_uid: str, sensor_node_id: int, plugin_name: str, action_name: str, parameters: dict):
    """Request Sensor Node plugin action for TAK

    Parameters
    ----------
    component : object
        Component
    tak_uid : str
        TAK unique identifier
    sensor_node_id : int
        Sensor node ID
    plugin_name : str
        Plugin name
    action_name : str
        Plugin action name
    parameters : dict
        Plugin action parameters
    """
    PARAMETERS = {
        "plugin_name": plugin_name,
        "action_name": action_name,
        "parameters": parameters,
        "sensor_node_id": sensor_node_id,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "plugin_action",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }

    # Resolve Identity
    identity = component.nodes[tak_uid].get("identity", None)
    if identity is None:
        return
    
    # Send through ROUTER
    await component.sensor_node_router.send_msg(
        fissure.comms.MessageTypes.COMMANDS,
        msg,
        target_ids=[identity]
    )    


async def pluginOperationStarted(component: object, sensor_node_id: int, operation_id: str, plugin: str, operation: str, parameters: dict):
    """Handle Plugin Operation Started Event

    Parameters
    ----------
    component : object
        Component
    sensor_node_id : int
        Sensor node ID
    operation_id : str
        Unique identifier for the operation
    plugin : str
        Plugin name
    operation : str
        Operation name
    parameters : dict
        Parameters for the operation
    """
    # Forward message to dashboard
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "operation_id": operation_id,
        "plugin": plugin,
        "operation": operation,
        "parameters": parameters,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "responsePluginOperationStarted",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)

async def pluginOperationStopped(component: object, sensor_node_id: int, operation_id: str, plugin: str, operation: str) -> None:
    """Handle Plugin Operation Stopped Event

    Parameters
    ----------
    component : object
        Component
    sensor_node_id : int
        Sensor node ID
    operation_id : str
        Unique identifier for the operation
    plugin : str
        Plugin name
    operation : str
        Operation name
    """
    # Forward message to dashboard
    PARAMETERS = {
        "sensor_node_id": sensor_node_id,
        "operation_id": operation_id,
        "plugin": plugin,
        "operation": operation,
    }
    msg = {
        fissure.comms.MessageFields.IDENTIFIER: component.identifier,
        fissure.comms.MessageFields.MESSAGE_NAME: "responsePluginOperationStopped",
        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
    }
    await component.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)