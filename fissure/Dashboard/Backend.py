# from .Signals import DashboardSignals
from inspect import isfunction
from PyQt5 import QtCore
from types import ModuleType
from typing import Dict, List, Tuple

import asyncio
import fissure.comms
import fissure.utils
from fissure.utils import PLUGIN_DIR
from fissure.utils.plugin import modify_database
import logging
import multiprocessing
import time
import zmq
import signal
import uuid

EVENT_LOOP_DELAY = 0.1  # Seconds


def run():
    """
    Never called.
    """
    asyncio.run(main())


async def main():
    """
    Never called.
    """
    print("[FISSURE][Dashboard] start")
    dashboard = DashboardBackend()

    await dashboard.begin()
    dashboard.shutdown()

    print("[FISSURE][Dashboard] end")
    fissure.utils.zmq_cleanup()


# Ignore SIGINT in the secondary process
def run_server():
    """
    Called by start_local_hiprfisr().
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    fissure.Server.run()


class DashboardBackend:
    callbacks: Dict = {}
    # logger: logging.Logger = fissure.utils.get_logger(f"{fissure.comms.Identifiers.DASHBOARD}.backend")
    frontend: QtCore.QObject
    settings: Dict
    ip_address: str
    os_info: Tuple[str, str, str]
    heartbeats: Dict[str, float]  # {name: time, name: time, ...}
    hiprfisr_address: fissure.comms.Address
    hiprfisr_connected: bool
    pd_connected: bool
    tsi_connected: bool
    sensor_node_connected: List[bool]
    session_active: bool  # For keeping track of StatusBar status
    shutdown: bool
    identifier: str = fissure.comms.Identifiers.DASHBOARD


    def __init__(self, frontend: QtCore.QObject):
        """
        """        
        self.logger = fissure.utils.get_logger(f"{fissure.comms.Identifiers.DASHBOARD}.backend")       
        self.logger.info("=== INITIALIZING ===")

        self.settings = fissure.utils.get_fissure_config()

        # Update Logging Levels
        fissure.utils.update_logging_levels(
            self.logger, 
            self.settings["console_logging_level"], 
            self.settings["file_logging_level"]
        )

        self.ip_address = fissure.utils.get_ip_address()
        self.hiprfisr_socket = None
        # self.initialize_comms()
        self.os_info = fissure.utils.get_os_info()

        # Initialize Connection/Heartbeat Variables
        self.heartbeats = {
            fissure.comms.Identifiers.DASHBOARD: None,
            fissure.comms.Identifiers.HIPRFISR: None,
            fissure.comms.Identifiers.PD: None,
            fissure.comms.Identifiers.TSI: None,
            fissure.comms.Identifiers.SENSOR_NODE: [
                {"time": None, "interval": None},
                {"time": None, "interval": None},
                {"time": None, "interval": None},
                {"time": None, "interval": None},
                {"time": None, "interval": None},
            ],
        }
        self.hiprfisr_address = None
        self.hiprfisr_connected = False
        self.pd_connected = False
        self.tsi_connected = False
        self.sensor_node_connected = [False, False, False, False, False]
        self.session_active = False
        self.shutdown = False
        self.shutting_down_message_received = False
        self.shutdown_complete = False
        self.child_tasks = []

        # Load Library
        self.plugins = []
        self.library = None
        self.frontend_initialized = False
        self.initial_database_retrieval = True

        self.frontend = frontend

        # Register Callbacks
        self.register_callbacks(fissure.callbacks.GenericCallbacks)
        self.register_callbacks(fissure.callbacks.DashboardCallbacks)
        self.register_callbacks(fissure.callbacks.DashboardCallbacksLT)

        self.logger.info("=== READY ===")


    def initialize_comms(self):
        """
        Create the Listener socket.
        """
        # Close on Reconnect
        if self.hiprfisr_socket:
            # self.hiprfisr_socket.shutdown()
            self.hiprfisr_socket = None 

        # Create HiprFisr Listener
        self.socket_id = f"{self.identifier}-{uuid.uuid4()}"
        self.hiprfisr_socket = fissure.comms.Listener(
            sock_type=zmq.PAIR, name=f"{fissure.comms.Identifiers.DASHBOARD}::backend"
        )
        self.hiprfisr_socket.set_identity(self.socket_id)


    async def shutdown_comms(self):
        """
        Cleanly shut down backend communications.
        Must be safe to call multiple times.
        """

        # If connected, perform a graceful disconnect
        if self.hiprfisr_connected:
            if self.__local_hiprfisr_process is not None:
                await self.shutdown_hiprfisr()
            else:
                await self.disconnect_from_hiprfisr()

        # SAFE SHUTDOWN (avoid NoneType crash)
        sock = self.hiprfisr_socket
        if sock is not None and hasattr(sock, "shutdown"):
            try:
                sock.shutdown()
            except Exception:
                pass

        # Ensure we never reference it again
        self.hiprfisr_socket = None


    def start(self):
        """
        Run the backend in the shared Qt/asyncio eventLoop
        """
        asyncio.ensure_future(self.__eventLoop__(), loop=asyncio.get_event_loop()).set_name("Dashboard Backend")


    def stop(self) -> bool:
        """
        Set the shutdown flag to stop the backend event loop. Is called in a loop on closing Dashboard.
        """
        if self.hiprfisr_connected is False:
            self.shutdown = True

        return not self.hiprfisr_connected


    def register_callbacks(self, ctx: ModuleType):
        """
        Register callbacks from the provided context

        :param ctx: context containing callbacks to register
        :type ctx: ModuleType
        """
        callbacks = [(f, getattr(ctx, f)) for f in dir(ctx) if isfunction(getattr(ctx, f))]
        for cb_name, cb_func in callbacks:
            self.callbacks[cb_name] = cb_func
        self.logger.debug(f"registered {len(callbacks)} callbacks from {ctx.__name__}")


    async def heartbeat_loop(self):
        """
        Sends and reads heartbeat messages, separate from event loop to prevent freezing on blocking events.
        """
        while self.shutdown is False:
            await self.send_heartbeat()
            await self.recv_heartbeat()
            self.check_heartbeats()

            await asyncio.sleep(EVENT_LOOP_DELAY)


    async def __eventLoop__(self):
        """
        DO NOT CALL DIRECTLY \\
        Instead call `DashboardBackend.start()` to run in the Qt/asyncio Event Loop
        """
        # Start Heartbeat Loop
        heartbeat_task = asyncio.create_task(self.heartbeat_loop())
        self.child_tasks.append(heartbeat_task)

        while self.shutdown is False:

            if self.hiprfisr_connected:
                await self.read_hiprfisr_messages()
                await asyncio.sleep(0)

                # Retrieve Initial Database Cache from HIPRFISR
                if self.initial_database_retrieval == True:
                    # Update Plugin Lists Retrieved from HIPRFISR Computer
                    #await self.requestPluginNamesHiprfisr()  # Future
                    #await self.checkPluginStatus(-1)  # Future

                    # Retrieve Initial Database Cache from HIPRFISR if Plugins Did Not Already
                    if self.library == None:
                        await self.retrieveDatabaseCache(False)
                    self.initial_database_retrieval = False
                
                # Inform the Frontend to Refresh
                if self.library != None and self.frontend_initialized == False:
                    self.frontend_initialized = True
                    self.frontend.__init2__()
            
            await asyncio.sleep(EVENT_LOOP_DELAY)

        # Ensure the Heartbeat Loop is Stopped
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass

        # Close Running Tasks
        for task in self.child_tasks:
            task.cancel()
        await asyncio.gather(*self.child_tasks, return_exceptions=True)

        # Shut Down Comms
        await self.shutdown_comms()

        # Give pyzmq a moment to resolve cancelled futures
        await asyncio.sleep(0)  # prevents errors/warnings

        fissure.utils.save_fissure_config(data=self.settings)  # Check for Remember Configuration is in save_fissure_config
        self.logger.info("=== BACKEND SHUTDOWN ===")
        self.shutdown_complete = True


    async def send_heartbeat(self):
        last_heartbeat = self.heartbeats.get(fissure.comms.Identifiers.DASHBOARD)
        now = time.time()
        if (last_heartbeat is None) or (now - last_heartbeat) >= float(self.settings.get("heartbeat_interval")):
            heartbeat = {
                fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                fissure.comms.MessageFields.MESSAGE_NAME: fissure.comms.MessageFields.HEARTBEAT,
                fissure.comms.MessageFields.TIME: now,
                fissure.comms.MessageFields.IP: self.ip_address,
            }
            if self.hiprfisr_connected:
                await self.hiprfisr_socket.send_heartbeat(heartbeat)
                self.heartbeats[fissure.comms.Identifiers.DASHBOARD] = now
                self.logger.debug(f"sent heartbeat ({fissure.utils.get_timestamp(now)})")


    async def recv_heartbeat(self):
        """
        """
        if self.shutdown:
            return

        try:
            heartbeat = await self.hiprfisr_socket.recv_heartbeat()
        except asyncio.CancelledError:
            return
        except Exception:
            # socket closed / shutdown in progress
            return

        if heartbeat is None or self.shutdown:
            return

        # HIPRFISR heartbeat timestamp
        heartbeat_time = float(heartbeat.get(fissure.comms.MessageFields.TIME))
        self.heartbeats[fissure.comms.Identifiers.HIPRFISR] = heartbeat_time
        self.logger.debug(
            f"received HiprFisr heartbeat ({fissure.utils.get_timestamp(heartbeat_time)})"
        )

        params = heartbeat.get(fissure.comms.MessageFields.PARAMETERS)
        if params is None or self.shutdown:
            return

        # PD/TSI heartbeats
        self.heartbeats[fissure.comms.Identifiers.PD] = params.get(fissure.comms.Identifiers.PD)
        self.heartbeats[fissure.comms.Identifiers.TSI] = params.get(fissure.comms.Identifiers.TSI)

        # sensor nodes
        sensor_data = params.get(fissure.comms.Identifiers.SENSOR_NODE)
        if sensor_data is None or self.shutdown:
            return

        node_list = self.heartbeats[fissure.comms.Identifiers.SENSOR_NODE]

        incoming_times = sensor_data.get(fissure.comms.MessageFields.TIME, [])
        incoming_intervals = sensor_data.get(fissure.comms.MessageFields.INTERVAL, [])

        for idx in range(5):
            if idx < len(incoming_times) and incoming_times[idx] is not None:
                node_list[idx]["time"] = incoming_times[idx]

            if idx < len(incoming_intervals) and incoming_intervals[idx] is not None:
                node_list[idx]["interval"] = incoming_intervals[idx]


    def check_heartbeats(self):
        current_time = time.time()

        # Global fallback interval used for HIPRFISR, PD, TSI, and nodes that have not reported yet
        global_interval = float(self.settings.get("heartbeat_interval"))
        failure_multiple = float(self.settings.get("failure_multiple"))

        #
        # --- HIPRFISR CHECK ---
        #
        last_hiprfisr = self.heartbeats.get(fissure.comms.Identifiers.HIPRFISR)
        hiprfisr_cutoff = current_time - (global_interval * failure_multiple)

        if last_hiprfisr is not None:
            if self.hiprfisr_connected and last_hiprfisr < hiprfisr_cutoff:
                self.hiprfisr_connected = False
                if self.session_active:
                    self.logger.warning("hiprfisr connection lost")
                    self.frontend.signals.ComponentStatus.emit(
                        fissure.comms.Identifiers.HIPRFISR, False, self.frontend.statusBar()
                    )

            elif (not self.hiprfisr_connected) and last_hiprfisr > hiprfisr_cutoff:
                self.hiprfisr_connected = True
                if self.session_active:
                    self.logger.warning("hiprfisr connection restored")
                    self.frontend.signals.ComponentStatus.emit(
                        fissure.comms.Identifiers.HIPRFISR, True, self.frontend.statusBar()
                    )
                else:
                    self.session_active = True

        #
        # --- PD CHECK ---
        #
        last_pd = self.heartbeats.get(fissure.comms.Identifiers.PD)
        pd_cutoff = current_time - (global_interval * failure_multiple)

        if last_pd is not None:
            if self.pd_connected and last_pd < pd_cutoff:
                self.pd_connected = False
                self.frontend.signals.ComponentStatus.emit(
                    fissure.comms.Identifiers.PD, False, self.frontend.statusBar()
                )
            elif (not self.pd_connected) and last_pd > pd_cutoff:
                self.pd_connected = True
                self.frontend.signals.ComponentStatus.emit(
                    fissure.comms.Identifiers.PD, True, self.frontend.statusBar()
                )

        #
        # --- TSI CHECK ---
        #
        last_tsi = self.heartbeats.get(fissure.comms.Identifiers.TSI)
        tsi_cutoff = current_time - (global_interval * failure_multiple)

        if last_tsi is not None:
            if self.tsi_connected and last_tsi < tsi_cutoff:
                self.tsi_connected = False
                self.frontend.signals.ComponentStatus.emit(
                    fissure.comms.Identifiers.TSI, False, self.frontend.statusBar()
                )
            elif (not self.tsi_connected) and last_tsi > tsi_cutoff:
                self.tsi_connected = True
                self.frontend.signals.ComponentStatus.emit(
                    fissure.comms.Identifiers.TSI, True, self.frontend.statusBar()
                )

        #
        # --- SENSOR NODE CHECKS ---
        #
        sensor_nodes = self.heartbeats.get(fissure.comms.Identifiers.SENSOR_NODE)

        for idx in range(5):
            node_record = sensor_nodes[idx]

            # SAFETY: No record at all (None) → treat as disconnected
            if node_record is None:
                if self.sensor_node_connected[idx]:  # only emit transition
                    self.sensor_node_connected[idx] = False
                    self.frontend.signals.ComponentStatus.emit(
                        f"fissure.comms.Identifiers.SENSOR_NODE_{idx+1}",
                        False,
                        self.frontend.statusBar()
                    )
                continue

            #
            # At this point, node_record is a dict with: {"time": X, "interval": Y}
            #

            # If time exists but is None → disconnected
            if node_record["time"] is None:
                if self.sensor_node_connected[idx]:
                    self.sensor_node_connected[idx] = False
                    self.frontend.signals.ComponentStatus.emit(
                        f"fissure.comms.Identifiers.SENSOR_NODE_{idx+1}",
                        False,
                        self.frontend.statusBar()
                    )
                continue

            last_time = float(node_record["time"])

            # Use node's interval if present, otherwise fallback to global
            interval = (
                float(node_record["interval"])
                if node_record["interval"] is not None
                else global_interval
            )

            node_cutoff = current_time - (interval * failure_multiple)

            # Transition to disconnected
            if self.sensor_node_connected[idx] and last_time < node_cutoff:
                self.sensor_node_connected[idx] = False
                self.frontend.signals.ComponentStatus.emit(
                    f"fissure.comms.Identifiers.SENSOR_NODE_{idx+1}",
                    False,
                    self.frontend.statusBar()
                )

            # Transition to connected
            elif (not self.sensor_node_connected[idx]) and last_time > node_cutoff:
                self.sensor_node_connected[idx] = True
                self.frontend.signals.ComponentStatus.emit(
                    f"fissure.comms.Identifiers.SENSOR_NODE_{idx+1}",
                    True,
                    self.frontend.statusBar()
                )


    async def read_hiprfisr_messages(self):
        """
        """
        if self.shutdown:
            return

        try:
            msg = await self.hiprfisr_socket.recv_msg()
        except asyncio.CancelledError:
            return
        except Exception:
            # socket is closed or dying during shutdown
            return

        if msg is None or self.shutdown:
            return

        msg_type = msg.get(fissure.comms.MessageFields.TYPE)

        if msg_type == fissure.comms.MessageTypes.HEARTBEATS:
            self.logger.warning(
                f"received heartbeat on message channel (from {msg.get(fissure.comms.MessageFields.IDENTIFIER)})"
            )

        elif msg_type == fissure.comms.MessageTypes.COMMANDS:
            try:
                await self.hiprfisr_socket.run_callback(self, msg)
            except asyncio.CancelledError:
                return
            except Exception:
                # ignore callback errors during shutdown
                pass

        elif msg_type == fissure.comms.MessageTypes.STATUS:
            msg_name = msg.get(fissure.comms.MessageFields.MESSAGE_NAME)
            if msg_name == "Disconnect OK":
                self.session_active = False
            elif msg_name == "Shutting Down":
                self.shutting_down_message_received = True


    async def start_local_hiprfisr(self):
        """
        Spawn Local HiprFisr Process
        """
        try:
            multiprocessing.set_start_method("spawn")  # The default `fork` start method causes ZMQ problems
        except RuntimeError:
            pass

        # Run
        self.__local_hiprfisr_process = multiprocessing.Process(target=run_server, name="FISSURE Server")
        self.__local_hiprfisr_process.start()

        # Give HiprFisr some time to spin up
        await asyncio.sleep(1)


    async def connect_to_hiprfisr(self, addr: fissure.comms.Address = None):
        """
        Connect Dashboard to a HiprFisr instance at the specified address

        :param addr: address of the HiprFisr instance
        :type addr: fissure.comms.Address
        """
        self.hiprfisr_address = addr
        if await self.hiprfisr_socket.connect(server_addr=self.hiprfisr_address, timeout=15):  # Small timeout affects connection on startup
            self.logger.info(f"connected to HiprFisr @ {self.hiprfisr_address}")
            self.hiprfisr_connected = True

            # Set Session Flag
            self.session_active = True

            # Send first Heartbeat
            await self.send_heartbeat()


    async def disconnect_from_hiprfisr(self):
        disconnect_notice = {
            fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
            fissure.comms.MessageFields.MESSAGE_NAME: "disconnect",
        }
        await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, disconnect_notice)

        while self.session_active:
            msg = await self.hiprfisr_socket.recv_msg()

            # HIPRFISR is gone — stop waiting
            if msg is None:
                self.logger.warning("HIPRFISR went away during disconnect — closing session immediately")
                self.close_session()
                break

            if (
                msg.get(fissure.comms.MessageFields.IDENTIFIER) == fissure.comms.Identifiers.HIPRFISR
                and msg.get(fissure.comms.MessageFields.TYPE) == fissure.comms.MessageTypes.STATUS
                and msg.get(fissure.comms.MessageFields.MESSAGE_NAME) == "Disconnect OK"
            ):
                self.logger.info("=== DISCONNECT ===")
                self.close_session()
                break


    async def shutdown_hiprfisr(self):
        shutdown_cmd = {
            fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
            fissure.comms.MessageFields.MESSAGE_NAME: "shutdown",
            fissure.comms.MessageFields.PARAMETERS: {
                fissure.comms.Parameters.IDENTIFIERS: [fissure.comms.Identifiers.HIPRFISR]
            },
        }

        # Send shutdown
        try:
            await self.hiprfisr_socket.send_msg(
                fissure.comms.MessageTypes.COMMANDS,
                shutdown_cmd
            )
        except Exception:
            # HIPRFISR already dead
            self.logger.warning("HIPRFISR unreachable — assuming already dead.")
            # self.close_session()
            # return
        
        # Immediately close our session without waiting.
        self.close_session()

        # # Wait for HIPRFISR exit message OR socket close
        # while self.session_active:
        #     if self.shutting_down_message_received:
        #         self.logger.warning("Received shutdown notice from HIPRFISR")
        #         self.close_session()
        #         break

        #     msg = await self.hiprfisr_socket.recv_msg()
        #     if msg is None:
        #         self.logger.warning("HIPRFISR socket closed — forcing shutdown")
        #         self.close_session()
        #         break

        #     await asyncio.sleep(0.1)


    def close_session(self):
        """
        """
        sock = self.hiprfisr_socket

        if sock is not None:

            # CLEAN WAY: Close the real ZMQ sockets
            try:
                sock.close_sockets()
            except Exception as e:
                print("Error closing HIPRFISR sockets:", e)

        # Wipe references
        self.hiprfisr_socket = None
        self.hiprfisr_address = None

        # Reset connections
        self.heartbeats[fissure.comms.Identifiers.DASHBOARD] = 0
        self.heartbeats[fissure.comms.Identifiers.HIPRFISR] = 0
        self.heartbeats[fissure.comms.Identifiers.PD] = 0
        self.heartbeats[fissure.comms.Identifiers.TSI] = 0
        self.heartbeats[fissure.comms.Identifiers.SENSOR_NODE] = [None] * 5

        self.hiprfisr_connected = False
        self.pd_connected = False
        self.tsi_connected = False
        self.sensor_node_connected = [False] * 5
        self.session_active = False

        # Reset frontend/database
        self.initial_database_retrieval = True
        self.frontend_initialized = False
        self.library = None


    async def connect_remote_sensor_node(self, sensor_node_id, ip_address, msg_port, hb_port, recall_settings):
        """
        Sends message to HIPRFISR to establish IP based connection to a remote sensor node.
        """
        PARAMETERS = {
            "sensor_node_id": str(sensor_node_id),
            "ip_address": ip_address,
            "msg_port": msg_port,
            "hb_port": hb_port,
            "recall_settings": recall_settings,
        }
        launch_cmd = {
            fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
            fissure.comms.MessageFields.MESSAGE_NAME: "connectToSensorNodeIP",
            fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
        }
        await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, launch_cmd)


    async def disconnect_local_sensor_node(self, sensor_node_id):
        """
        Forwards the terminate sensor node message to the HIPRFISR/Sensor Node.
        """
        PARAMETERS = {"sensor_node_id": str(sensor_node_id)}
        terminate_cmd = {
            fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
            fissure.comms.MessageFields.MESSAGE_NAME: "terminateSensorNode",
            fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
        }
        await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, terminate_cmd)


    async def disconnect_remote_sensor_node(self, sensor_node_id, delete_node):
        PARAMETERS = {
            "dashboard_index": str(sensor_node_id),
            "delete_node": delete_node,
        }
        disconnect_cmd = {
            fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
            fissure.comms.MessageFields.MESSAGE_NAME: "disconnectFromSensorNode",
            fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
        }
        await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, disconnect_cmd)


    async def disconnectFromMeshtastic(self, sensor_node_id):
        """
        Ends connections to local serial connection to Meshatastic.
        """
        PARAMETERS = {
            "sensor_node_id": str(sensor_node_id),
        }
        disconnect_cmd = {
            fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
            fissure.comms.MessageFields.MESSAGE_NAME: "disconnectFromMeshtastic",
            fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
        }
        await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, disconnect_cmd)


    async def scanHardware(self, tab_index, hardware_list):
        """
        Scans the listed hardware on the sensor node for information.
        """
        PARAMETERS = {"tab_index": tab_index, "hardware_list": hardware_list}
        scan_cmd = {
            fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
            fissure.comms.MessageFields.MESSAGE_NAME: "scanHardware",
            fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
        }
        await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, scan_cmd)


    async def probeHardware(self, tab_index, table_row_text):
        """
        Probes hardware connected to a sensor node.
        """
        PARAMETERS = {"tab_index": tab_index, "table_row_text": table_row_text}
        probe_cmd = {
            fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
            fissure.comms.MessageFields.MESSAGE_NAME: "probeHardware",
            fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
        }
        await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, probe_cmd)    


    async def guessHardware(self, tab_index=0, table_row=0, table_row_text=[], guess_index=0):
        """
        Guesses identifiers for hardware connected to a sensor node.
        """
        PARAMETERS = {
            "tab_index": tab_index,
            "table_row": table_row,
            "table_row_text": table_row_text,
            "guess_index": guess_index,
        }
        guess_cmd = {
            fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
            fissure.comms.MessageFields.MESSAGE_NAME: "guessHardware",
            fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
        }
        await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, guess_cmd)


    async def updateFISSURE_Configuration(self, settings_dict={}):
        """
        Updates the FISSURE settings for all components.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {"settings_dict": settings_dict}
            msg = {
                fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                fissure.comms.MessageFields.MESSAGE_NAME: "updateFISSURE_Configuration",
                fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def updateLoggingLevels(self, new_console_level, new_file_level):
        """
        Updates the console and file logging levels for all components.
        """
        # Update New Levels for the Dashboard
        fissure.utils.update_logging_levels(self.logger, new_console_level, new_file_level)

        # For Testing
        # self.logger.debug("=== debug ===")
        # self.logger.info("=== info ===")
        # self.logger.warning("=== warning ===")
        # self.logger.error("=== error ===")

        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {"new_console_level": new_console_level, "new_file_level": new_file_level}
            msg = {
                fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                fissure.comms.MessageFields.MESSAGE_NAME: "updateLoggingLevels",
                fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def archivePlaylistStart(
        self,
        sensor_node_id,
        flow_graph,
        filenames,
        frequencies,
        sample_rates,
        formats,
        channels,
        gains,
        durations,
        repeat,
        ip_address,
        serial,
        trigger_values,
    ):
        """
        Starts Archive Playlist in response to button press.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
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
                "trigger_values": trigger_values,
            }
            msg = {
                fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                fissure.comms.MessageFields.MESSAGE_NAME: "archivePlaylistStart",
                fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def archivePlaylistStop(self, sensor_node_id):
        """
        Stops Archive Playlist in response to button press.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {"sensor_node_id": sensor_node_id}
            msg = {
                fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                fissure.comms.MessageFields.MESSAGE_NAME: "archivePlaylistStop",
                fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def attackFlowGraphStart(self, sensor_node_id, flow_graph_filepath, variable_names, variable_values, file_type, run_with_sudo, autorun_index, trigger_values):
        """
        Sends a message to start a single-stage attack.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {"sensor_node_id": sensor_node_id, "flow_graph_filepath": flow_graph_filepath, "variable_names": variable_names, "variable_values": variable_values, "file_type": file_type, "run_with_sudo": run_with_sudo, "autorun_index": autorun_index, "trigger_values": trigger_values}
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "attackFlowGraphStart",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def attackFlowGraphStop(self, sensor_node_id, parameter, autorun_index):
        """
        Sends a message to stop a single-stage attack.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {"sensor_node_id": sensor_node_id, "parameter": parameter, "autorun_index": autorun_index}
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "attackFlowGraphStop",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def multiStageAttackStart(
        self, 
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
        Sends a message to start a multi-stage attack.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
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
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "multiStageAttackStart",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def multiStageAttackStop(self, sensor_node_id=0, autorun_index=0):
        """
        Sends a message to stop a multi-stage attack.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
                "autorun_index": autorun_index,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "multiStageAttackStop",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def autorunPlaylistStart(self, sensor_node_id, playlist_dict, trigger_values):
        """
        Sends a message to transfer and start an autorun playlist.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
                "playlist_dict": playlist_dict,
                "trigger_values": trigger_values,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "autorunPlaylistStart",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def autorunPlaylistExecute(self, sensor_node_id=0, playlist_filename=""):
        """
        Sends a message to execute an autorun playlist already located on the sensor node.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
                "playlist_filename": playlist_filename,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "autorunPlaylistExecute",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def autorunPlaylistStop(self, sensor_node_id=0):
        """
        Sends a message to stop the running autorun playlist.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "autorunPlaylistStop",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def overwriteDefaultAutorunPlaylist(self, sensor_node_id=0, playlist_dict={}):
        """
        Sends a message to overwrite the default autorun playlist on the sensor node.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
                "playlist_dict": playlist_dict
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "overwriteDefaultAutorunPlaylist",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def refreshSensorNodeFiles(self, sensor_node_id=0, sensor_node_folder=""):
        """
        Sends a message to get the sensor node folder contents and return to the Dashboard.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
                "sensor_node_folder": sensor_node_folder
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "refreshSensorNodeFiles",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def deleteSensorNodeFile(self, sensor_node_id=0, sensor_node_file=""):
        """
        Deletes a file/folder on the sensor node.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
                "sensor_node_file": sensor_node_file
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "deleteSensorNodeFile",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def downloadSensorNodeFile(self, sensor_node_id=0, sensor_node_file="", download_folder=""):
        """
        Signals to sensor node to transfer a copy of a file or folder for saving it to a specified file path.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
                "sensor_node_file": sensor_node_file,
                "download_folder": download_folder
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "downloadSensorNodeFile",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def transferSensorNodeFile(self, sensor_node_id=0, local_file="", remote_folder="", refresh_file_list=False):
        """
        Loads a local file and transfers the data to a remote sensor node.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
                "local_file": local_file,
                "remote_folder": remote_folder,
                "refresh_file_list": refresh_file_list
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "transferSensorNodeFile",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg) 


    async def searchLibrary(self, soi_data="", field_data=""):
        """
        Sends message to search library.yaml for occurences of hex_str.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "soi_data": soi_data,
                "field_data": field_data,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "searchLibrary",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg) 


    async def addToLibrary(
        self,
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
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "protocol_name": protocol_name,
                "packet_name": packet_name,
                "packet_data": packet_data,
                "soi_data": soi_data,
                "modulation_type": modulation_type,
                "demodulation_fg_data": demodulation_fg_data,
                "attack": attack,
                "dissector": dissector,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "addToLibrary",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def removeFromLibrary(
        self,
        table_name = "",
        row_id = "",
        delete_files = False
    ):
        """
        Removes a row (and files) from the library database
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "table_name": table_name,
                "row_id": row_id,
                "delete_files": delete_files
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "removeFromLibrary",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def iqFlowGraphStart(self, sensor_node_id=0, flow_graph_filepath="", variable_names=[], variable_values=[], file_type=""):
        """
        Command for running an IQ flow graph.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
                "flow_graph_filepath": flow_graph_filepath,
                "variable_names": variable_names,
                "variable_values": variable_values,
                "file_type": file_type,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "iqFlowGraphStart",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def iqFlowGraphStop(self, sensor_node_id=0, parameter=""):
        """
        Command for stopping an IQ flow graph.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
                "parameter": parameter,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "iqFlowGraphStop",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def inspectionFlowGraphStart(
        self, 
        sensor_node_id=0, 
        flow_graph_filepath="", 
        variable_names=[], 
        variable_values=[], 
        file_type=""
    ):
        """
        Command for starting an inspection flow graph.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
                "flow_graph_filepath": flow_graph_filepath,
                "variable_names": variable_names,
                "variable_values": variable_values,
                "file_type": file_type,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "inspectionFlowGraphStart",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def inspectionFlowGraphStop(self, sensor_node_id=0, parameter=""):
        """
        Command for stopping an inspection flow graph.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
                "parameter": parameter,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "inspectionFlowGraphStop",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def findEntropy(self, message_length=0, preamble=""):
        """
        Sends a message to Protocol Discovery to find the entropy for the bit positions of fixed-length messages.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "message_length": message_length,
                "preamble": preamble,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "findEntropy",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)

    
    async def setBufferSize(self, min_buffer_size=0, max_buffer_size=0):
        """
        Sends a message to Protocol Discovery to find the entropy for the bit positions of fixed-length messages.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "min_buffer_size": min_buffer_size,
                "max_buffer_size": max_buffer_size,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "setBufferSize",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def clearPD_Buffer(self):
        """
        Sends a message to Protocol Discovery to clear its buffer.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "clearPD_Buffer",
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def stopPD(self, sensor_node_id=0):
        """
        Signals to PD to stop protocol discovery.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "stopPD",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)

    
    async def startPD(self, sensor_node_id=0):
        """
        Signals to PD and sensor node to start protocol discovery.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "startPD",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)
            

    async def setAutoStartPD(self, value=False):
        """
        Controls whether Protocol Discovery will begin immediately when a target signal is selected.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "value": value,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "setAutoStartPD",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def addPubSocket(self, ip_address="", port=0):
        """
        Signals to Protocol Discovery to add an additional ZMQ PUB for reading bits.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "ip_address": ip_address,
                "port": port,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "addPubSocket",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def removePubSocket(self, address=""):
        """
        Signals to Protocol Discovery to remove a ZMQ PUB.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {"address": address}
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "removePubSocket",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def searchLibraryForFlowGraphs(self, soi_data=[], hardware=""):
        """
        Queries protocol discovery to look in its version of the library to recommend flow graphs for the SOI.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "soi_data": soi_data,
                "hardware": hardware
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "searchLibraryForFlowGraphs",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def protocolDiscoveryFG_Stop(self, sensor_node_id=0):
        """
        Sends message to Sensor Node to stop a running flow graph.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "protocolDiscoveryFG_Stop",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def protocolDiscoveryFG_Start(self, sensor_node_id=0, flow_graph_filepath="", variable_names=[], variable_values=[]):
        """
        Sends message to Sensor Node to run a flow graph.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
                "flow_graph_filepath": flow_graph_filepath,
                "variable_names": variable_names,
                "variable_values": variable_values
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "protocolDiscoveryFG_Start",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def setVariable(self, sensor_node_id=0, flow_graph="", variable="", value=""):
        """
        Sends a message to Sensor Node to change the variable of the running flow graph.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
                "flow_graph": flow_graph,
                "variable": variable,
                "value": value
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "setVariable",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)
            
    
    async def findPreambles(self, window_min=0, window_max=0, ranking=0, std_deviations=0):
        """
        Sends message to PD to search the buffer for preambles.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "window_min": window_min,
                "window_max": window_max,
                "ranking": ranking,
                "std_deviations": std_deviations
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "findPreambles",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)
            

    async def sliceByPreamble(self, preamble="", first_n=0, estimated_length=0):
        """
        Sends message to PD to slice the data by a single preamble.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "preamble": preamble,
                "first_n": first_n,
                "estimated_length": estimated_length,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "sliceByPreamble",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def snifferFlowGraphStart(self, sensor_node_id=0, flow_graph_filepath="", variable_names=[], variable_values=[]):
        """
        Starts a sniffer flow graph.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
                "flow_graph_filepath": flow_graph_filepath,
                "variable_names": variable_names,
                "variable_values": variable_values,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "snifferFlowGraphStart",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def snifferFlowGraphStop(self, sensor_node_id=0, parameter=""):
        """
        Stops a sniffer flow graph.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
                "parameter": parameter,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "snifferFlowGraphStop",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def physicalFuzzingStop(self, sensor_node_id=0):
        """
        Sends message to Sensor Node to stop the physical fuzzing being performed on a running flow graph.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "physicalFuzzingStop",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)

    
    async def physicalFuzzingStart(
        self, 
        sensor_node_id=0,
        fuzzing_variables=[],
        fuzzing_type="",
        fuzzing_min=0,
        fuzzing_max=0,
        fuzzing_update_period=0,
        fuzzing_seed_step=0,
    ):
        """
        Command for starting physical fuzzing on a running flow graph.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
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
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "physicalFuzzingStart",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def clearWidebandList(self):
        """
        Clears the Wideband List.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "clearWidebandList",
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)

    
    async def updateConfiguration(
        self, 
        sensor_node_id=0, 
        start_frequency=0, 
        end_frequency=0, 
        step_size=0, 
        dwell_time=0,
        detector_port=0
    ):
        """
        Forwards the Update Configuration message to a sensor node.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
                "start_frequency": start_frequency,
                "end_frequency": end_frequency,
                "step_size": step_size,
                "dwell_time": dwell_time,
                "detector_port": detector_port,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "updateConfiguration",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def addBlacklist(self, start_frequency=0, end_frequency=0):
        """
        Forwards Add Blacklist message to TSI.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "start_frequency": start_frequency,
                "end_frequency": end_frequency,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "addBlacklist",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def removeBlacklist(self, start_frequency=0, end_frequency=0):
        """
        Forwards Remove Blacklist message to TSI.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "start_frequency": start_frequency,
                "end_frequency": end_frequency,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "removeBlacklist",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)

    
    async def startTSI_Detector(self, sensor_node_id=0, detector="", variable_names=[], variable_values=[], detector_port=0):
        """
        Signals to sensor node to start TSI detector.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
                "detector": detector,
                "variable_names": variable_names,
                "variable_values": variable_values,
                "detector_port": detector_port,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "startTSI_Detector",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def stopTSI_Detector(self, sensor_node_id=0):
        """
        Signals to sensor node to stop TSI detector.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "stopTSI_Detector",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)

    
    async def startTSI_Conditioner(
        self,
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
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
                "common_parameter_names": common_parameter_names,
                "common_parameter_values": common_parameter_values,
                "method_parameter_names": method_parameter_names,
                "method_parameter_values": method_parameter_values,
                "method_filepath": method_filepath,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "startTSI_Conditioner",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def stopTSI_Conditioner(self, sensor_node_id=0):
        """
        Signals to TSI to stop TSI conditioner.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "stopTSI_Conditioner",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def startTSI_FE(self, common_parameter_names=[], common_parameter_values=[]):
        """
        Signals to TSI to start TSI feature extractor.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "common_parameter_names": common_parameter_names,
                "common_parameter_values": common_parameter_values,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "startTSI_FE",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def stopTSI_FE(self):
        """
        Signals to TSI to stop TSI feature extractor.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "stopTSI_FE",
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def startScapy(self, sensor_node_id=0, interface="", interval=0, loop=False, operating_system=""):
        """
        Signals to Sensor Node to start Scapy.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
                "interface": interface,
                "interval": interval,
                "loop": loop,
                "operating_system": operating_system,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "startScapy",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def stopScapy(self, sensor_node_id=0):
        """
        Signals to Sensor Node to stop Scapy.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "stopScapy",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def deleteArchiveReplayFiles(self, sensor_node_id=0):
        """
        Deletes all the files in the Archive_Replay folder on the sensor node ahead of file transfer for replay.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "deleteArchiveReplayFiles",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def retrieveDatabaseCache(self, refresh_frontend_widgets=False):
        """
        Retrieves a copy of important database tables needed for operating the Dashboard.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "refresh_frontend_widgets": refresh_frontend_widgets,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "retrieveDatabaseCache",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def checkPluginStatus(self, sensor_node_id: int):
        """Check Status of Plugins on Sensor Node

        Parameters
        ----------
        sensor_node_id : int
            Sensor node ID
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "checkPlugin",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def transferPlugins(self, sensor_node_id: int, plugin_names: List[str]):
        """Transfer Plugins from HIPFISR to Sensor Node

        Parameters
        ----------
        sensor_node_id : int
            Sensor node ID
        plugin_names : str
            Plugin names with file extension or no extension if folder
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
                "plugin_names": plugin_names
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "transferPlugins",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def installPlugins(self, sensor_node_id: int, plugin_names: List[str]):
        """Install Plugins on Sensor Node

        Parameters
        ----------
        sensor_node_id : int
            Sensor node ID
        plugin_names : str
            Plugin names with file extension or no extension if folder
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
                "plugin_names": plugin_names
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "installPlugins",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def uninstallPlugin(self, sensor_node_id: int, plugin_name: str):
        """Uninstall Plugin on Sensor Node

        Parameters
        ----------
        sensor_node_id : int
            Sensor node ID
        plugin_name : str
            Plugin name with file extension or no extension if folder
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
                "plugin_name": plugin_name
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "uninstallPlugin",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def removePlugin(self, sensor_node_id: int, plugin_name: str):
        """Remove Plugin on Sensor Node

        **WARNING**: This will remove the plugin from the sensor node file system

        Parameters
        ----------
        sensor_node_id : int
            Sensor node ID
        plugin_name : str
            Plugin name with file extension or no extension if folder
        """
        if sensor_node_id > -1:
            # Send the Message
            if self.hiprfisr_connected is True:
                PARAMETERS = {
                    "sensor_node_id": sensor_node_id,
                    "plugin_name": plugin_name
                }
                msg = {
                        fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                        fissure.comms.MessageFields.MESSAGE_NAME: "removePlugin",
                        fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
                }
                await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def requestPluginNamesHiprfisr(self):
        """
        Request Plugin Names from HIPRFISR
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "requestPluginNamesHiprfisr",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def openPluginHiprfisr(self, plugin_name: str):
        """
        Open Plugin for Editing on HIPRFISR
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "plugin_name": plugin_name
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "openPluginHiprfisr",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def closePluginHiprfisr(self):
        """
        Close Plugin for Editing on HIPRFISR
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "closePluginHiprfisr",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def pluginDelete(self, plugin_name: str, delete_from_library: bool):
        """
        Deletes a plugin folder and optionally removes it from the library/database.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "plugin_name": plugin_name,
                "delete_from_library": delete_from_library
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "pluginDelete",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def pluginApplyChanges(self, table_data_json: dict, supporting_files_data_json: dict):
        """
        Overwrites the csv files with table data.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "table_data_json": table_data_json,
                "supporting_files_data_json": supporting_files_data_json
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "pluginApplyChanges",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def pluginAddProtocolHiprfisr(self, protocol_name: str):
        """
        Add Protocol to Open Plugin on HIPRFISR
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "protocol_name": protocol_name
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "pluginAddProtocolHiprfisr",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def pluginSetProtocolParameters(self, protocol_name: str, parameters: dict):
        """
        
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "protocol_name": protocol_name,
                "parameters": parameters
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "pluginSetProtocolParameters",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def pluginAddProtocolModType(self, protocol_name: str, mod_type: str):
        """
        
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "protocol_name": protocol_name,
                "mod_type": mod_type
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "pluginAddProtocolModType",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def pluginRemoveProtocolModTypes(self, protocol_name: str, mod_types: List[str]):
        """
        
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "protocol_name": protocol_name,
                "mod_types": mod_types
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "pluginRemoveProtocolModTypes",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def pluginEditProtocolPktTypes(self, protocol_name: str, pkt_types: List[List[str]]):
        """
        
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "protocol_name": protocol_name,
                "pkt_types": pkt_types
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "pluginEditProtocolPktTypes",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def findGPS_Coordinates(self, tab_index=0, gps_source="", format=""):
        """
        Queries the remote sensor node for its GPS coordinates. 
        """
        PARAMETERS = {
            "tab_index": tab_index,
            "gps_source": gps_source,
            "format": format
        }
        find_gps_cmd = {
            fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
            fissure.comms.MessageFields.MESSAGE_NAME: "findGPS_Coordinates",
            fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
        }
        await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, find_gps_cmd)


    async def enableDisableListener(self, listener_type="", listener_name="", parameters={}):
        """
        Creates a listener if it does not exist and then toggles its enable/disable status.
        """
        PARAMETERS = {
            "listener_type": listener_type,
            "listener_name": listener_name,
            "parameters": parameters
        }
        listener_cmd = {
            fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
            fissure.comms.MessageFields.MESSAGE_NAME: "enableDisableListener",
            fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
        }
        await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, listener_cmd)


    async def deleteListener(self, listener_name=""):
        """
        Deletes an existing listener.
        """
        PARAMETERS = {"listener_name": listener_name}
        listener_cmd = {
            fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
            fissure.comms.MessageFields.MESSAGE_NAME: "deleteListener",
            fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
        }
        await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, listener_cmd)


    async def connectToSensorNodeMeshtastic(self, sensor_node_id, serial_port, serial_baud_rate):
        """
        Sends message to HIPRFISR to establish local serial connection to communicate with preconfigured remote sensor node.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": str(sensor_node_id),
                "serial_port": serial_port,
                "serial_baud_rate": serial_baud_rate,
            }
            launch_cmd = {
                fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                fissure.comms.MessageFields.MESSAGE_NAME: "connectToSensorNodeMeshtastic",
                fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, launch_cmd)


    async def gpsBeaconEnableDisableIP(self, sensor_node_id):
        """
        Sends a message to the HIPRFISR to enable/disable the GPS TAK beacon at the sensor node.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "gpsBeaconEnableDisableIP",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def gpsBeaconRefreshIP(self, sensor_node_id):
        """
        Sends a message to the HIPRFISR to retrieve the GPS TAK beacon state from the sensor node.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "gpsBeaconRefreshIP",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def rebootIP(self, sensor_node_id):
        """
        Sends a message to the HIPRFISR to reboot the sensor node computer.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "rebootIP",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def uptimeIP(self, sensor_node_id):
        """
        Sends a message to the HIPRFISR to retrieve the uptime of the sensor node computer.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "uptimeIP",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def memoryIP(self, sensor_node_id):
        """
        Sends a message to the HIPRFISR to retrieve the memory usage of the sensor node computer.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "memoryIP",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def diskIP(self, sensor_node_id):
        """
        Sends a message to the HIPRFISR to retrieve the disk usage of the sensor node computer.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "diskIP",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def cpuIP(self, sensor_node_id):
        """
        Sends a message to the HIPRFISR to retrieve the CPU percentage of the sensor node computer.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "cpuIP",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def processesIP(self, sensor_node_id):
        """
        Sends a message to the HIPRFISR to retrieve the processes on the sensor node computer.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "processesIP",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def ifconfigIP(self, sensor_node_id):
        """
        Sends a message to the HIPRFISR to retrieve the ifconfig output on the sensor node computer.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "ifconfigIP",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def iwconfigIP(self, sensor_node_id):
        """
        Sends a message to the HIPRFISR to retrieve the iwconfig output on the sensor node computer.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "iwconfigIP",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)            


    async def pingIP(self, sensor_node_id):
        """
        Sends a message to the HIPRFISR to ping the sensor node computer.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "pingIP",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def nodeRefreshIP(self, dashboard_node_index):
        """
        Sends a message to the HIPRFISR to.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "dashboard_node_index": dashboard_node_index
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "nodeRefreshIP",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def nodeSelectIP(self, dashboard_node_index, node_uuid):
        """
        Sends a message to the HIPRFISR to.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "dashboard_node_index": dashboard_node_index,
                "node_uuid": node_uuid
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "nodeSelectIP",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def nodeReconnectIP(self, dashboard_node_index):
        """
        Sends a message to the HIPRFISR to.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "dashboard_node_index": dashboard_node_index,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "nodeReconnectIP",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


#######################################################################################
############################## Low Throughput Messages ################################
#######################################################################################


    async def recallInfoMeshtasticLT(self, tab_index=""):
        """
        Sends message to HIPRFISR to recall sensor node config file information.
        """
        PARAMETERS = {
            "tab_index": tab_index,
        }
        msg = {
            fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
            fissure.comms.MessageFields.MESSAGE_NAME: "recallInfoMeshtasticLT",
            fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
        }
        await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def recallHardwareMeshtasticLT(self, tab_index=""):
        """
        Sends message to HIPRFISR to recall sensor node config file information.
        """
        PARAMETERS = {
            "tab_index": tab_index,
        }
        msg = {
            fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
            fissure.comms.MessageFields.MESSAGE_NAME: "recallHardwareMeshtasticLT",
            fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
        }
        await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)
        

    async def recallStatusMeshtasticLT(self, tab_index=""):
        """
        Sends message to HIPRFISR to recall sensor node status information.
        """
        PARAMETERS = {
            "tab_index": tab_index,
        }
        msg = {
            fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
            fissure.comms.MessageFields.MESSAGE_NAME: "recallStatusMeshtasticLT",
            fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
        }
        await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def findGPS_CoordinatesLT(self, tab_index=0, gps_source="", format=""):
        """
        Queries the remote sensor node for its GPS coordinates. 
        """
        PARAMETERS = {
            "tab_index": tab_index,
            "gps_source": gps_source,
            "format": format
        }
        find_gps_cmd = {
            fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
            fissure.comms.MessageFields.MESSAGE_NAME: "findGPS_CoordinatesLT",
            fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
        }
        await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, find_gps_cmd)


    async def scanHardwareLT(self, tab_index, hardware_list):
        """
        Scans the listed hardware on the sensor node for information.
        """
        PARAMETERS = {"tab_index": tab_index, "hardware_list": hardware_list}
        scan_cmd = {
            fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
            fissure.comms.MessageFields.MESSAGE_NAME: "scanHardwareLT",
            fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
        }
        await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, scan_cmd)


    async def probeHardwareLT(self, tab_index, table_row_text):
        """
        Probes hardware connected to a sensor node.
        """
        PARAMETERS = {"tab_index": tab_index, "table_row_text": table_row_text}
        probe_cmd = {
            fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
            fissure.comms.MessageFields.MESSAGE_NAME: "probeHardwareLT",
            fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
        }
        await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, probe_cmd)


    async def guessHardwareLT(self, tab_index=0, table_row=0, table_row_text=[], guess_index=0):
        """
        Guesses identifiers for hardware connected to a sensor node.
        """
        PARAMETERS = {
            "tab_index": tab_index,
            "table_row": table_row,
            "table_row_text": table_row_text,
            "guess_index": guess_index,
        }
        guess_cmd = {
            fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
            fissure.comms.MessageFields.MESSAGE_NAME: "guessHardwareLT",
            fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
        }
        await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, guess_cmd)


    async def autorunPlaylistExecuteLT(self, sensor_node_id=0, playlist_filename=""):
        """
        Sends a message to execute an autorun playlist already located on the sensor node.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
                "playlist_filename": playlist_filename,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "autorunPlaylistExecuteLT",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def autorunPlaylistStopLT(self, sensor_node_id=0):
        """
        Sends a message to stop the running autorun playlist.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "autorunPlaylistStopLT",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def gpsBeaconEnableMeshtasticLT(self, sensor_node_id: str):
        """
        Sends a message to enable the GPS TAK beacon.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "gpsBeaconEnableMeshtasticLT",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def gpsBeaconDisableMeshtasticLT(self, sensor_node_id: str):
        """
        Sends a message to disable the GPS TAK beacon.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "gpsBeaconDisableMeshtasticLT",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def rebootMeshtasticLT(self, sensor_node_id: str):
        """
        Sends a message to reboot the sensor node computer.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "rebootMeshtasticLT",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def uptimeMeshtasticLT(self, sensor_node_id: str):
        """
        Sends a message to retrieve the uptime of the sensor node computer.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "uptimeMeshtasticLT",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def memoryMeshtasticLT(self, sensor_node_id: str):
        """
        Sends a message to retrieve the memory usage of the sensor node computer.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "memoryMeshtasticLT",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def diskMeshtasticLT(self, sensor_node_id: str):
        """
        Sends a message to retrieve the disk usage of the sensor node computer.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "diskMeshtasticLT",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def cpuMeshtasticLT(self, sensor_node_id: str):
        """
        Sends a message to retrieve the CPU percentage of the sensor node computer.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "cpuMeshtasticLT",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def processesMeshtasticLT(self, sensor_node_id: str):
        """
        Sends a message to retrieve the processes on the sensor node computer.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "processesMeshtasticLT",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)


    async def ifconfigMeshtasticLT(self, sensor_node_id: str):
        """
        Sends a message to retrieve the ifconfig output on the sensor node computer.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "ifconfigMeshtasticLT",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)

    
    async def iwconfigMeshtasticLT(self, sensor_node_id: str):
        """
        Sends a message to retrieve the iwconfig output on the sensor node computer.
        """
        # Send the Message
        if self.hiprfisr_connected is True:
            PARAMETERS = {
                "sensor_node_id": sensor_node_id,
            }
            msg = {
                    fissure.comms.MessageFields.IDENTIFIER: fissure.comms.Identifiers.DASHBOARD,
                    fissure.comms.MessageFields.MESSAGE_NAME: "iwconfigMeshtasticLT",
                    fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
            }
            await self.hiprfisr_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)            

