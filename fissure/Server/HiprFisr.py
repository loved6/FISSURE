from inspect import isfunction
from types import ModuleType
from typing import Dict, List, Union

import asyncio
import fissure.callbacks
import fissure.comms
import fissure.utils
from fissure.utils.plugin_editor import PluginEditor
import sys
import time
import uuid
import zmq
import subprocess
import os
import atexit
import ssl
from datetime import datetime, timezone, timedelta
from fissure.utils.tak_server import load_config as load_tak_config
from fissure.utils.tak_server import TakReceiver
import pytak


import pytak
from fissure.utils.tak_server import load_config as load_tak_config
from fissure.utils.tak_server import TakReceiver

HEARTBEAT_LOOP_DELAY = 0.1  # Seconds
EVENT_LOOP_DELAY = 0.1


def run():
    asyncio.run(main())


async def main():
    """
    Server __main__.py does not call this function. Do not edit! Edit __init__() or begin().
    """
    # Initialize HIPFISR
    print("[FISSURE][HiprFisr] start")
    hiprfisr = HiprFisr()

    # Start Event Loop
    await hiprfisr.begin()

    # End and Clean Up
    print("[FISSURE][HiprFisr] end")
    fissure.utils.zmq_cleanup()


class SensorNode:
    listener: fissure.comms.Listener
    connected: bool
    uuid: str
    last_heartbeat: float  # FIX: Use this or self.heartbeats?
    terminated: bool
    plugins: list = []


    def __init__(self, connection_type="IP", serial_port=None, name=None, context=None):
        """
        Initialize Listener
        """
        # self.listener = fissure.comms.Listener(zmq.PAIR, name=f"{fissure.comms.Identifiers.HIPRFISR}::sensor_node")
        self.connected = False
        self.uuid = ""
        self.last_heartbeat = 0
        self.terminated = False
        self.connection_type = connection_type

        if connection_type == "IP":
            self.listener = fissure.comms.Listener(
                zmq.PAIR, name=f"{fissure.comms.Identifiers.HIPRFISR}::sensor_node"
            )
        elif connection_type == "Meshtastic":
            if serial_port is None or context is None:
                raise ValueError("Meshtastic connection requires a serial port and context")

            self.listener = fissure.comms.FissureMeshtasticNode(serial_port, name, context)


    async def close(self):
        """
        Needed to use async close functions.
        """
        if hasattr(self, "listener") and self.listener is not None:
            if self.connection_type == "IP":
                self.listener.shutdown()
            elif self.connection_type == "Meshtastic":
                await self.listener.disconnect()


    def __del__(self):
        """
        Cleanup on GC
        """
        pass
        # if hasattr(self, "listener") and self.listener is not None:
        #     if self.connection_type == "IP":
        #         self.listener.shutdown()
        #     elif self.connection_type == "Meshtastic":
        #         self.listener.disconnect()


class HiprFisr:
    """Fissure HIPRFISR Class"""

    settings: Dict
    identifier: str = fissure.comms.Identifiers.HIPRFISR
    identifierLT: str = fissure.comms.Identifiers.HIPRFISR_LT
    # logger: logging.Logger = fissure.utils.get_logger(fissure.comms.Identifiers.HIPRFISR)
    ip_address: str
    session_active: bool
    dashboard_socket: fissure.comms.Server  # PAIR
    dashboard_connected: bool
    backend_router: fissure.comms.Server  # ROUTER-DEALER
    backend_id: str
    tsi_id: bytes
    tsi_connected: bool
    pd_id: bytes
    pd_connected: bool
    sensor_nodes: List[SensorNode]
    local_plugins: List[str] = []
    heartbeats: Dict[str, Union[float, Dict[int, float]]]  # {name: time, name: time, ... sensor_nodes: {node_id: time}}
    callbacks: Dict = {}
    shutdown: bool
    alert_listeners: Dict = {}
    tak_mode: str
    tak_connected: bool


    def __init__(self, address: fissure.comms.Address):
        self.logger = fissure.utils.get_logger(fissure.comms.Identifiers.HIPRFISR)
        self.logger.info("=== INITIALIZING ===")

        # Get IP Address
        self.ip_address = fissure.utils.get_ip_address()

        # Sensor Node GPS Tracker for TAK
        self.sensor_node_tracker = SensorNodeTracker(self.logger)

        # Store Collected Wideband and Narrowband Signals in Lists
        self.wideband_list = []
        self.soi_list = []

        # SOI Blacklist
        self.soi_blacklist = []

        # Don't Process SOIs at Start
        self.process_sois = False

        # Create SOI sorting variables
        # SOI_priority = (0, 1, 2)
        # SOI_filter = ("Highest", "Highest", "Containing")
        self.soi_parameters = (None, None, "FSK")

        # Create the Variable
        self.auto_start_pd = False
        self.soi_manually_triggered = False

        # Initialize Connection/Heartbeat Variables
        self.heartbeats = {
            fissure.comms.Identifiers.HIPRFISR: None,
            fissure.comms.Identifiers.DASHBOARD: None,
            fissure.comms.Identifiers.PD: None,
            fissure.comms.Identifiers.TSI: None,
            fissure.comms.Identifiers.SENSOR_NODE: [None, None, None, None, None],
        }
        self.session_active = False
        self.dashboard_connected = False
        self.pd_id = None
        self.pd_connected = False
        self.tsi_id = None
        self.tsi_connected = False
        self.connect_loop = True
        self.child_tasks = []
        self.sockets = []

        self.nodes = {}
        self.dashboard_node_map = [None] * 5

        # Load settings from Fissure Config YAML
        self.settings = fissure.utils.get_fissure_config()

        # Update Logging Levels
        fissure.utils.update_logging_levels(
            self.logger, 
            self.settings["console_logging_level"], 
            self.settings["file_logging_level"]
        )

        # Detect Operating System
        self.os_info = fissure.utils.get_os_info()

        # Start the Database Docker Container (if not running)
        self.start_database_docker_container()
        tak_info = self.settings.get("tak")
        run_tak = tak_info.get("tak_on_startup")
        if run_tak == 'True':
            self.start_tak_docker_container()

        # Create the HIPRFISR ZMQ Nodes
        listen_addr = self.initialize_comms(address)
        self.initialize_sensor_nodes()
        self.message_counter = 0
        self.shutdown = False

        # Track Local Sensor Node UUID
        self.local_node_uuid = self.load_or_create_uuid()

        # Initialize TAK Variables
        self.tak_connected = False
        self.tak_task = None

        # Register Callbacks
        self.register_callbacks(fissure.callbacks.GenericCallbacks)
        self.register_callbacks(fissure.callbacks.HiprFisrCallbacks)
        self.register_callbacks(fissure.callbacks.HiprFisrCallbacksLT)

        self.logger.info("=== READY ===")
        self.logger.info(f"Server listening @ {listen_addr}")


    def initialize_comms(self, frontend_address: fissure.comms.Address):
        comms_info = self.settings.get("hiprfisr")
        backend_address = fissure.comms.Address(address_config=comms_info.get("backend"))

        # 1) Frontend (unchanged)
        self.dashboard_socket = fissure.comms.Server(
            address=frontend_address,
            sock_type=zmq.PAIR,
            name=f"{self.identifier}::frontend",
        )
        self.dashboard_socket.start()
        self.sockets.append(self.dashboard_socket)

        # 2) Backend (unchanged)
        self.backend_id = f"{self.identifier}-{uuid.uuid4()}"
        self.backend_router = fissure.comms.Server(
            address=backend_address,
            sock_type=zmq.ROUTER,
            name=f"{self.identifier}::backend",
        )
        self.backend_router.start()
        self.sockets.append(self.backend_router)

        # 3) Single shared Sensor Node ROUTER on fixed ports
        self.sensor_node_router = fissure.comms.Server(
            address=fissure.comms.Address(
                protocol="tcp",
                address="0.0.0.0",
                hb_channel=6100,   # <-- Correct key  #TODO: get from YAML
                msg_channel=6101,     # <-- Correct key
            ),
            sock_type=zmq.ROUTER,
            name=f"{self.identifier}::sensor_node_router",
        )
        self.sensor_node_router.start()
        self.sockets.append(self.sensor_node_router)

        # Local IPC bind for Sensor Nodes
        try:
            self.sensor_node_router.message_channel.bind("ipc:///tmp/ipc-msg")
            self.sensor_node_router.heartbeat_channel.bind("ipc:///tmp/ipc-hb")
            self.logger.info("Sensor Node IPC endpoints bound successfully")
        except Exception as e:
            self.logger.warning(f"Could not bind IPC endpoints: {e}")

        return frontend_address


    def initialize_sensor_nodes(self):
        """
        Initialize Sensor Node Listeners, Heartbeats, etc
        """
        self.sensor_nodes = []
        for n in range(0,5):
            # self.sensor_nodes.append(SensorNode())
            self.sensor_nodes.append(None)

    
    async def reset_sensor_node_listener(self, sensor_node_index=0, connection_type="IP", **kwargs):
        """
        Resets the Sensor Node slot entry for HIPRFISR.
        This version matches the PD/TSI model:
        - HIPRFISR does NOT create per-node DEALER sockets.
        - The ROUTER handles all inbound/outbound messages.
        - UUID is assigned on first message from the Sensor Node.
        """

        # -----------------------------------------------------
        # 1. Clear old entry
        # -----------------------------------------------------
        self.sensor_nodes[sensor_node_index] = None

        # -----------------------------------------------------
        # 2. Build a fresh slot entry
        # -----------------------------------------------------
        class SensorNodeEntry:
            pass

        entry = SensorNodeEntry()

        # General per-node state
        entry.UUID = None             # assigned after first ROUTER message
        entry.connected = False       # becomes True once UUID is registered
        entry.terminated = False
        entry.heartbeat_time = None   # last heartbeat timestamp

        # Store entry and return
        self.sensor_nodes[sensor_node_index] = entry
        return entry


    async def disconnectSensorNode(self, sensor_node_id: int, delete_node: bool = False):
        """
        Fully tear down a Sensor Node’s ROUTER/DEALER connection.
        Called internally by disconnectFromSensorNode().

        Handles:
        - heartbeat clearing
        - DEALER socket shutdown
        - ZMQ context cleanup
        - slot removal (optional)
        """

        node = self.sensor_nodes[sensor_node_id]

        if node is None:
            return

        # ---------------------------------------------------------
        # 1. Stop connection flags
        # ---------------------------------------------------------
        node.connected = False
        node.terminated = True

        # ---------------------------------------------------------
        # 2. Remove heartbeat record
        # ---------------------------------------------------------
        try:
            self.heartbeats[fissure.comms.Identifiers.SENSOR_NODE][sensor_node_id] = None
        except Exception:
            pass

        # ---------------------------------------------------------
        # 3. Shutdown DEALER socket
        # ---------------------------------------------------------
        listener = getattr(node, "listener", None)

        if listener:
            try:
                listener.terminated = True
            except:
                pass

            # Close ZMQ DEALER socket
            try:
                listener.shutdown()
            except:
                pass

            # Destroy its private ZMQ context if present
            if hasattr(listener, "ctx") and listener.ctx:
                try:
                    listener.ctx.term()
                except:
                    pass

            node.listener = None

        # ---------------------------------------------------------
        # 4. Delete node slot or leave empty
        # ---------------------------------------------------------
        if delete_node:
            self.sensor_nodes[sensor_node_id] = None

            # Shift all later nodes up one slot
            for i in range(sensor_node_id, len(self.sensor_nodes) - 1):
                self.sensor_nodes[i] = self.sensor_nodes[i + 1]
                self.heartbeats[fissure.comms.Identifiers.SENSOR_NODE][i] = \
                    self.heartbeats[fissure.comms.Identifiers.SENSOR_NODE][i + 1]

            # Clear last slot
            self.sensor_nodes[-1] = None
            self.heartbeats[fissure.comms.Identifiers.SENSOR_NODE][-1] = None

        # ---------------------------------------------------------
        # 5. Final dashboard notification
        # ---------------------------------------------------------
        if self.dashboard_connected:
            msg = {
                fissure.comms.MessageFields.IDENTIFIER: self.identifier,
                fissure.comms.MessageFields.MESSAGE_NAME: "componentDisconnected",
                fissure.comms.MessageFields.PARAMETERS: str(sensor_node_id),
            }
            await self.dashboard_socket.send_msg(
                fissure.comms.MessageTypes.COMMANDS, msg
            )


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


    def load_or_create_uuid(self):
        # If the UUID file exists, reuse it
        UUID_PATH = os.path.expanduser("~/.fissure/local_sensor_node_uuid.uuid")
        if os.path.exists(UUID_PATH):
            with open(UUID_PATH, "r") as f:
                return f.read().strip()

        # Otherwise create a new one
        node_uuid = str(uuid.uuid4())

        # Ensure the folder exists
        os.makedirs(os.path.dirname(UUID_PATH), exist_ok=True)

        # Save it for future runs
        with open(UUID_PATH, "w") as f:
            f.write(node_uuid)

        return node_uuid


    async def shutdown_comms(self):
        """
        Cleanly shut down all ZMQ communications:
        - Close dashboard socket (PAIR)
        - Close backend router (ROUTER)
        - Close all PD/TSI comm sockets (DEALER)
        - Close all sensor-node listener sockets
        """
        # Mark state inactive
        self.dashboard_connected = False
        self.session_active = False
        
        # Close any sockets the component registered as a group
        for s in getattr(self, "sockets", []):
            try:
                s.close_sockets()
            except Exception:
                pass

        # Stop ZAP authenticator (must run before ctx.destroy, does not work in zmq_cleanup, HIPRFISR is the last program that closes)
        try:
            from fissure.utils.common import authenticator_cleanup
            authenticator_cleanup()
        except Exception:
            pass


    async def heartbeat_loop(self):
        while not self.shutdown:
            if not self.connect_loop:
                try:
                    await self.send_heartbeat()
                    await self.recv_heartbeats()
                    await self.check_heartbeats()
                except Exception as e:
                    self.logger.error(f"[HB] {e}")

            await asyncio.sleep(HEARTBEAT_LOOP_DELAY)

        self.logger.info("[HB] Heartbeat loop exiting cleanly")


    async def begin(self):
        """
        HIPRFISR main event loop with simplified & reliable TAK handling.
        TAK logic:
        auto  -> connect at start + reconnect on loss
        manual -> connect only when triggered by user (no reconnect)
        off   -> disabled
        """

        self.logger.info("=== STARTING HIPRFISR ===")

        # Ensure clean state
        self.tak_task = None
        self.clitool = None
        self.tak_mode = self.settings["tak"]["connect_mode"].lower()

        heartbeat_task = asyncio.create_task(self.heartbeat_loop())
        self.child_tasks.append(heartbeat_task)

        # Load TAK config
        from fissure.utils.common import get_fissure_config
        fissure_config = get_fissure_config()
        tak_config = load_tak_config()

        tak_ip = fissure_config["tak"]["ip_addr"]
        tak_port = fissure_config["tak"]["port"]

        # ---------------------------------------------------------
        # AUTO MODE: wait until the server comes up, then connect
        # ---------------------------------------------------------
        if self.tak_mode == "auto":
            self.logger.info(f"TAK auto-connect: checking {tak_ip}:{tak_port}")

            while not self.shutdown:
                try:
                    r, w = await asyncio.open_connection(tak_ip, tak_port)
                    w.close()
                    await w.wait_closed()
                    self.logger.info("TAK server reachable, launching pytak...")
                    break
                except:
                    await asyncio.sleep(1)

            if not self.shutdown:
                self.tak_task = asyncio.create_task(self.run_tak_loop(tak_config, auto_reconnect=True))
                self.child_tasks.append(self.tak_task)

        elif self.tak_mode == "manual":
            self.logger.info("TAK manual mode: waiting for user to connect")
            # You can trigger manual start later with:
            # self.tak_task = asyncio.create_task(self.run_tak_loop(tak_config, auto_reconnect=False))

        else:
            self.logger.info("TAK is disabled in config")

        # ---------------------------------------------------------
        # Main HIPRFISR event loop
        # ---------------------------------------------------------
        loop = asyncio.get_event_loop()
        self.silence_asyncio_ssl_errors(loop)
        while not self.shutdown:
            if not self.connect_loop:
                if self.dashboard_connected:
                    await self.read_dashboard_messages()
                if self.pd_connected or self.tsi_connected:
                    await self.read_backend_messages()
                try:
                    await self.read_sensor_node_messages()
                except Exception as e:
                    self.logger.error(f"read_sensor_node_messages crashed: {e}")
                await asyncio.sleep(EVENT_LOOP_DELAY)
            else:
                await self.connect_components()

        self.logger.debug("Shutdown reached in HIPRFISR event loop")

        # ---------------------------------------------------------
        # Cleanup
        # ---------------------------------------------------------
        # Stop TAK Client
        await self.stop_tak_client()

        # Stop Heartbeat Task
        heartbeat_task.cancel()  # Double cancel is needed to brute force close, faster
        await asyncio.sleep(0)
        heartbeat_task.cancel()  # Needs to happen after sleep
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass

        # Close Running Tasks
        for task in self.child_tasks:
            if isinstance(task, asyncio.Task):
                task.cancel()
        await asyncio.gather(
            *[t for t in self.child_tasks if isinstance(t, asyncio.Task)],
            return_exceptions=True
        )

        # Extra cleanup for any pytak worker tasks still alive
        for t in asyncio.all_tasks():
            c = str(t.get_coro())
            if "pytak" in c or "RXWorker" in c or "TXWorker" in c or "TakReceiver" in c:
                t.cancel()

        # Shut Down Comms
        await self.shutdown_comms()
        await asyncio.sleep(0)

        self.logger.info("=== HIPRFISR SHUTDOWN COMPLETE ===")

        return


    async def run_tak_loop(self, tak_config, auto_reconnect=True):
        """
        Run pytak. Restart only if auto_reconnect=True.
        Never double-spawns.
        """

        while not self.shutdown:
            try:
                self.clitool = pytak.CLITool(tak_config)
                await self.clitool.setup()

                # Attach receiver once per loop
                self.clitool.add_tasks({
                    TakReceiver(self.clitool.rx_queue, tak_config, self, self.logger)
                })

                # Collect pytak-created asyncio tasks
                for t in getattr(self.clitool, "tasks", []):
                    if isinstance(t, asyncio.Task):
                        self.child_tasks.append(t)

                self.logger.info("Starting pytak client...")
                self.tak_connected = True
                await self.clitool.run()
                self.logger.warning("TAK connection ended")
                self.tak_connected = False

                if self.shutdown:
                    break

                if not auto_reconnect:
                    self.logger.info("TAK manual mode: not reconnecting")
                    break

                self.logger.warning("TAK connection lost. Reconnecting in 5 seconds...")
                await asyncio.sleep(5)

            except Exception as e:
                if self.shutdown:
                    break
                self.logger.error(f"pytak crashed: {e}")

            # auto reconnect delay
            if auto_reconnect and not self.shutdown:
                await asyncio.sleep(5)

        self.logger.info("Exiting Tak loop")


    async def stop_tak_client(self):
        """Cleanly stop pytak workers, receiver, and writer."""
        # No client running
        if not self.clitool:
            return

        # ----------------------------------------------
        # 1. Signal TX queue to stop
        # ----------------------------------------------
        try:
            self.clitool.tx_queue.put_nowait(None)
        except Exception:
            pass

        # ----------------------------------------------
        # 2. Signal RX queue to stop
        # ----------------------------------------------
        try:
            self.clitool.rx_queue.put_nowait(None)
        except Exception:
            pass

        # ----------------------------------------------
        # 3. Cancel pytak worker tasks explicitly
        # ----------------------------------------------
        worker_tasks = []

        for task in asyncio.all_tasks():
            coro = str(task.get_coro())
            if "RXWorker" in coro or "TXWorker" in coro or "Worker.run" in coro:
                task.cancel()
                worker_tasks.append(task)

        # Give them a moment to exit
        await asyncio.gather(*worker_tasks, return_exceptions=True)

        # ----------------------------------------------
        # 4. Cancel TakReceiver task
        # ----------------------------------------------
        if self.tak_task:
            self.tak_task.cancel()
            try:
                await self.tak_task
            except:
                pass

        # ----------------------------------------------
        # 5. Close TLS writer if it exists
        # ----------------------------------------------
        writer = getattr(self.clitool, "writer", None)
        if writer:
            try:
                writer.close()
                await writer.wait_closed()
            except:
                pass

        # Cleanup
        self.clitool = None
        self.tak_task = None


    async def start_tak_manual(self):
        if not self.tak_task:
            tak_config = load_tak_config()
            self.tak_task = asyncio.create_task(self.run_tak_loop(tak_config, auto_reconnect=False))
            self.logger.info("Manual TAK connection started")


    def silence_asyncio_ssl_errors(self, loop):
        """Ignore spurious SSL errors on shutdown (Python 3.8 TLS bug)."""
        orig_handler = loop.get_exception_handler()

        def handler(loop, context):
            msg = context.get("message", "")
            exc = context.get("exception")
            if (
                "Fatal error on SSL transport" in msg or
                (exc and isinstance(exc, OSError) and exc.errno == 9)
            ):
                # ignore bad fd & ssl closure errors
                return
            if orig_handler is not None:
                orig_handler(loop, context)
            else:
                loop.default_exception_handler(context)

        loop.set_exception_handler(handler)


    async def connect_components(self):
        self.logger.debug("Entering connect loop")

        while self.connect_loop is True:

            # Heartbeats (Dashboard + PD/TSI)
            await self.recv_heartbeats()
            await self.send_heartbeat()
            await self.check_heartbeats()

            # Dashboard first — user may press "Exit Connect Loop"
            await self.read_dashboard_messages()

            # Backend (PD/TSI)
            await self.read_backend_messages()

            # Dashboard expected
            if self.settings.get("auto_connect_hiprfisr", True):
                if not self.dashboard_connected:
                    self.logger.warning("Dashboard lost during connect loop")
                    break

                # All components connected?
                if self.dashboard_connected and self.pd_connected and self.tsi_connected:
                    msg = {
                        fissure.comms.MessageFields.IDENTIFIER: self.identifier,
                        fissure.comms.MessageFields.MESSAGE_NAME: "Connected",
                        fissure.comms.MessageFields.PARAMETERS: [
                            fissure.comms.Identifiers.PD,
                            fissure.comms.Identifiers.TSI,
                        ],
                    }
                    await self.dashboard_socket.send_msg(
                        fissure.comms.MessageTypes.STATUS, msg
                    )
                    self.connect_loop = False

            # Headless/remote Dashboard
            else:
                # All components connected?
                if self.pd_connected and self.tsi_connected:
                    self.connect_loop = False

        self.logger.debug("Exiting connect loop")


    async def read_dashboard_messages(self):
        received_message = ""

        while received_message is not None and not self.shutdown:
            
            received_message = await self.dashboard_socket.recv_msg()
            if received_message is None:
                break

            self.dashboard_connected = True
            msg_type = received_message.get(fissure.comms.MessageFields.TYPE)

            # Heartbeats arriving on the message channel are ignored
            if msg_type == fissure.comms.MessageTypes.HEARTBEATS:
                self.logger.warning("received heartbeat on message channel [from Dashboard]")
                continue

            # COMMANDS
            if msg_type == fissure.comms.MessageTypes.COMMANDS:
                await self.dashboard_socket.run_callback(self, received_message)
                continue

            # STATUS
            if msg_type == fissure.comms.MessageTypes.STATUS:
                msg_name = received_message.get(fissure.comms.MessageFields.MESSAGE_NAME)

                if msg_name == "Connected":
                    response = {
                        fissure.comms.MessageFields.IDENTIFIER: self.identifier,
                        fissure.comms.MessageFields.MESSAGE_NAME: "OK",
                    }
                    await self.dashboard_socket.send_msg(
                        fissure.comms.MessageTypes.STATUS,
                        response
                    )
                    self.session_active = True
                    continue

                elif msg_name == "Exit Connect Loop":
                    self.connect_loop = False
                    continue

                # Unknown STATUS message: ignore
                continue


    async def read_backend_messages(self):
        received_message = ""

        while received_message is not None:
            received_message = await self.backend_router.recv_msg()

            if received_message is None:
                break

            sender_id = received_message.get(fissure.comms.MessageFields.SENDER_ID)
            component = received_message.get(fissure.comms.MessageFields.IDENTIFIER)
            msg_type = received_message.get(fissure.comms.MessageFields.TYPE)

            # ------------------------------------------------------------------
            # Register backend DEALER identities (PD, TSI)
            # ------------------------------------------------------------------
            if component == fissure.comms.Identifiers.PD:
                self.pd_id = sender_id

            elif component == fissure.comms.Identifiers.TSI:
                self.tsi_id = sender_id

            # ------------------------------------------------------------------
            # Heartbeats should NOT arrive on the message channel
            # ------------------------------------------------------------------
            if msg_type == fissure.comms.MessageTypes.HEARTBEATS:
                self.logger.warning(f"received backend heartbeat on message channel from {component}")
                continue

            # ------------------------------------------------------------------
            # COMMANDS
            # ------------------------------------------------------------------
            if msg_type == fissure.comms.MessageTypes.COMMANDS:
                await self.backend_router.run_callback(self, received_message)
                continue

            # ------------------------------------------------------------------
            # STATUS messages
            # ------------------------------------------------------------------
            if msg_type == fissure.comms.MessageTypes.STATUS:
                msg_name = received_message.get(fissure.comms.MessageFields.MESSAGE_NAME)

                if msg_name == "Connected":
                    response = {
                        fissure.comms.MessageFields.IDENTIFIER: self.identifier,
                        fissure.comms.MessageFields.MESSAGE_NAME: "OK",
                    }
                    await self.backend_router.send_msg(
                        fissure.comms.MessageTypes.STATUS,
                        response,
                        target_ids=[sender_id]
                    )
                continue


    async def read_sensor_node_messages(self):
        """
        Reads inbound messages from Sensor Nodes (ROUTER socket).

        Responsibilities:
        - Identify the sender identity (ROUTER identity)
        - Parse JSON message
        - Register/update node entries for sensorNodeHello
        - Only update runtime fields for non-HELLO messages
        - Dispatch callbacks for COMMAND messages
        - Log STATUS messages
        """

        while not self.shutdown:

            # # Don't block if no data is ready
            # if not self.sensor_node_router.poll(0):
            #     await asyncio.sleep(0)
            #     continue

            received = await self.sensor_node_router.recv_msg()
            if received is None:
                return  # socket was shut down

            # print(received)
            sender_id = received.get(fissure.comms.MessageFields.SENDER_ID)
            uuid = received.get(fissure.comms.MessageFields.IDENTIFIER)
            msg_type  = received.get(fissure.comms.MessageFields.TYPE)
            msg_name  = received.get(fissure.comms.MessageFields.MESSAGE_NAME)
            params    = received.get(fissure.comms.MessageFields.PARAMETERS, {})

            if not uuid:
                self.logger.warning(
                    f"[ROUTER] Message '{msg_name}' from {sender_id} missing UUID; ignoring."
                )
                continue

            node = self.nodes.get(uuid)

            # ===============================================================
            # Update identity, last_seen from UUID
            # ===============================================================
            if node is None:
                self.logger.warning(
                    f"Message '{msg_name}' from unknown uuid={uuid}"
                )
                continue

            node["identity"]  = sender_id  # sensor-node-sensor node 9221cf85-87215684-ac17-43e4-9aad-ad33c9f14815
            node["connected"] = True
            node["last_seen"] = time.time()

            # ===============================================================
            # DISPATCH MESSAGE TYPES
            # ===============================================================

            # COMMANDS ------------------------------------------------------
            if msg_type == fissure.comms.MessageTypes.COMMANDS:
                try:
                    params = received.get("Parameters", {})

                    # AUTO-INJECT UUID so all callbacks can access it
                    if isinstance(params, dict):
                        params.setdefault("uuid", uuid)

                        await self.sensor_node_router.run_callback(self, received)
                except Exception as e:
                    self.logger.error(
                        f"Callback failed for node uuid={uuid}, msg={msg_name}: {e}"
                    )
                continue

            # STATUS --------------------------------------------------------
            if msg_type == fissure.comms.MessageTypes.STATUS:
                self.logger.info(f"STATUS from node {uuid}: {msg_name}")  # TODO: remove status message type?
                continue

            # UNKNOWN -------------------------------------------------------
            self.logger.warning(
                f"Unknown msg_type={msg_type} from node uuid={uuid}"
            )
            continue


    async def send_heartbeat(self):
        last_heartbeat = self.heartbeats[self.identifier]
        now = time.time()

        if (last_heartbeat is None) or (
            now - last_heartbeat >= float(self.settings.get("heartbeat_interval"))
        ):
            node_times = []
            node_intervals = []

            for idx in range(5):
                hb_record = self.heartbeats[fissure.comms.Identifiers.SENSOR_NODE][idx]

                if hb_record is None:
                    node_times.append(None)
                    node_intervals.append(None)
                else:
                    timestamp = hb_record.get("time")
                    interval = hb_record.get("interval")
                    node_times.append(timestamp)
                    node_intervals.append(interval)

            heartbeat = {
                fissure.comms.MessageFields.IDENTIFIER: self.identifier,
                fissure.comms.MessageFields.MESSAGE_NAME: fissure.comms.MessageFields.HEARTBEAT,
                fissure.comms.MessageFields.TIME: now,
                fissure.comms.MessageFields.PARAMETERS: {
                    fissure.comms.Identifiers.PD: self.heartbeats.get(fissure.comms.Identifiers.PD),
                    fissure.comms.Identifiers.TSI: self.heartbeats.get(fissure.comms.Identifiers.TSI),
                    fissure.comms.Identifiers.SENSOR_NODE: {
                        fissure.comms.MessageFields.TIME: node_times,
                        fissure.comms.MessageFields.INTERVAL: node_intervals,
                    },
                },
            }

            # -------------------------------------------------------------
            # DASHBOARD HEARTBEAT
            # -------------------------------------------------------------
            if self.dashboard_connected:
                await self.dashboard_socket.send_heartbeat(heartbeat)
            else:
                if self.settings.get("auto_connect_hiprfisr", True):
                    self.logger.error("[HIPRFISR][HB] Dashboard not connected.")
                else:
                    pass

            # -------------------------------------------------------------
            # PD / TSI HEARTBEAT
            # -------------------------------------------------------------
            if self.pd_connected or self.tsi_connected:
                heartbeat_with_ip = dict(heartbeat)
                heartbeat_with_ip[fissure.comms.MessageFields.IP] = "localhost"

                await self.backend_router.send_heartbeat(
                    heartbeat_with_ip,
                    target_ids=[self.pd_id, self.tsi_id],
                )
            else:
                self.logger.error("[HIPRFISR][HB] PD/TSI not connected.")

            # -------------------------------------------------------------
            # SENSOR NODE HEARTBEAT (Don't heartbeat to nodes)
            # -------------------------------------------------------------
            # target_ids = []

            # for uuid in self.dashboard_node_map:

            #     # Skip unassigned slots
            #     if uuid is None:
            #         continue

            #     node_entry = self.nodes.get(uuid)
            #     if not node_entry:
            #         continue

            #     # Skip nodes not marked connected
            #     if not node_entry.get("connected", False):
            #         continue

            #     identity = node_entry.get("identity")
            #     if identity:
            #         target_ids.append(identity)

            # if target_ids:
            #     try:
            #         await self.sensor_node_router.send_heartbeat(
            #             heartbeat,
            #             target_ids=target_ids
            #         )
            #     except Exception as e:
            #         self.logger.error(f"[HB] Failed sending SN heartbeat: {e}")
            # else:
            #     pass
            #     # self.logger.debug("[HB] No connected dashboard nodes — skipping SN heartbeat.")

            # -------------------------------------------------------------
            # SELF/HIPRFISR
            # -------------------------------------------------------------
            # Update HIPRFISR's own heartbeat timestamp
            self.heartbeats[self.identifier] = now


    async def recv_heartbeats(self):
        """
        Receive heartbeats from:
        • Dashboard (PAIR)
        • PD / TSI (backend_router ROUTER)
        • Sensor Nodes (sensor_node_router ROUTER)
        """
        # -------------------------
        # Dashboard heartbeat (PAIR)
        # -------------------------
        dashboard_hb = await self.dashboard_socket.recv_heartbeat()
        if dashboard_hb:
            t = float(dashboard_hb[fissure.comms.MessageFields.TIME])
            self.heartbeats[fissure.comms.Identifiers.DASHBOARD] = t
            # self.logger.debug(f"received Dashboard heartbeat ({fissure.utils.get_timestamp(t)})")

        # -----------------------------
        # Backend PD / TSI (ROUTER)
        # -----------------------------
        backend_hbs = await self.backend_router.recv_heartbeats()
        if backend_hbs:
            for hb in backend_hbs:
                sender_id = hb.get(fissure.comms.MessageFields.SENDER_ID)
                ident     = hb.get(fissure.comms.MessageFields.IDENTIFIER)
                t         = float(hb[fissure.comms.MessageFields.TIME])

                if ident == fissure.comms.Identifiers.PD and self.pd_id is None:
                    self.pd_id = sender_id

                if ident == fissure.comms.Identifiers.TSI and self.tsi_id is None:
                    self.tsi_id = sender_id

                self.heartbeats[ident] = t
                # self.logger.debug(
                #     f"received backend heartbeat from {ident} "
                #     f"({fissure.utils.get_timestamp(t)})"
                # )

        # ---------------------------------------------------
        # Sensor Node heartbeat handling (NEW CLEAN FUNCTION)
        # ---------------------------------------------------
        await self.recv_sensor_node_heartbeats()


    async def recv_sensor_node_heartbeats(self):
        """
        Reads heartbeats from Sensor Nodes via the heartbeat channel.
        Updates metadata in self.nodes just like a hello would,
        but WITHOUT notifying the dashboard or assigning slots.
        """
        hbs = await self.sensor_node_router.recv_heartbeats()
        if not hbs:
            return

        print("\n=== NODE HEARTBEATS RECEIVED ===")
        # print("Raw heartbeats list:", hbs)
        # print(self.nodes)

        node_list = self.heartbeats[fissure.comms.Identifiers.SENSOR_NODE]

        for hb in hbs:
            sn_time = float(hb.get(fissure.comms.MessageFields.TIME))
            sn_int  = hb.get(fissure.comms.MessageFields.INTERVAL, 5)
            sn_uuid  = hb.get(fissure.comms.MessageFields.IDENTIFIER)  # ← UUID is the real node key

            params = hb.get(fissure.comms.MessageFields.PARAMETERS, {})
            sn_ip        = hb.get(fissure.comms.MessageFields.IP)        # ← IP now at top level
            sn_nickname  = params.get("nickname", "-")
            sn_nettype   = params.get("network_type", "IP")
            sn_id_zmq    = hb.get(fissure.comms.MessageFields.SENDER_ID) # sensor-node-sensor node f8be9c34-39dbbc90-32e1-43ea-b0ce-f4729ab2e3a5

            if not sn_uuid:
                self.logger.error("Heartbeat missing UUID — ignoring.")
                continue

            # ---------------------------------------------------------
            # Use UUID as the ONLY dictionary key
            # ---------------------------------------------------------
            # LOOKUP BY UUID (correct)
            node = self.nodes.get(sn_uuid)
            callsign_prefix = self.settings.get("callsign_prefix", "FTN")

            if node is None:
                # CREATE NEW NODE ENTRY (keyed by UUID — correct)
                callsign_prefix = self.settings.get("callsign_prefix", "FTN")
                node = {
                    "uuid": sn_uuid,
                    "identity": sn_id_zmq,          # router identity
                    "callsign": callsign_prefix + "-" + sn_nickname,
                    "ip": sn_ip,
                    "network_type": sn_nettype,
                    "nickname": sn_nickname,
                    "settings": {},             # settings returned only when requested
                    "last_seen": sn_time,
                    "interval": sn_int,
                    "connected": True,
                }
                self.nodes[sn_uuid] = node
            else:
                # UPDATE EXISTING NODE           
                node["identity"]     = sn_id_zmq
                node["callsign"]     = callsign_prefix + "-" + sn_nickname
                node["ip"]           = sn_ip
                node["network_type"] = sn_nettype
                node["nickname"]     = sn_nickname
                node["last_seen"]    = sn_time
                node["interval"]     = sn_int
                node["connected"]    = True

            # ---------------------------------------------------------
            # AUTO-SELECT LOCAL SENSOR NODE (UUID match)
            # ---------------------------------------------------------
            if sn_uuid == self.local_node_uuid and sn_uuid not in self.dashboard_node_map:

                self.logger.info(f"[LOCAL NODE] Auto-selecting UUID {sn_uuid}")

                # Find free dashboard slot (0-4)
                dashboard_node_index = None
                for idx, entry in enumerate(self.dashboard_node_map):
                    if entry is None:
                        dashboard_node_index = idx
                        break

                if dashboard_node_index is None:
                    self.logger.warning("[LOCAL NODE] No free dashboard slots — skipping auto-select.")
                else:
                    # Assign UUID into that slot
                    self.dashboard_node_map[dashboard_node_index] = sn_uuid

                    # Perform the callback that configures node settings etc.
                    cb = self.callbacks.get("nodeSelectIP")
                    if cb:
                        await cb(self, dashboard_node_index, sn_uuid)
                    else:
                        self.logger.error("nodeSelectIP callback not registered!")

            # ---------------------------------------------------------
            # Maintain heartbeat slot table (still uses identity)
            # ---------------------------------------------------------
            slot_index = None

            # Existing slot
            for idx, entry in enumerate(node_list):
                if entry and entry.get("uuid") == sn_uuid:
                    slot_index = idx
                    break

            # Free slot
            if slot_index is None:
                for idx, entry in enumerate(node_list):
                    if entry is None:
                        slot_index = idx
                        break

            if slot_index is None:
                self.logger.error(f"No free heartbeat slots for sensor node identity={sn_id_zmq}")
                continue

            node_list[slot_index] = {
                "uuid": sn_uuid,
                "identity": sn_id_zmq,
                "time": sn_time,
                "interval": sn_int,
            }

            # ---------------------------------------------------------
            # Update sensor_nodes slot state (unchanged behavior)
            # ---------------------------------------------------------
            if slot_index < len(self.sensor_nodes):
                sn_entry = self.sensor_nodes[slot_index]
                if sn_entry:
                    sn_entry.connected = True
                    sn_entry.terminated = False
                    sn_entry.heartbeat_time = sn_time


    async def check_heartbeats(self):
        """
        Check heartbeat timestamps for Dashboard, PD, TSI, and Sensor Nodes,
        and update connection state accordingly.
        """
        current_time = time.time()

        # HIPRFISR global heartbeat interval (used for Dashboard, PD, TSI)
        global_interval = float(self.settings.get("heartbeat_interval"))
        failure_multiple = float(self.settings.get("failure_multiple"))

        # -----------------------------------------------------------------
        # Dashboard check
        # -----------------------------------------------------------------
        cutoff_dashboard = current_time - (global_interval * failure_multiple)
        last_dashboard = self.heartbeats.get(fissure.comms.Identifiers.DASHBOARD)

        if last_dashboard is not None:
            if self.dashboard_connected and (last_dashboard < cutoff_dashboard):
                self.dashboard_connected = False
                self.logger.warning("lost dashboard connection")

            elif (not self.dashboard_connected) and (last_dashboard > cutoff_dashboard):
                msg = {
                    fissure.comms.MessageFields.IDENTIFIER: self.identifier,
                    fissure.comms.MessageFields.MESSAGE_NAME: "componentConnected",
                    fissure.comms.MessageFields.PARAMETERS: fissure.comms.Identifiers.DASHBOARD,
                }
                await self.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)
                self.dashboard_connected = True

        # -----------------------------------------------------------------
        # PD check
        # -----------------------------------------------------------------
        cutoff_pd = current_time - (global_interval * failure_multiple)
        last_pd = self.heartbeats.get(fissure.comms.Identifiers.PD)

        if last_pd is not None:
            if self.pd_connected and (last_pd < cutoff_pd):
                msg = {
                    fissure.comms.MessageFields.IDENTIFIER: self.identifier,
                    fissure.comms.MessageFields.MESSAGE_NAME: "componentDisconnected",
                    fissure.comms.MessageFields.PARAMETERS: fissure.comms.Identifiers.PD,
                }
                if self.dashboard_connected:
                    await self.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)
                self.pd_connected = False

            elif (not self.pd_connected) and (last_pd > cutoff_pd):
                msg = {
                    fissure.comms.MessageFields.IDENTIFIER: self.identifier,
                    fissure.comms.MessageFields.MESSAGE_NAME: "componentConnected",
                    fissure.comms.MessageFields.PARAMETERS: fissure.comms.Identifiers.PD,
                }
                if self.dashboard_connected:
                    await self.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)
                self.pd_connected = True

        # -----------------------------------------------------------------
        # TSI check
        # -----------------------------------------------------------------
        cutoff_tsi = current_time - (global_interval * failure_multiple)
        last_tsi = self.heartbeats.get(fissure.comms.Identifiers.TSI)

        if last_tsi is not None:
            if self.tsi_connected and (last_tsi < cutoff_tsi):
                msg = {
                    fissure.comms.MessageFields.IDENTIFIER: self.identifier,
                    fissure.comms.MessageFields.MESSAGE_NAME: "componentDisconnected",
                    fissure.comms.MessageFields.PARAMETERS: fissure.comms.Identifiers.TSI,
                }
                if self.dashboard_connected:
                    await self.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)
                self.tsi_connected = False

            elif (not self.tsi_connected) and (last_tsi > cutoff_tsi):
                msg = {
                    fissure.comms.MessageFields.IDENTIFIER: self.identifier,
                    fissure.comms.MessageFields.MESSAGE_NAME: "componentConnected",
                    fissure.comms.MessageFields.PARAMETERS: fissure.comms.Identifiers.TSI,
                }
                if self.dashboard_connected:
                    await self.dashboard_socket.send_msg(fissure.comms.MessageTypes.COMMANDS, msg)
                self.tsi_connected = True

        # -----------------------------------------------------------------
        # Sensor Node checks
        # -----------------------------------------------------------------
        for uuid, node in list(self.nodes.items()):
            last_seen = node["last_seen"]
            interval = node.get("interval", global_interval)
            cutoff = current_time - (interval * failure_multiple)

            # Node has gone silent → disconnected
            if node["connected"] and last_seen < cutoff:
                node["connected"] = False

                # Find which dashboard slot (0–4) this UUID is assigned to
                try:
                    dashboard_index = self.dashboard_node_map.index(uuid)
                except ValueError:
                    dashboard_index = None

                # If the dashboard is showing this node, notify it
                if dashboard_index is not None and self.dashboard_connected:
                    msg = {
                        fissure.comms.MessageFields.IDENTIFIER: self.identifier,
                        fissure.comms.MessageFields.MESSAGE_NAME: "componentDisconnected",
                        fissure.comms.MessageFields.PARAMETERS: str(dashboard_index),
                    }
                    await self.dashboard_socket.send_msg(
                        fissure.comms.MessageTypes.COMMANDS,
                        msg
                    )


    def resolve_sensor_node_identity(self, dashboard_index: int):
        """
        Given a dashboard node index (0-4), return (uuid, identity) for the
        corresponding sensor node, or (None, None) if not available.
        """
        # 1) Validate dashboard_index → UUID
        uuid = self.dashboard_node_map[int(dashboard_index)]
        if uuid is None:
            self.logger.error(f"[resolve] No node assigned to dashboard index {dashboard_index}")
            return None, None

        # 2) Validate UUID exists in self.nodes
        node_entry = self.nodes.get(uuid)
        if not node_entry:
            self.logger.error(f"[resolve] No node entry found for uuid {uuid}")
            return None, None

        # 3) Extract ROUTER identity
        identity = node_entry.get("identity")
        if not identity:
            self.logger.error(f"[resolve] UUID {uuid} has no ROUTER identity")
            return None, None

        return uuid, identity


    def resolve_dashboard_target(self, uuid: str):
        """
        Given a node UUID, find which dashboard node index (0-4)
        this node is mapped to.

        Returns (node_entry, dashboard_index) or (node_entry, None)
        """
        node = self.nodes.get(uuid)
        if not node:
            self.logger.warning(f"[resolve] No node entry for uuid={uuid}")
            return None, None

        # Reverse lookup: find which dashboard index currently maps to this UUID
        for idx, mapped_uuid in enumerate(self.dashboard_node_map):
            if mapped_uuid == uuid:
                return node, idx

        # UUID exists in nodes[] but not assigned to any dashboard slot
        self.logger.warning(f"[resolve] UUID {uuid} is not assigned to any dashboard index")
        return node, None


    async def updateLoggingLevels(self, new_console_level="", new_file_level=""):
        """Update the logging levels on the HIPRFISR and forward to all components."""
        # Update New Levels for the HIPRFISR
        fissure.utils.update_logging_levels(self.logger, new_console_level, new_file_level)

        # For Testing
        # self.logger.info("INFO")
        # self.logger.debug("DEBUG")
        # self.logger.warning("WARNING")
        # self.logger.error("ERROR")
        # print(f"Logger level: {self.logger.level}")
        # for handler in self.logger.handlers:
        #     print(f"Handler {type(handler).__name__} level: {handler.level}")

        # Update Other Components
        PARAMETERS = {
            "new_console_level": new_console_level, 
            "new_file_level": new_file_level
        }
        msg = {
            fissure.comms.MessageFields.IDENTIFIER: self.identifier,
            fissure.comms.MessageFields.MESSAGE_NAME: "updateLoggingLevels",
            fissure.comms.MessageFields.PARAMETERS: PARAMETERS,
        }

        if (self.pd_connected is True) and (self.tsi_connected is True):
            await self.backend_router.send_msg(
                fissure.comms.MessageTypes.COMMANDS, msg, target_ids=[self.pd_id, self.tsi_id]
            )

        # -----------------------------------------
        # Send to all connected IP Sensor Nodes
        # -----------------------------------------
        for dashboard_index, mapped_uuid in enumerate(self.dashboard_node_map):

            # Skip empty dashboard slots
            if mapped_uuid is None:
                continue

            # Resolve uuid -> identity via dashboard index
            uuid, identity = self.resolve_sensor_node_identity(dashboard_index)
            if identity is None or uuid is None:
                continue

            # Look up node entry to check network_type
            node_entry = self.nodes.get(uuid)
            if not node_entry:
                continue

            # Only send to IP-based nodes (skip Meshtastic, etc.)
            net_type = node_entry.get("network_type", "IP")
            if net_type != "IP":
                continue

            # Send via ROUTER
            try:
                await self.sensor_node_router.send_msg(
                    fissure.comms.MessageTypes.COMMANDS,
                    msg,
                    target_ids=[identity],
                )
            except Exception as e:
                self.logger.error(
                    f"Failed sending logging-level update to node {uuid} ({net_type}): {e}"
                )


    def start_database_docker_container(self):
        """
        Starts the database Docker container if it is not already running.
        """
        def run_docker_command(command, use_sudo=False, cwd=None):
            """ Helper to run Docker commands with optional sudo and working directory. """
            if use_sudo:
                command.insert(0, "sudo")
            return subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=cwd)

        try:
            # Define the Docker image to check
            image_name = "postgres:13"

            # Check if the Docker container is running
            result = run_docker_command(['docker', 'ps', '--filter', f'ancestor={image_name}', '--format', '{{.Image}}'])

            # If the command failed due to permissions, retry with sudo
            if result.returncode != 0 and "permission denied" in result.stderr.lower():
                self.logger.info("Docker requires sudo. Retrying with sudo.")
                result = run_docker_command(['docker', 'ps', '--filter', f'ancestor={image_name}', '--format', '{{.Image}}'], use_sudo=True)

            # Check if the container is already running
            if image_name in result.stdout.strip():
                self.logger.info("Database Docker container is already running.")
                return

            # Container not running, start it
            self.logger.info("Database Docker container not found. Starting it...")

            # Define the start command
            start_command = ["docker", "compose", "up", "-d"]
            docker_compose_directory = fissure.utils.FISSURE_ROOT

            # Attempt to start without sudo
            start_result = run_docker_command(start_command, cwd=docker_compose_directory)
            if start_result.returncode != 0 and "permission denied" in start_result.stderr.lower():
                self.logger.info("Starting Docker with sudo.")
                start_result = run_docker_command(start_command, use_sudo=True, cwd=docker_compose_directory)

            if start_result.returncode == 0:
                self.logger.info("Docker container started successfully.")
            else:
                self.logger.error(f"Failed to start Docker container: {start_result.stderr.strip()}")

        except Exception as e:
            self.logger.error(f"Error: {e}")


    def start_tak_docker_container(self):
        """
        Starts the TAK Docker containers (DB and server) if not already running.
        Handles versioned container names dynamically.
        """
        def run_docker_command(command, use_sudo=False, cwd=None):
            """ Helper to run Docker commands with optional sudo and working directory. """
            if use_sudo:
                command.insert(0, "sudo")
            return subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=cwd)


        def get_matching_containers(name_prefix):
            """ Returns a list of container names matching a given prefix. """
            cmd = ["docker", "ps", "-a", "--filter", f"name={name_prefix}", "--format", "{{.Names}}"]
            result = run_docker_command(cmd)
            if result.returncode != 0 and "permission denied" in result.stderr.lower():
                result = run_docker_command(cmd, use_sudo=True)
            if result.returncode == 0:
                return result.stdout.strip().splitlines()
            else:
                self.logger.error(f"Failed to list containers with prefix '{name_prefix}': {result.stderr.strip()}")
                return []


        def start_container(name):
            """ Starts a container by name, handling sudo if needed. """
            start_cmd = ["docker", "start", name]
            result = run_docker_command(start_cmd)
            if result.returncode != 0 and "permission denied" in result.stderr.lower():
                result = run_docker_command(start_cmd, use_sudo=True)
            return result

        try:
            db_containers = get_matching_containers("takserver-db-")
            server_containers = get_matching_containers("takserver-")

            # Remove DB containers from the server list (avoid duplication)
            server_containers = [c for c in server_containers if not c.startswith("takserver-db-")]

            if not db_containers and not server_containers:
                self.logger.warning("No TAK Docker containers found.")
                return

            for container in db_containers:
                result = start_container(container)
                if result.returncode == 0:
                    self.logger.info(f"Started TAK DB container: {container}")
                else:
                    self.logger.error(f"Failed to start TAK DB container {container}: {result.stderr.strip()}")

            for container in server_containers:
                result = start_container(container)
                if result.returncode == 0:
                    self.logger.info(f"Started TAK Server container: {container}")
                else:
                    self.logger.error(f"Failed to start TAK Server container {container}: {result.stderr.strip()}")

        except Exception as e:
            self.logger.error(f"Exception while starting TAK containers: {e}")


    def openPluginEditor(self, plugin_name: str):
        self.plugin_editor = PluginEditor(plugin_name)


    # def closePluginEditor(self):
    #     self.plugin_editor = None


    # def pluginEditorGetProtocols(self):
    #     return self.plugin_editor.get_protocols()


    def pluginAddProtocolHiprfisr(self, protocol_name: str):
        # add protocol (or edit if it already exists)
        self.plugin_editor.add_protocol(protocol_name)
        return self.plugin_editor.get_protocol_parameters(protocol_name)
    

    def sensorNodeCleanup(self, sensor: SensorNode):
        """
        Closes SensorNode object on exit.
        """
        asyncio.run(sensor.close())


    async def send_cot(self, uid, callsign, lat, lon, alt, time, remarks, type:str="a-f-G-U-H"):
        """
        Sends CoT message to TAK server. Used for alerts but not for the sensor node GPS beacon.
        """
        # Get TAK server settings
        settings: dict = fissure.utils.get_fissure_config()
        s_addr = settings["tak"]["ip_addr"]
        s_port = settings["tak"]["port"]
        tak_cert = settings["tak"]["cert"]
        tak_key = settings["tak"]["key"]

        self.logger.debug(f"Sending CoT to TAK server with type {type}")

        cot_msg = f"""<?xml version="1.0" encoding="UTF-8"?>
        <event version="2.0" type="{type}" uid="{uid}" how="m-g" time="{time}" start="{time}" stale="2029-08-09T18:18:06.521956Z">
            <detail>
                <contact callsign="{callsign}"/>
                <remarks>"{remarks}"</remarks>
            </detail>
            <point lat="{lat}" lon="{lon}" ce="0" le="0" hae="0"/>
        </event>"""

        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        context.load_cert_chain(certfile=tak_cert, keyfile=tak_key)

        # Base directory where TAK server versions are installed
        tak_base_path = os.path.expanduser("~/Installed_by_FISSURE")

        # List directories, ignoring .zip files
        try:
            tak_dirs = [
                d for d in os.listdir(tak_base_path)
                if d.startswith("takserver-docker-") and os.path.isdir(os.path.join(tak_base_path, d))
            ]
            tak_dirs.sort(reverse=True)  # Sort to get the latest version first
            takserver_path = os.path.join(tak_base_path, tak_dirs[0]) if tak_dirs else None
        except FileNotFoundError:
            takserver_path = None  # Handle case where directory does not exist

        # Construct the full path to root-ca.pem
        if takserver_path:
            root_ca_path = os.path.join(takserver_path, "tak/certs/files/root-ca.pem")
        else:
            root_ca_path = None  # No TAK server found

        #print("Using TAK Root CA:", root_ca_path)
        
        context.load_verify_locations(root_ca_path)
        context.check_hostname = False
        
        try:
            reader, writer = await asyncio.open_connection(s_addr, s_port, ssl=context)
            writer.write(cot_msg.encode('utf-8'))
            await writer.drain()
            self.logger.info("Message sent to TAK server.")
            await asyncio.sleep(1)  # Add a small delay before closing, prevents [SSL: APPLICATION_DATA_AFTER_CLOSE_NOTIFY] error
            writer.close()
            await writer.wait_closed()

        except Exception as e:
            self.logger.error(f"Error sending to TAK: {e}")


class SensorNodeTracker:
    def __init__(self, logger):
        self.logger = logger
        self.past_positions = {}


    async def send_cot_gps_update(self, uid, callsign, lat, lon, alt, time, remarks, max_history=10):
        """
        Sends GPS update CoT messages to TAK server for current position and past positions.
        """
        # Ensure history size limit
        self.past_positions.setdefault(uid, []).append((lat, lon, alt, time))  # Creates dict key and empty list if it does not exist
        if len(self.past_positions[uid]) > max_history:
            self.past_positions[uid].pop(0)

        # Generate stale time (5 minutes ahead)
        time_format = "%Y-%m-%dT%H:%M:%SZ"
        stale_time = (datetime.now(timezone.utc) + timedelta(seconds=60)).strftime(time_format)

        # Get TAK server settings
        settings: dict = fissure.utils.get_fissure_config()
        s_addr = settings["tak"]["ip_addr"]
        s_port = settings["tak"]["port"]
        tak_cert = settings["tak"]["cert"]
        tak_key = settings["tak"]["key"]

        # List to hold CoT messages
        cot_messages = []

        # head of cot message
        message = f"""<?xml version="1.0" encoding="utf-16"?>
        <event version="2.0" uid="{uid}" type="b-m-r" how="h-e" time="{time}" start="{time}" stale="{stale_time}">
            <point lat="0" lon="0" hae="0" ce="9999999" le="9999999" />
            <detail>
                <contact callsign="{callsign}"/>""" # message for single message

        # **Loop through past positions and add them to CoT messages**
        for i, past_entry in enumerate(self.past_positions[uid]):
            if len(past_entry) == 4:
                p_lat, p_lon, p_alt, p_time = past_entry
            elif len(past_entry) == 3:
                p_lat, p_lon, p_time = past_entry
                p_alt = 0  # Default altitude if missing
            else:
                self.logger.error(f"Skipping malformed entry: {past_entry}")
                continue  # Skip invalid entries

            # append position to linked message
            message += f"""
                    <link type="b-m-p-w" point="{p_lat}, {p_lon}" />"""

        # close cot message
        message += f"""
                <link_attr color="-16776961" method="Driving" direction="Advance" routetype="Primary" order="Ascending Check Points" />
                <remarks>"{remarks}"</remarks>
                <archive />
                <__routeinfo>
                    <__navcues />
                </__routeinfo>
            </detail>
        </event>"""
        cot_messages.append(message)

        # Use Single Persistent Connection
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        context.load_cert_chain(certfile=tak_cert, keyfile=tak_key)

        # Base directory where TAK server versions are installed
        tak_base_path = os.path.expanduser("~/Installed_by_FISSURE")

        # List directories, ignoring .zip files
        try:
            tak_dirs = [
                d for d in os.listdir(tak_base_path)
                if d.startswith("takserver-docker-") and os.path.isdir(os.path.join(tak_base_path, d))
            ]
            tak_dirs.sort(reverse=True)  # Sort to get the latest version first
            takserver_path = os.path.join(tak_base_path, tak_dirs[0]) if tak_dirs else None
        except FileNotFoundError:
            takserver_path = None  # Handle case where directory does not exist

        # Construct the full path to root-ca.pem
        if takserver_path:
            root_ca_path = os.path.join(takserver_path, "tak/certs/files/root-ca.pem")
        else:
            root_ca_path = None  # No TAK server found

        context.load_verify_locations(root_ca_path)
        context.check_hostname = False

        try:
            reader, writer = await asyncio.open_connection(s_addr, s_port, ssl=context)

            for msg in cot_messages:
                writer.write(msg.encode('utf-8'))
                await writer.drain()  # Ensure the message is fully sent
                await asyncio.sleep(0.1)  # Prevent message flooding
                # print(msg)

            self.logger.info(f"Sent {len(cot_messages)} CoT messages (latest position + track history)")
            await asyncio.sleep(1.0)  # Add a small delay before closing, prevents [SSL: APPLICATION_DATA_AFTER_CLOSE_NOTIFY] error
            writer.close()
            await writer.wait_closed()

        except Exception as e:
            self.logger.error(f"Error sending to TAK: {e}")


if __name__ == "__main__":
    rc = 0
    try:
        run()
    except Exception:
        rc = 1

    sys.exit(rc)
