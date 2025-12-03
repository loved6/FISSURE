#!/usr/bin/env python3
import asyncio
import xml.etree.ElementTree as ET
import pytak
from configparser import ConfigParser
import logging

from fissure.utils.common import get_fissure_config
from fissure.callbacks import HiprFisrCallbacks

def load_config() -> ConfigParser:
    """
    Prepare TAK Configuration

    Returns
    -------
    ConfigParser
        Configuration parser object with TAK settings.
    """
    # load fissure config
    fissure_config = get_fissure_config()
    tak_config = fissure_config['tak']
    url = f"tls://{tak_config.get('ip_addr')}:{tak_config.get('port', '8089')}"

    # prepare config parser
    config = ConfigParser()
    config["mycottool"] = {"COT_URL": url}
    config = config["mycottool"]
    config["COT_URL"] = url
    config["PYTAK_TLS_CLIENT_CAFILE"] = tak_config.get('cert')
    config["PYTAK_TLS_CLIENT_CERT"] = tak_config.get('webadmin_cert')
    config["PYTAK_TLS_CLIENT_KEY"] = tak_config.get('key')
    config["PYTAK_TLS_DONT_VERIFY"] = str(1)
    config["PYTAK_TLS_CLIENT_PASSWORD"] = 'atakatak'
    return config

def gen_cot() -> str:
    """
    Generate CoT Event
    
    This is a simple example of generating a CoT Event.

    Returns
    -------
    str
        CoT Event as XML string.
    """
    root = ET.Element("event")
    root.set("version", "2.0")
    root.set("type", "a-f-G")  # insert your type of marker
    root.set("uid", "example")
    root.set("how", "h-g-i-g-o")
    root.set("time", pytak.cot_time())
    root.set("start", pytak.cot_time())
    root.set(
        "stale", pytak.cot_time(60)
    )  # time difference in seconds from 'start' when stale initiates
    detail = ET.SubElement(root, "detail")
    remarks = ET.SubElement(detail, "remarks")
    remarks.text = "Testing Testing2"
    contact = ET.SubElement(detail, "contact")
    callsign = ET.SubElement(contact, "callsign")
    callsign.text = "example"
    pt_attr = {
        "lat": "40.781789",  # set your lat (this loc points to Central Park NY)
        "lon": "-73.968698",  # set your long (this loc points to Central Park NY)
        "hae": "0",
        "ce": "0",
        "le": "0",
    }
    ET.SubElement(root, "point", attrib=pt_attr)
    print(ET.tostring(root))
    return ET.tostring(root)

class TakSender(pytak.QueueWorker):
    """
    TAK Sender Class
    """
    def __init__(self, queue: asyncio.queues.Queue, config: ConfigParser, cot: str, logger: logging.Logger = logging.getLogger()):
        """Initialize TAK Sender.

        Parameters
        ----------
        queue : asyncio.queues.Queue
            The queue to send data to.
        config : ConfigParser
            Configuration parser object with TAK settings.
        cot : str
            CoT Event as XML string.
        logger : logging.Logger, optional
            Logger instance, by default logging.getLogger()
        """
        super().__init__(queue, config)
        self._logger = logger
        self.cot = cot

    async def run(self):
        await self.put_queue(self.cot)

class TakReceiver(pytak.QueueWorker):
    """
    TAK Receiver Class

    Overload handle_data to process received CoT Events.
    """
    def __init__(self, queue: asyncio.queues.Queue, config: ConfigParser, hipfisr: object = None, logger: logging.Logger = logging.getLogger()):
        """Initialize TAK Receiver.

        Parameters
        ----------
        queue : asyncio.queues.Queue
            The queue to receive data from.
        config : ConfigParser
            Configuration parser object with TAK settings.
        hipfisr : object, optional
            HIPRFISR instance, by default None
        logger : logging.Logger, optional
            Logger instance, by default logging.getLogger()
        """
        super().__init__(queue, config)
        self._logger = logger
        self.hipfisr = hipfisr

    async def handle_data(self, data: str) -> None:
        """
        Handle data from the receive queue

        Parameters
        ----------
        data : str
            Data received from TAK server
        """
        data_decode = data.decode()
        self._logger.debug("TAK Msg Received:\n%s\n", data_decode)
        # print("TAK Msg Received:\n%s\n", data_decode)
        try:
            root = ET.fromstring(data_decode)
            uid = root.get("uid", "unknown")
            remarks = root.find(".//detail/remarks")
            if remarks is not None and remarks.text:
                if remarks.text == "FTN Requested: Plugin Names":
                    self._logger.info(f"FTN Plugin Names Requested for UID: {uid}")
                    await HiprFisrCallbacks.sendPluginNamesTak(self.hipfisr, uid, sensor_node_id=0)
                elif remarks.text.startswith("FTN Requested: Plugin Actions:"):
                    plugin_name = remarks.text.split("FTN Requested: Plugin Actions:")[-1].strip()
                    self._logger.info(f"FTN Plugin Action Names Requested: {plugin_name} for UID: {uid}")
                    await HiprFisrCallbacks.sendPluginActionNamesTak(self.hipfisr, uid, plugin_name, sensor_node_id=0)
                elif remarks.text.startswith("FTN Requested: Plugin Action:"):
                    remaining = remarks.text.split("FTN Requested: Plugin Action:")[-1].strip()
                    plugin_name, action_name = remaining.split(":", 1)
                    plugin_name = plugin_name.strip()
                    action_name = action_name.strip()
                    self._logger.info(f"FTN Plugin Action Requested: {action_name} for UID: {uid}")
                    await HiprFisrCallbacks.sendPluginActionTak(self.hipfisr, uid, sensor_node_id=0, plugin_name=plugin_name, action_name=action_name, parameters={})
                elif remarks.text.startswith("FTN Requested: Plugin Action Stop"):
                    self._logger.info(f"FTN Plugin Stop Requested for UID: {uid}")
                    await HiprFisrCallbacks.stop_all_plugin_operations(self.hipfisr, uid, sensor_node_id=0)
        except ET.ParseError as e:
            self._logger.error("XML Parse Error: %s", e)

    async def run(self) -> None:
        """
        Wait forever for CoT events from TAK server.
        Only exits on shutdown or None sentinel.
        """
        self._logger.debug("TAK Receiver started, waiting for data...")

        while True:
            try:
                data = await self.queue.get()  # blocks until a message arrives

                # pytak uses None as shutdown sentinel in some cases
                if data is None:
                    self._logger.debug("TAK receiver got shutdown signal.")
                    return

                await self.handle_data(data)

            except asyncio.CancelledError:
                # Task cancelled cleanly on shutdown
                return
            except Exception as e:
                self._logger.error(f"TAK receiver error: {e}")
                await asyncio.sleep(1)

async def main(rx: bool = True, tx: bool = False):
    """
    TAK server client main function

    Parameters
    ----------
    rx : bool, optional
        Enable receiving, by default True
    tx : bool, optional
        Enable transmitting, by default False
    """
    # load config
    config = load_config()

    # Initializes worker queues and tasks.
    clitool = pytak.CLITool(config)
    await clitool.setup()

    logger = logging.getLogger("TakReceiver")
    logger.setLevel(logging.INFO)
    while True:
        # add tasks
        tasks = []
        if rx:
            tasks.append(TakReceiver(clitool.rx_queue, config, logger=logger))
        if tx:
            tasks.append(TakSender(clitool.tx_queue, config, cot=gen_cot(), logger=logger))
        clitool.add_tasks(
            set(tasks)
        )

        # run task
        await clitool.run()

        # wait before restarting
        await asyncio.sleep(1.0)

if __name__ == "__main__":
    asyncio.run(main(True, True))