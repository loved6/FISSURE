from ..Slots import HardwareSelectSlots
from .UI_Types import UI_Types
from PyQt5 import QtCore, QtWidgets, QtGui

import fissure.comms
import os
import time
import yaml


class HardwareSelectDialog(QtWidgets.QDialog, UI_Types.HW_Select):
    tabWidget: QtWidgets.QTabWidget
    guess_index: int

    def __init__(self, parent: QtWidgets.QWidget, dashboard: QtCore.QObject, node_idx: int = None):
        QtWidgets.QDialog.__init__(self, parent)
        self.parent = parent
        self.dashboard = dashboard
        self.setupUi(self)
        self.guess_index = 0
        #dashboard.logger.critical(f"HWSelect clicked (node_idx = {node_idx})")

        # Prevent Resizing/Maximizing
        self.parent.setFixedSize(QtCore.QSize(1100, 850))

        # Connect Signals to Slots
        self.__connect_slots__()

        # Disable Unused Tabs
        self.tabWidget_nodes.setTabEnabled(0, False)
        self.tabWidget_nodes.setTabEnabled(1, False)  # No function to hide tab visibility in PyQt4
        self.tabWidget_nodes.setTabEnabled(2, False)
        self.tabWidget_nodes.setTabEnabled(3, False)
        self.tabWidget_nodes.setTabEnabled(4, False)

        # Hide Import/Export until fixed
        self.pushButton_import.setVisible(False)  # TODO: does import/export make sense for the current connection mechanism?
        self.pushButton_export.setVisible(False)

        # Enable Tabs for Configured Nodes
        get_sensor_node = ["sensor_node1", "sensor_node2", "sensor_node3", "sensor_node4", "sensor_node5"]
        for n in range(0, len(get_sensor_node)):
            if self.dashboard.backend.settings[get_sensor_node[n]]["nickname"] != "":
                self.tabWidget_nodes.setTabEnabled(n, True)
            if n == node_idx:
                self.tabWidget_nodes.setTabEnabled(n, True)

        # Change Tab
        self.tabWidget_nodes.setCurrentIndex(node_idx)
        self.tabWidget_nodes.setTabEnabled((node_idx), True)

        # Support Only One Local Sensor Node
        local_assigned = False
        for n in range(0, 5):
            if str(self.dashboard.backend.settings[get_sensor_node[n]]["local_remote"]) == "local":
                local_assigned = True
        
        # Detect Connecting to Local Sensor Node without Saving
        self.new_local_connection = [False, False, False, False, False]

        # Hide Temporary Text
        self.label2_scan_results_probe_1.setVisible(False)
        self.label2_scan_results_probe_2.setVisible(False)
        self.label2_scan_results_probe_3.setVisible(False)
        self.label2_scan_results_probe_4.setVisible(False)
        self.label2_scan_results_probe_5.setVisible(False)

        # Recall Saved Settings
        nickname_widgets = [
            self.textEdit_nickname_1,
            self.textEdit_nickname_2,
            self.textEdit_nickname_3,
            self.textEdit_nickname_4,
            self.textEdit_nickname_5,
        ]
        location_widgets = [
            self.textEdit_location_1,
            self.textEdit_location_2,
            self.textEdit_location_3,
            self.textEdit_location_4,
            self.textEdit_location_5,
        ]
        notes_widgets = [
            self.textEdit_notes_1,
            self.textEdit_notes_2,
            self.textEdit_notes_3,
            self.textEdit_notes_4,
            self.textEdit_notes_5,
        ]
        ip_widgets = [
            self.textEdit_ip_addr_1, 
            self.textEdit_ip_addr_2, 
            self.textEdit_ip_addr_3, 
            self.textEdit_ip_addr_4, 
            self.textEdit_ip_addr_5
        ]
        msg_port_widgets = [
            self.textEdit_msg_port_1,
            self.textEdit_msg_port_2,
            self.textEdit_msg_port_3,
            self.textEdit_msg_port_4,
            self.textEdit_msg_port_5,
        ]
        hb_port_widgets = [
            self.textEdit_hb_port_1,
            self.textEdit_hb_port_2,
            self.textEdit_hb_port_3,
            self.textEdit_hb_port_4,
            self.textEdit_hb_port_5,
        ]
        local_widgets = [
            self.radioButton_local_1,
            self.radioButton_local_2,
            self.radioButton_local_3,
            self.radioButton_local_4,
            self.radioButton_local_5,
        ]
        remote_widgets = [
            self.radioButton_remote_1,
            self.radioButton_remote_2,
            self.radioButton_remote_3,
            self.radioButton_remote_4,
            self.radioButton_remote_5,
        ]
        autorun_widgets = [
            self.label2_autorun_value_1,
            self.label2_autorun_value_2,
            self.label2_autorun_value_3,
            self.label2_autorun_value_4,
            self.label2_autorun_value_5,
        ]
        autorun_delay_widgets = [
            self.label2_autorun_delay_value_1,
            self.label2_autorun_delay_value_2,
            self.label2_autorun_delay_value_3,
            self.label2_autorun_delay_value_4,
            self.label2_autorun_delay_value_5,
        ]
        console_logging_level_widgets = [
            self.label2_console_logging_level_value_1,
            self.label2_console_logging_level_value_2,
            self.label2_console_logging_level_value_3,
            self.label2_console_logging_level_value_4,
            self.label2_console_logging_level_value_5
        ]
        file_logging_level_widgets = [
            self.label2_file_logging_level_value_1,
            self.label2_file_logging_level_value_2,
            self.label2_file_logging_level_value_3,
            self.label2_file_logging_level_value_4,
            self.label2_file_logging_level_value_5
        ]
        meshtastic_serial_port_widgets = [
            self.comboBox_meshtastic_port_1,
            self.comboBox_meshtastic_port_2,
            self.comboBox_meshtastic_port_3,
            self.comboBox_meshtastic_port_4,
            self.comboBox_meshtastic_port_5
        ]
        meshtastic_serial_baud_rate_widgets = [
            self.comboBox_meshtastic_baud_rate_1,
            self.comboBox_meshtastic_baud_rate_2,
            self.comboBox_meshtastic_baud_rate_3,
            self.comboBox_meshtastic_baud_rate_4,
            self.comboBox_meshtastic_baud_rate_5
        ]
        network_type_widgets = [
            self.comboBox_network_type_1,
            self.comboBox_network_type_2,
            self.comboBox_network_type_3,
            self.comboBox_network_type_4,
            self.comboBox_network_type_5
        ]                        

        for n in range(0, len(get_sensor_node)):
            # Already occupied
            if self.dashboard.backend.settings[get_sensor_node[n]]["nickname"] != "":
                self.tabWidget_nodes.setTabText(n, str(self.dashboard.backend.settings[get_sensor_node[n]]["nickname"]))
                if str(self.dashboard.backend.settings[get_sensor_node[n]]["local_remote"]).lower() == "local":
                    pass
                else:
                    if local_assigned:
                        local_widgets[n].setEnabled(False)
                    remote_widgets[n].setChecked(True)
                    
                nickname_widgets[n].setPlainText(str(self.dashboard.backend.settings[get_sensor_node[n]]["nickname"]))
                location_widgets[n].setPlainText(str(self.dashboard.backend.settings[get_sensor_node[n]]["location"]))
                notes_widgets[n].setPlainText(str(self.dashboard.backend.settings[get_sensor_node[n]]["notes"]))
                ip_widgets[n].setPlainText(str(self.dashboard.backend.settings[get_sensor_node[n]]["ip_address"]))
                msg_port_widgets[n].setPlainText(str(self.dashboard.backend.settings[get_sensor_node[n]]["msg_port"]))
                hb_port_widgets[n].setPlainText(str(self.dashboard.backend.settings[get_sensor_node[n]]["hb_port"]))
                autorun_widgets[n].setText(str(self.dashboard.backend.settings[get_sensor_node[n]]["autorun"]))
                autorun_delay_widgets[n].setText(str(self.dashboard.backend.settings[get_sensor_node[n]]["autorun_delay_seconds"]))
                console_logging_level_widgets[n].setText(str(self.dashboard.backend.settings[get_sensor_node[n]]['console_logging_level']))
                file_logging_level_widgets[n].setText(str(self.dashboard.backend.settings[get_sensor_node[n]]['file_logging_level']))
                meshtastic_serial_port_widgets[n].addItem(str(self.dashboard.backend.settings[get_sensor_node[n]]['meshtastic_serial_port']))
                meshtastic_serial_baud_rate_widgets[n].addItem(str(self.dashboard.backend.settings[get_sensor_node[n]]['meshtastic_serial_baud_rate']))
                network_type_widgets[n].setCurrentText(str(self.dashboard.backend.settings[get_sensor_node[n]]['network_type']))

            # No nickname
            else:
                if local_assigned:
                    local_widgets[n].setEnabled(False)
                    remote_widgets[n].setEnabled(False)  # TODO: gets enabled in sensorNodeDisconnected, needs local_assigned
                    remote_widgets[n].setChecked(True)

            # Update if Connected
            if "OK" in self.dashboard.statusBar().sensor_nodes[n].text():
                if self.dashboard.backend.settings[get_sensor_node[n]]["network_type"] == "IP":
                    self.sensorNodeConnected(n, False)
                elif self.dashboard.backend.settings[get_sensor_node[n]]["network_type"] == "Meshtastic":
                    self.sensorNodeConnected(n, True)
            else:
                if self.dashboard.backend.settings[get_sensor_node[n]]["network_type"] == "IP":
                    self.sensorNodeDisconnected(n)
                elif self.dashboard.backend.settings[get_sensor_node[n]]["network_type"] == "Meshtastic":
                    self.sensorNodeDisconnected(n)
                else:
                    self.sensorNodeDisconnected(n)  # network_type is empty on first time


    def __connect_slots__(self):
        """
        Contains the connect functions for all the signals and slots
        """
        # Node Slots
        for node_idx in range(1, 6):
            # Connect slots for each node
            local_button: QtWidgets.QPushButton = getattr(self, f"radioButton_local_{node_idx}")
            remote_button: QtWidgets.QPushButton = getattr(self, f"radioButton_remote_{node_idx}")
            launch_button: QtWidgets.QPushButton = getattr(self, f'pushButton_launch_{node_idx}')
            ping_button: QtWidgets.QPushButton = getattr(self, f"pushButton_ping_{node_idx}")
            connect_button: QtWidgets.QPushButton = getattr(self, f"pushButton_connect_{node_idx}")
            disconnect_button: QtWidgets.QPushButton = getattr(self, f"pushButton_disconnect_{node_idx}")
            more_a_button: QtWidgets.QPushButton = getattr(self, f"pushButton_more_a_{node_idx}")
            more_b_button: QtWidgets.QPushButton = getattr(self, f"pushButton_more_b_{node_idx}")
            find_button: QtWidgets.QPushButton = getattr(self, f"pushButton_find_{node_idx}")
            map_button: QtWidgets.QPushButton = getattr(self, f"pushButton_map_{node_idx}")
            manual_button: QtWidgets.QPushButton = getattr(self, f"pushButton_manual_{node_idx}")
            scan_results_remove_button: QtWidgets.QPushButton = getattr(self, f"pushButton_scan_results_remove_{node_idx}")
            scan_results_remove_all_button: QtWidgets.QPushButton = getattr(self, f"pushButton_scan_results_remove_all_{node_idx}")
            add_to_all_button: QtWidgets.QPushButton = getattr(self, f"pushButton_add_to_all_{node_idx}")
            rows_to_all_button: QtWidgets.QPushButton = getattr(self, f"pushButton_rows_to_all_{node_idx}")
            tsi_button: QtWidgets.QPushButton = getattr(self, f"pushButton_tsi_{node_idx}")
            pd_button: QtWidgets.QPushButton = getattr(self, f"pushButton_pd_{node_idx}")
            attack_button: QtWidgets.QPushButton = getattr(self, f"pushButton_attack_{node_idx}")
            iq_button: QtWidgets.QPushButton = getattr(self, f"pushButton_iq_{node_idx}")
            archive_button: QtWidgets.QPushButton = getattr(self, f"pushButton_archive_{node_idx}")
            remove_tsi_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remove_tsi_{node_idx}")
            remove_pd_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remove_pd_{node_idx}")
            remove_attack_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remove_attack_{node_idx}")
            remove_iq_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remove_iq_{node_idx}")
            remove_archive_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remove_archive_{node_idx}")
            remove_all_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remove_all_{node_idx}")
            scan_button: QtWidgets.QPushButton = getattr(self, f"pushButton_scan_{node_idx}")
            probe_button: QtWidgets.QPushButton = getattr(self, f"pushButton_scan_results_probe_{node_idx}")
            guess_button: QtWidgets.QPushButton = getattr(self, f"pushButton_scan_results_guess_{node_idx}")
            network_type_combobox: QtWidgets.QComboBox = getattr(self, f"comboBox_network_type_{node_idx}")
            meshtastic_refresh_button: QtWidgets.QPushButton = getattr(self, f"pushButton_meshtastic_refresh_{node_idx}")
            meshtastic_info_button: QtWidgets.QPushButton = getattr(self, f"pushButton_meshtastic_info_{node_idx}")
            meshtastic_connect_button: QtWidgets.QPushButton = getattr(self, f"pushButton_meshtastic_connect_{node_idx}")
            meshtastic_disconnect_button: QtWidgets.QPushButton = getattr(self, f"pushButton_meshtastic_disconnect_{node_idx}")
            meshtastic_recall_info_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remote_actions_meshtastic_recall_info_{node_idx}")
            meshtastic_recall_hardware_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remote_actions_meshtastic_recall_hardware_{node_idx}")
            meshtastic_recall_status_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remote_actions_meshtastic_recall_status_{node_idx}")
            meshtastic_gps_beacon_enable_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remote_actions_meshtastic_gps_beacon_enable_{node_idx}")
            meshtastic_gps_beacon_disable_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remote_actions_meshtastic_gps_beacon_disable_{node_idx}")
            ip_gps_beacon_disable_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remote_actions_ip_gps_beacon_enable_disable_{node_idx}")
            ip_gps_beacon_refresh_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remote_actions_ip_gps_beacon_refresh_{node_idx}")
            local_actions_meshtastic_info_button: QtWidgets.QPushButton = getattr(self, f"pushButton_local_actions_meshtastic_info_{node_idx}")
            remote_actions_ip_reboot_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remote_actions_ip_reboot_{node_idx}")
            remote_actions_meshtastic_reboot_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remote_actions_meshtastic_reboot_{node_idx}")
            remote_actions_ip_uptime_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remote_actions_ip_uptime_{node_idx}")
            remote_actions_meshtastic_uptime_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remote_actions_meshtastic_uptime_{node_idx}")
            remote_actions_meshtastic_memory_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remote_actions_meshtastic_memory_{node_idx}")
            remote_actions_ip_memory_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remote_actions_ip_memory_{node_idx}")
            remote_actions_meshtastic_disk_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remote_actions_meshtastic_disk_{node_idx}")
            remote_actions_ip_disk_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remote_actions_ip_disk_{node_idx}")
            remote_actions_meshtastic_cpu_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remote_actions_meshtastic_cpu_{node_idx}")
            remote_actions_ip_cpu_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remote_actions_ip_cpu_{node_idx}")           
            remote_actions_meshtastic_processes_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remote_actions_meshtastic_processes_{node_idx}")           
            remote_actions_ip_processes_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remote_actions_ip_processes_{node_idx}")           
            remote_actions_meshtastic_ifconfig_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remote_actions_meshtastic_ifconfig_{node_idx}")           
            remote_actions_ip_ifconfig_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remote_actions_ip_ifconfig_{node_idx}")           
            remote_actions_meshtastic_iwconfig_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remote_actions_meshtastic_iwconfig_{node_idx}")           
            remote_actions_ip_iwconfig_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remote_actions_ip_iwconfig_{node_idx}")
            remote_actions_ip_ping_button: QtWidgets.QPushButton = getattr(self, f"pushButton_remote_actions_ip_ping_{node_idx}")

            local_button.clicked.connect(lambda _, idx=node_idx: HardwareSelectSlots.local(self, tab_index=idx - 1))
            remote_button.clicked.connect(lambda _, idx=node_idx: HardwareSelectSlots.remote(self, tab_index=idx - 1))
            launch_button.clicked.connect(lambda: HardwareSelectSlots.launch(self))
            ping_button.clicked.connect(lambda: HardwareSelectSlots.ping(self))
            connect_button.clicked.connect(lambda: HardwareSelectSlots.connect(self))
            disconnect_button.clicked.connect(lambda: HardwareSelectSlots.disconnect(self))
            more_a_button.clicked.connect(lambda: HardwareSelectSlots.more(self))
            more_b_button.clicked.connect(lambda: HardwareSelectSlots.more(self))
            find_button.clicked.connect(lambda: HardwareSelectSlots.find(self))
            map_button.clicked.connect(lambda: HardwareSelectSlots.map(self))
            manual_button.clicked.connect(lambda: HardwareSelectSlots.manual(self))
            scan_results_remove_button.clicked.connect(lambda: HardwareSelectSlots.scan_results_remove(self))
            scan_results_remove_all_button.clicked.connect(lambda: HardwareSelectSlots.scan_results_remove_all(self))
            add_to_all_button.clicked.connect(lambda: HardwareSelectSlots.add_to_all(self))
            rows_to_all_button.clicked.connect(lambda: HardwareSelectSlots.rows_to_all(self))
            tsi_button.clicked.connect(lambda: HardwareSelectSlots.tsi(self))
            pd_button.clicked.connect(lambda: HardwareSelectSlots.pd(self))
            attack_button.clicked.connect(lambda: HardwareSelectSlots.attack(self))
            iq_button.clicked.connect(lambda: HardwareSelectSlots.iq(self))
            archive_button.clicked.connect(lambda: HardwareSelectSlots.archive(self))
            remove_tsi_button.clicked.connect(lambda: HardwareSelectSlots.remove_tsi(self))
            remove_pd_button.clicked.connect(lambda: HardwareSelectSlots.remove_pd(self))
            remove_attack_button.clicked.connect(lambda: HardwareSelectSlots.remove_attack(self))
            remove_iq_button.clicked.connect(lambda: HardwareSelectSlots.remove_iq(self))
            remove_archive_button.clicked.connect(lambda: HardwareSelectSlots.remove_archive(self))
            remove_all_button.clicked.connect(lambda: HardwareSelectSlots.remove_all(self))
            scan_button.clicked.connect(lambda: HardwareSelectSlots.scan(self))
            probe_button.clicked.connect(lambda: HardwareSelectSlots.probe(self))
            guess_button.clicked.connect(lambda: HardwareSelectSlots.guess(self))
            network_type_combobox.currentIndexChanged.connect(lambda: HardwareSelectSlots.network_type_changed(self))
            meshtastic_refresh_button.clicked.connect(lambda: HardwareSelectSlots.meshtastic_refresh(self))
            meshtastic_info_button.clicked.connect(lambda: HardwareSelectSlots.meshtastic_info(self))
            meshtastic_connect_button.clicked.connect(lambda: HardwareSelectSlots.meshtastic_connect(self))
            meshtastic_disconnect_button.clicked.connect(lambda: HardwareSelectSlots.meshtastic_disconnect(self))
            meshtastic_recall_info_button.clicked.connect(lambda: HardwareSelectSlots.meshtastic_recall_info(self))
            meshtastic_recall_hardware_button.clicked.connect(lambda: HardwareSelectSlots.meshtastic_recall_hardware(self))
            meshtastic_recall_status_button.clicked.connect(lambda: HardwareSelectSlots.meshtastic_recall_status(self))
            meshtastic_gps_beacon_enable_button.clicked.connect(lambda: HardwareSelectSlots.meshtastic_gps_beacon_enable(self))
            meshtastic_gps_beacon_disable_button.clicked.connect(lambda: HardwareSelectSlots.meshtastic_gps_beacon_disable(self))
            ip_gps_beacon_disable_button.clicked.connect(lambda: HardwareSelectSlots.ip_gps_beacon_enable_disable(self))
            ip_gps_beacon_refresh_button.clicked.connect(lambda: HardwareSelectSlots.ip_gps_beacon_refresh(self))
            local_actions_meshtastic_info_button.clicked.connect(lambda: HardwareSelectSlots.meshtastic_info(self))
            remote_actions_ip_reboot_button.clicked.connect(lambda: HardwareSelectSlots.ip_reboot(self))
            remote_actions_meshtastic_reboot_button.clicked.connect(lambda: HardwareSelectSlots.meshtastic_reboot(self))
            remote_actions_ip_uptime_button.clicked.connect(lambda: HardwareSelectSlots.ip_uptime(self))
            remote_actions_meshtastic_uptime_button.clicked.connect(lambda: HardwareSelectSlots.meshtastic_uptime(self))
            remote_actions_meshtastic_memory_button.clicked.connect(lambda: HardwareSelectSlots.meshtastic_memory(self))
            remote_actions_ip_memory_button.clicked.connect(lambda: HardwareSelectSlots.ip_memory(self))
            remote_actions_meshtastic_disk_button.clicked.connect(lambda: HardwareSelectSlots.meshtastic_disk(self))
            remote_actions_ip_disk_button.clicked.connect(lambda: HardwareSelectSlots.ip_disk(self))
            remote_actions_meshtastic_cpu_button.clicked.connect(lambda: HardwareSelectSlots.meshtastic_cpu(self))
            remote_actions_ip_cpu_button.clicked.connect(lambda: HardwareSelectSlots.ip_cpu(self))              
            remote_actions_meshtastic_processes_button.clicked.connect(lambda: HardwareSelectSlots.meshtastic_processes(self))
            remote_actions_ip_processes_button.clicked.connect(lambda: HardwareSelectSlots.ip_processes(self))         
            remote_actions_meshtastic_ifconfig_button.clicked.connect(lambda: HardwareSelectSlots.meshtastic_ifconfig(self))
            remote_actions_ip_ifconfig_button.clicked.connect(lambda: HardwareSelectSlots.ip_ifconfig(self))   
            remote_actions_meshtastic_iwconfig_button.clicked.connect(lambda: HardwareSelectSlots.meshtastic_iwconfig(self))
            remote_actions_ip_iwconfig_button.clicked.connect(lambda: HardwareSelectSlots.ip_iwconfig(self))                              
            remote_actions_ip_ping_button.clicked.connect(lambda: HardwareSelectSlots.ip_ping(self))                              

        # Connect general slots
        self.pushButton_import.clicked.connect(lambda: HardwareSelectSlots.importClicked(self, settings_dict="", recall_settings_on_connect=False, first_time=True))
        self.pushButton_export.clicked.connect(lambda: HardwareSelectSlots.export(self))
        self.pushButton_apply.clicked.connect(lambda: HardwareSelectSlots.apply(self))
        self.pushButton_cancel.clicked.connect(self.close)
        self.pushButton_delete.clicked.connect(lambda: HardwareSelectSlots.delete(self))

        # New Test Items, Move these up
        pushButton_node_refresh_button: QtWidgets.QPushButton = getattr(self, f"pushButton_node_refresh_1")
        pushButton_node_refresh_button.clicked.connect(lambda: HardwareSelectSlots.ip_node_refresh(self))
        pushButton_node_select_button: QtWidgets.QPushButton = getattr(self, f"pushButton_node_select_1")
        pushButton_node_select_button.clicked.connect(lambda: HardwareSelectSlots.ip_node_select(self))        
        pushButton_ip_node_reconnect_button: QtWidgets.QPushButton = getattr(self, f"pushButton_ip_node_reconnect_1")
        pushButton_ip_node_reconnect_button.clicked.connect(lambda: HardwareSelectSlots.ip_node_reconnect(self))

        


    def scanReturn(self, tab_index, all_scan_results):
        """Populates the scan results table with the results of the hardware scan."""
        tab_index = int(tab_index) + 1  # +1 to match widget numbering

        # Dynamically retrieve widgets based on tab index
        get_tableWidget = getattr(self, f"tableWidget_scan_results_{tab_index}")
        get_tableWidget_scan_results = get_tableWidget  # Alias for clarity
        get_line3_scan_results = getattr(self, f"line3_scan_results_{tab_index}")

        # Get all relevant push buttons dynamically
        push_button_names = [
            "pushButton_add_to_all",
            "pushButton_rows_to_all",
            "pushButton_tsi",
            "pushButton_pd",
            "pushButton_attack",
            "pushButton_iq",
            "pushButton_archive",
            "pushButton_scan_results_remove",
            "pushButton_scan_results_remove_all",
            "pushButton_scan_results_probe",
            "pushButton_scan_results_guess",
        ]
        get_pushButtons = [getattr(self, f"{name}_{tab_index}") for name in push_button_names]

        # Add to Scan Results Table
        for row_data in all_scan_results:
            rows = get_tableWidget.rowCount()
            get_tableWidget.setRowCount(rows + 1)
            for col, cell_value in enumerate(row_data):
                table_item = QtWidgets.QTableWidgetItem(cell_value)
                table_item.setTextAlignment(QtCore.Qt.AlignCenter)
                get_tableWidget.setItem(rows, col, table_item)
            self.highlight_hardware_id(get_tableWidget, rows)

        # Update UI
        get_tableWidget.setCurrentCell(get_tableWidget.rowCount() - 1, 0)
        get_tableWidget.resizeColumnsToContents()
        get_tableWidget.resizeRowsToContents()
        get_tableWidget.horizontalHeader().setStretchLastSection(False)
        get_tableWidget.horizontalHeader().setStretchLastSection(True)

        # Enable relevant buttons if there are rows in the table
        if get_tableWidget.rowCount() > 0:
            for btn in get_pushButtons:
                btn.setEnabled(True)
            get_tableWidget_scan_results.setEnabled(True)
            get_line3_scan_results.setEnabled(True)


    def guessReturn(self, tab_index, get_row, get_hardware, get_row_text, get_guess_index):
        """Populates the scan results table with the results of the hardware scan."""
        tab_index = int(tab_index)

        # Update Guess Index
        self.guess_index = get_guess_index

        # Fill Cells by Hardware
        scan_results_tables = [
            self.tableWidget_scan_results_1,
            self.tableWidget_scan_results_2,
            self.tableWidget_scan_results_3,
            self.tableWidget_scan_results_4,
            self.tableWidget_scan_results_5,
        ]

        if get_hardware == "USRP X3x0":
            pass

        elif get_hardware == "USRP B2x0":
            new_serial = str(get_row_text[3])
            table_item = QtWidgets.QTableWidgetItem(new_serial)
            table_item.setTextAlignment(QtCore.Qt.AlignCenter)
            scan_results_tables[tab_index].setItem(get_row, 3, table_item)

        elif get_hardware == "USRP B20xmini":
            new_serial = str(get_row_text[3])
            table_item = QtWidgets.QTableWidgetItem(new_serial)
            table_item.setTextAlignment(QtCore.Qt.AlignCenter)
            scan_results_tables[tab_index].setItem(get_row, 3, table_item)

        elif get_hardware == "bladeRF":
            new_serial = str(get_row_text[3])
            table_item = QtWidgets.QTableWidgetItem(new_serial)
            table_item.setTextAlignment(QtCore.Qt.AlignCenter)
            scan_results_tables[tab_index].setItem(get_row, 3, table_item)

        elif get_hardware == "LimeSDR":
            pass

        elif get_hardware == "HackRF":
            new_serial = str(get_row_text[3])
            table_item = QtWidgets.QTableWidgetItem(new_serial)
            table_item.setTextAlignment(QtCore.Qt.AlignCenter)
            scan_results_tables[tab_index].setItem(get_row, 3, table_item)

        elif get_hardware == "PlutoSDR":
            pass

        elif get_hardware == "USRP2":
            # Update Serial, IP Address, Daughterboard
            new_serial = str(get_row_text[3])
            table_item1 = QtWidgets.QTableWidgetItem(new_serial)
            table_item1.setTextAlignment(QtCore.Qt.AlignCenter)
            scan_results_tables[tab_index].setItem(get_row, 3, table_item1)

            new_ip = str(get_row_text[5])
            table_item2 = QtWidgets.QTableWidgetItem(new_ip)
            table_item2.setTextAlignment(QtCore.Qt.AlignCenter)
            scan_results_tables[tab_index].setItem(get_row, 5, table_item2)

            new_daughterboard = str(get_row_text[6])
            table_item3 = QtWidgets.QTableWidgetItem(new_daughterboard)
            table_item3.setTextAlignment(QtCore.Qt.AlignCenter)
            scan_results_tables[tab_index].setItem(get_row, 6, table_item3)

        elif get_hardware == "USRP N2xx":
            # Update Serial, IP Address, Daughterboard
            new_serial = str(get_row_text[3])
            table_item1 = QtWidgets.QTableWidgetItem(new_serial)
            table_item1.setTextAlignment(QtCore.Qt.AlignCenter)
            scan_results_tables[tab_index].setItem(get_row, 3, table_item1)

            new_ip = str(get_row_text[5])
            table_item2 = QtWidgets.QTableWidgetItem(new_ip)
            table_item2.setTextAlignment(QtCore.Qt.AlignCenter)
            scan_results_tables[tab_index].setItem(get_row, 5, table_item2)

            new_daughterboard = str(get_row_text[6])
            table_item3 = QtWidgets.QTableWidgetItem(new_daughterboard)
            table_item3.setTextAlignment(QtCore.Qt.AlignCenter)
            scan_results_tables[tab_index].setItem(get_row, 6, table_item3)

        elif get_hardware == "bladeRF 2.0":
            new_serial = str(get_row_text[3])
            table_item = QtWidgets.QTableWidgetItem(new_serial)
            table_item.setTextAlignment(QtCore.Qt.AlignCenter)
            scan_results_tables[tab_index].setItem(get_row, 3, table_item)

        elif get_hardware == "USRP X410":
            # Update Serial, IP Address, Daughterboard
            new_serial = str(get_row_text[3])
            table_item1 = QtWidgets.QTableWidgetItem(new_serial)
            table_item1.setTextAlignment(QtCore.Qt.AlignCenter)
            scan_results_tables[tab_index].setItem(get_row, 3, table_item1)

            new_ip = str(get_row_text[5])
            table_item2 = QtWidgets.QTableWidgetItem(new_ip)
            table_item2.setTextAlignment(QtCore.Qt.AlignCenter)
            scan_results_tables[tab_index].setItem(get_row, 5, table_item2)

            new_daughterboard = str(get_row_text[6])
            table_item3 = QtWidgets.QTableWidgetItem(new_daughterboard)
            table_item3.setTextAlignment(QtCore.Qt.AlignCenter)
            scan_results_tables[tab_index].setItem(get_row, 6, table_item3)

        elif get_hardware == "802.11x Adapter":
            new_network_interface = str(get_row_text[4])
            table_item = QtWidgets.QTableWidgetItem(new_network_interface)
            table_item.setTextAlignment(QtCore.Qt.AlignCenter)
            scan_results_tables[tab_index].setItem(get_row, 4, table_item)

        elif get_hardware == "RTL2832U":
            new_serial = str(get_row_text[3])
            table_item = QtWidgets.QTableWidgetItem(new_serial)
            table_item.setTextAlignment(QtCore.Qt.AlignCenter)
            scan_results_tables[tab_index].setItem(get_row, 3, table_item)

        elif get_hardware == "RSPduo":
            new_serial = str(get_row_text[3])
            table_item = QtWidgets.QTableWidgetItem(new_serial)
            table_item.setTextAlignment(QtCore.Qt.AlignCenter)
            scan_results_tables[tab_index].setItem(get_row, 3, table_item)

        elif get_hardware == "RSPdx":
            new_serial = str(get_row_text[3])
            table_item = QtWidgets.QTableWidgetItem(new_serial)
            table_item.setTextAlignment(QtCore.Qt.AlignCenter)
            scan_results_tables[tab_index].setItem(get_row, 3, table_item)
            
        elif get_hardware == "RSPdx R2":
            new_serial = str(get_row_text[3])
            table_item = QtWidgets.QTableWidgetItem(new_serial)
            table_item.setTextAlignment(QtCore.Qt.AlignCenter)
            scan_results_tables[tab_index].setItem(get_row, 3, table_item)

        elif get_hardware == "CaribouLite":
            new_uuid = str(get_row_text[1])
            table_item = QtWidgets.QTableWidgetItem(new_uuid)
            table_item.setTextAlignment(QtCore.Qt.AlignCenter)
            scan_results_tables[tab_index].setItem(get_row, 1, table_item)            

        # Highlight
        self.highlight_hardware_id(scan_results_tables[tab_index], get_row)
            
        # Resize the Scan Results Table
        scan_results_tables[tab_index].resizeColumnsToContents()
        scan_results_tables[tab_index].resizeRowsToContents()
        scan_results_tables[tab_index].horizontalHeader().setStretchLastSection(False)
        scan_results_tables[tab_index].horizontalHeader().setStretchLastSection(True)


    def sensorNodeConnected(self, tab_index=0, serial=False):
        """Updates widgets for a sensor node once it is connected to the rest of FISSURE."""
        # Reload hardware tables
        self.populate_hardware_tables(tab_index)

        # Adjust the tab index to match widget numbering
        tab_index += 1

        # Dynamically retrieve widgets
        stacked_widget = getattr(self, f"stackedWidget_local_remote_{tab_index}")
        select_connect_stacked_widget = getattr(self, f"stackedWidget_select_connect_{tab_index}")
        bottom_stacked_widget = getattr(self, f"stackedWidget_bottom_{tab_index}")
        scan_pushbutton = getattr(self, f"pushButton_scan_{tab_index}")
        local_button = getattr(self, f"radioButton_local_{tab_index}")
        remote_button = getattr(self, f"radioButton_remote_{tab_index}")
        recall_settings_local_widget = getattr(self, f"checkBox_recall_settings_local_{tab_index}")
        recall_settings_widget = getattr(self, f"checkBox_recall_settings_remote_{tab_index}")
        launch_widget = getattr(self, f"pushButton_launch_{tab_index}")
        connect_widget = getattr(self, f"pushButton_connect_{tab_index}")
        ip_widget = getattr(self, f"textEdit_ip_addr_{tab_index}")
        msg_port_widget = getattr(self, f"textEdit_msg_port_{tab_index}")
        hb_port_widget = getattr(self, f"textEdit_hb_port_{tab_index}")
        find_widget = getattr(self, f"pushButton_find_{tab_index}")
        network_type_label = getattr(self, f"label2_network_type_{tab_index}")
        network_type_combobox = getattr(self, f"comboBox_network_type_{tab_index}")

        # Update widget states
        if serial == True:
            stacked_widget.setCurrentIndex(4)
            if stacked_widget.currentIndex() != 6:
                select_connect_stacked_widget.setCurrentIndex(1)
        else:
            stacked_widget.setCurrentIndex(2)
            if stacked_widget.currentIndex() != 6:
                select_connect_stacked_widget.setCurrentIndex(1)
        stacked_widget.setEnabled(True)
        bottom_stacked_widget.setCurrentIndex(0)
        scan_pushbutton.setEnabled(True)
        local_button.setEnabled(False)
        remote_button.setEnabled(False)
        launch_widget.setEnabled(True)
        recall_settings_local_widget.setEnabled(True)
        recall_settings_widget.setEnabled(True)
        connect_widget.setEnabled(True)
        ip_widget.setEnabled(True)
        msg_port_widget.setEnabled(True)
        hb_port_widget.setEnabled(True)
        find_widget.setEnabled(True)
        network_type_label.setEnabled(False)
        network_type_combobox.setEnabled(False)


    def importResults(self, settings_dict="", recall_settings_on_connect=False):
        """
        Reuses the importClicked function on recall settings return from sensor node.
        """
        # Function in Slots
        HardwareSelectSlots.importClicked(self, settings_dict, recall_settings_on_connect)


    def sensorNodeDisconnected(self, tab_index=0):
        """Updates widgets for a sensor node once it is disconnected from the rest of FISSURE."""
        # Adjust the tab index to match widget numbering
        tab_index += 1

        # Dynamically retrieve widgets
        stacked_widget = getattr(self, f"stackedWidget_local_remote_{tab_index}")
        select_connect_stacked_widget = getattr(self, f"stackedWidget_select_connect_{tab_index}")
        bottom_stacked_widget = getattr(self, f"stackedWidget_bottom_{tab_index}")
        scan_pushbutton = getattr(self, f"pushButton_scan_{tab_index}")
        local_button = getattr(self, f"radioButton_local_{tab_index}")
        remote_button = getattr(self, f"radioButton_remote_{tab_index}")
        details_stacked_widget = getattr(self, f"stackedWidget_details_{tab_index}")
        find_widget = getattr(self, f"pushButton_find_{tab_index}")
        network_type_label = getattr(self, f"label2_network_type_{tab_index}")
        network_type_combobox = getattr(self, f"comboBox_network_type_{tab_index}")
        
        sensor_node = f"sensor_node{tab_index}"

        # Handle widget state based on connection type
        if local_button.isChecked():
            # Saved
            if str(self.dashboard.backend.settings[sensor_node]["local_remote"]) == "local":
                # Saved
                stacked_widget.setCurrentIndex(6)  # Reconnect
                select_connect_stacked_widget.setCurrentIndex(1)
                local_button.setEnabled(False)
                remote_button.setEnabled(False)
            else:
                # Local node launched but not saved
                if self.new_local_connection[tab_index-1] == True:
                    stacked_widget.setCurrentIndex(6)  # Reconnect
                    select_connect_stacked_widget.setCurrentIndex(1)
                    local_button.setEnabled(False)
                    remote_button.setEnabled(False)
                
                # Not saved, not launched
                else:
                    stacked_widget.setCurrentIndex(0)  # Launch
                    select_connect_stacked_widget.setCurrentIndex(2)
                    local_button.setEnabled(True)
                    remote_button.setEnabled(True)

            network_type_label.setVisible(False)
            network_type_combobox.setVisible(False)
        else:
            # Remote disconnect, show reconnect
            if network_type_combobox.currentText() == "IP":
                stacked_widget.setCurrentIndex(6)
                if str(self.dashboard.backend.settings[sensor_node]["nickname"]) == "":
                    select_connect_stacked_widget.setCurrentIndex(0)
                    local_button.setEnabled(True)
                    remote_button.setEnabled(True)
                    network_type_label.setEnabled(True)
                    network_type_combobox.setEnabled(True)     
                else:
                    select_connect_stacked_widget.setCurrentIndex(1)
                    local_button.setEnabled(False)
                    remote_button.setEnabled(False)
                    network_type_label.setEnabled(False)
                    network_type_combobox.setEnabled(False)     

            elif network_type_combobox.currentText() == "Serial":
                stacked_widget.setCurrentIndex(3)
                # select_connect_stacked_widget.setCurrentIndex(1)

            network_type_label.setVisible(True)
            network_type_combobox.setVisible(True)

        bottom_stacked_widget.setCurrentIndex(1)
        scan_pushbutton.setEnabled(True)
        details_stacked_widget.setCurrentIndex(0)
        find_widget.setEnabled(False)

        # Support Only One Local Sensor Node
        for n in range(1, 6):  # Loop from 1 to 5 (matching widget numbering)
            sensor_node = f"sensor_node{n}"
            if str(self.dashboard.backend.settings[sensor_node]["local_remote"]) == "local":
                if n != tab_index:
                    local_button.setEnabled(False)


    def highlight_hardware_id(self, table_widget, row):
        """
        Highlights the hardware ID cell used when selecting hardware throughout FISSURE.
        """
        # Get Hardware
        get_hardware = str(table_widget.item(row, 0).text())

        # Retrieve Hardware ID Field
        get_column = fissure.utils.hardware.hardwareID_Column(get_hardware)

        # Highlight the Cell
        if get_column:
            cell_item = table_widget.item(row, get_column)
            cell_item.setBackground(QtGui.QColor("yellow"))


    def hardwareID_Present(self, table_widget, row):
        """
        Determines if the hardware ID is filled out for a row in the scan results before adding it to the defaults table.
        """
        # Determine Hardware Type
        get_hardware = str(table_widget.item(row,0).text())

        # Read Hardware ID Column
        get_column = fissure.utils.hardware.hardwareID_Column(get_hardware)
        cell_item = str(table_widget.item(row, get_column).text())
        if cell_item:
            return True
        else:
            return False
        

    def closeEvent(self, event):
        """
        Close the HW Select window without saving changes
        """
        # Detect Connect without Saving
        if any(self.new_local_connection):
            fissure.Dashboard.UI_Components.Qt5.errorMessage("Click Apply or delete local sensor node before cancelling.")
            event.ignore()
        else:
            # Close Window
            event.accept()


    def refreshNodes(self, dashboard_node_index, nodes):
        """
        Populate the table for the given dashboard node panel.
        """
        # Dynamic widget lookup
        dashboard_node_index = str(int(dashboard_node_index) + 1)
        table = getattr(self, f"tableWidget_node_list_{dashboard_node_index}")

        # Clear existing rows
        table.setRowCount(0)

        # Iterate over nodes dict (uuid → info)
        for uuid, info in nodes.items():
            # Extract fields with fallbacks
            ip            = info.get("ip", "—")
            nickname      = info.get("nickname", "—")
            # network_type  = info.get("network_type", "—")
            last_seen_ts  = info.get("last_seen", None)
            # connected     = info.get("connected", False)

            # Format last seen
            if last_seen_ts:
                delta = time.time() - last_seen_ts
                last_seen = f"{delta:.1f} sec ago"
            else:
                last_seen = "—"

            # Format connected
            # conn_text = "Yes" if connected else "No"

            # Add row
            row = table.rowCount()
            table.insertRow(row)

            table.setItem(row, 0, QtWidgets.QTableWidgetItem(str(uuid)))
            table.setItem(row, 1, QtWidgets.QTableWidgetItem(str(ip)))
            table.setItem(row, 2, QtWidgets.QTableWidgetItem(str(nickname)))
            # table.setItem(row, 3, QtWidgets.QTableWidgetItem(str(network_type)))
            table.setItem(row, 3, QtWidgets.QTableWidgetItem(str(last_seen)))
            # table.setItem(row, 5, QtWidgets.QTableWidgetItem(str(conn_text)))

            # Resize Table
            table.resizeColumnsToContents()
            table.resizeRowsToContents()
            table.horizontalHeader().setStretchLastSection(False)
            table.horizontalHeader().setStretchLastSection(True)


    def populate_hardware_tables(self, n):

        # Short alias to the node settings
        get_sensor_node = ["sensor_node1", "sensor_node2", "sensor_node3", "sensor_node4", "sensor_node5"]
        node_settings = self.dashboard.backend.settings[get_sensor_node[n]]

        # Mapping of settings key -> table widget list
        table_map = {
            "tsi": getattr(self, f"tableWidget_tsi_{n+1}"),
            "pd": getattr(self, f"tableWidget_pd_{n+1}"),
            "attack": getattr(self, f"tableWidget_attack_{n+1}"),
            "iq": getattr(self, f"tableWidget_iq_{n+1}"),
            "archive": getattr(self, f"tableWidget_archive_{n+1}"),
        }

        # Populate each table
        for key, table_widget in table_map.items():
            table_widget.setRowCount(0)  # Clear existing rows (safer for refresh)

            hardware_list = node_settings[key]

            for row in range(len(hardware_list)):
                row_items = hardware_list[row]
                table_widget.insertRow(table_widget.rowCount())

                for c, text in enumerate(row_items):
                    item = QtWidgets.QTableWidgetItem(text)
                    item.setTextAlignment(QtCore.Qt.AlignCenter)
                    table_widget.setItem(table_widget.rowCount() - 1, c, item)

            # Resize behavior for each table
            table_widget.resizeColumnsToContents()
            table_widget.resizeRowsToContents()
            table_widget.horizontalHeader().setStretchLastSection(False)
            table_widget.horizontalHeader().setStretchLastSection(True)