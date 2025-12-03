from PyQt5 import QtCore, QtWidgets

# import fissure.comms
import fissure.utils
import qasync
import time
import os
import yaml
import asyncio
import tempfile
import subprocess
import serial.tools.list_ports
import re


@QtCore.pyqtSlot(QtCore.QObject)
# def importClicked(HWSelect, settings_dict="", recall_settings_on_connect=False):
def importClicked(HWSelect, settings_dict="", recall_settings_on_connect=False):
    """Import all sensor node information from a .csv file."""
    # Choose File
    if len(settings_dict) == 0:
        import_button_pressed = False
        get_archive_folder = os.path.join(fissure.utils.SENSOR_NODE_DIR, "Import_Export_Files")
        fname = QtWidgets.QFileDialog.getOpenFileNames(
            None, "Select YAML File...", get_archive_folder, filter="YAML (*.yaml)"
        )
        if len(fname[0]) == 0:
            return
        sensor_index_start = 0
        sensor_index_end = 5
    else:
        import_button_pressed = True
        sensor_index_start = int(HWSelect.tabWidget_nodes.currentIndex())
        sensor_index_end = int(HWSelect.tabWidget_nodes.currentIndex()) + 1

    # Gather Widgets
    nickname_widgets = [
        HWSelect.textEdit_nickname_1,
        HWSelect.textEdit_nickname_2,
        HWSelect.textEdit_nickname_3,
        HWSelect.textEdit_nickname_4,
        HWSelect.textEdit_nickname_5,
    ]
    location_widgets = [
        HWSelect.textEdit_location_1,
        HWSelect.textEdit_location_2,
        HWSelect.textEdit_location_3,
        HWSelect.textEdit_location_4,
        HWSelect.textEdit_location_5,
    ]
    notes_widgets = [
        HWSelect.textEdit_notes_1,
        HWSelect.textEdit_notes_2,
        HWSelect.textEdit_notes_3,
        HWSelect.textEdit_notes_4,
        HWSelect.textEdit_notes_5,
    ]
    ip_widgets = [
        HWSelect.textEdit_ip_addr_1,
        HWSelect.textEdit_ip_addr_2, 
        HWSelect.textEdit_ip_addr_3, 
        HWSelect.textEdit_ip_addr_4, 
        HWSelect.textEdit_ip_addr_5
    ]
    
    msg_port_widgets = [
        HWSelect.textEdit_msg_port_1,
        HWSelect.textEdit_msg_port_2,
        HWSelect.textEdit_msg_port_3,
        HWSelect.textEdit_msg_port_4,
        HWSelect.textEdit_msg_port_5,
    ]
    hb_port_widgets = [
        HWSelect.textEdit_hb_port_1,
        HWSelect.textEdit_hb_port_2,
        HWSelect.textEdit_hb_port_3,
        HWSelect.textEdit_hb_port_4,
        HWSelect.textEdit_hb_port_5,
    ]
    local_widgets = [
        HWSelect.radioButton_local_1,
        HWSelect.radioButton_local_2,
        HWSelect.radioButton_local_3,
        HWSelect.radioButton_local_4,
        HWSelect.radioButton_local_5,
    ]
    remote_widgets = [
        HWSelect.radioButton_remote_1,
        HWSelect.radioButton_remote_2,
        HWSelect.radioButton_remote_3,
        HWSelect.radioButton_remote_4,
        HWSelect.radioButton_remote_5,
    ]
    hardware_tsi_widgets = [
        HWSelect.tableWidget_tsi_1,
        HWSelect.tableWidget_tsi_2,
        HWSelect.tableWidget_tsi_3,
        HWSelect.tableWidget_tsi_4,
        HWSelect.tableWidget_tsi_5,
    ]
    hardware_pd_widgets = [
        HWSelect.tableWidget_pd_1,
        HWSelect.tableWidget_pd_2,
        HWSelect.tableWidget_pd_3,
        HWSelect.tableWidget_pd_4,
        HWSelect.tableWidget_pd_5,
    ]
    hardware_attack_widgets = [
        HWSelect.tableWidget_attack_1,
        HWSelect.tableWidget_attack_2,
        HWSelect.tableWidget_attack_3,
        HWSelect.tableWidget_attack_4,
        HWSelect.tableWidget_attack_5,
    ]
    hardware_iq_widgets = [
        HWSelect.tableWidget_iq_1,
        HWSelect.tableWidget_iq_2,
        HWSelect.tableWidget_iq_3,
        HWSelect.tableWidget_iq_4,
        HWSelect.tableWidget_iq_5,
    ]
    hardware_archive_widgets = [
        HWSelect.tableWidget_archive_1,
        HWSelect.tableWidget_archive_2,
        HWSelect.tableWidget_archive_3,
        HWSelect.tableWidget_archive_4,
        HWSelect.tableWidget_archive_5,
    ]
    autorun_widgets = [
        HWSelect.label2_autorun_value_1,
        HWSelect.label2_autorun_value_2,
        HWSelect.label2_autorun_value_3,
        HWSelect.label2_autorun_value_4,
        HWSelect.label2_autorun_value_5,
    ]
    autorun_delay_widgets = [
        HWSelect.label2_autorun_delay_value_1,
        HWSelect.label2_autorun_delay_value_2,
        HWSelect.label2_autorun_delay_value_3,
        HWSelect.label2_autorun_delay_value_4,
        HWSelect.label2_autorun_delay_value_5,
    ]
    console_logging_level_widgets = [
        HWSelect.label2_console_logging_level_value_1,
        HWSelect.label2_console_logging_level_value_2,
        HWSelect.label2_console_logging_level_value_3,
        HWSelect.label2_console_logging_level_value_4,
        HWSelect.label2_console_logging_level_value_5
    ]
    file_logging_level_widgets = [
        HWSelect.label2_file_logging_level_value_1,
        HWSelect.label2_file_logging_level_value_2,
        HWSelect.label2_file_logging_level_value_3,
        HWSelect.label2_file_logging_level_value_4,
        HWSelect.label2_file_logging_level_value_5
    ]

    # Load the YAML File
    if len(settings_dict) == 0:
        with open(fname[0][0]) as yaml_library_file:
            settings_dict = yaml.load(yaml_library_file, yaml.FullLoader)
    else:
        # # Load the YAML String into a Dictionary
        # settings_dict = yaml.load(settings_dict, yaml.FullLoader)

        # Update Sensor Node Dictionary Key
        settings_dict["Sensor Node " + str(int(HWSelect.tabWidget_nodes.currentIndex()) + 1)] = settings_dict["Sensor Node"]
        del settings_dict["Sensor Node"]

    # Each Tab/Sensor Node
    for n in range(sensor_index_start, sensor_index_end):
        local_assigned = False
        ignore_nickname = False

        # Tab Enabled/Disabled
        if settings_dict["Sensor Node " + str(n + 1)]["enabled_disabled"] == "enabled":
            HWSelect.tabWidget_nodes.setTabEnabled(n, True)
            ignore_nickname = False
        else:
            HWSelect.tabWidget_nodes.setTabEnabled(n, False)
            HWSelect.tabWidget_nodes.setTabText(n, "")
            ignore_nickname = True

        # Local/Remote
        if recall_settings_on_connect == True:
            # Recalling Settings on Connect
            if local_widgets[n].isChecked() == True:
                settings_dict["Sensor Node " + str(n + 1)]["local_remote"] = "local"
                local_widgets[n].setChecked(True)
                local_assigned = True
                # local(HWSelect, tab_index=n)
                HWSelect.tabWidget_nodes.setTabText(n, "Local Sensor Node")
            else:
                settings_dict["Sensor Node " + str(n + 1)]["local_remote"] = "remote"
                remote_widgets[n].setChecked(True)
                # remote(HWSelect, tab_index=n)
        else:
            # Loading a YAML File
            if settings_dict["Sensor Node " + str(n + 1)]["local_remote"] == "local":
                local_widgets[n].setChecked(True)
                local_assigned = True
                # local(HWSelect, tab_index=n)
                HWSelect.tabWidget_nodes.setTabText(n, "Local Sensor Node")
            else:
                remote_widgets[n].setChecked(True)
                # remote(HWSelect, tab_index=n)

        # Nickname
        if local_widgets[n].isChecked() == True:
            nickname_widgets[n].setPlainText("Local Sensor Node")
            HWSelect.tabWidget_nodes.setTabText(n, "Local Sensor Node")
        else:
            nickname_widgets[n].setPlainText(settings_dict["Sensor Node " + str(n + 1)]["nickname"])
            HWSelect.tabWidget_nodes.setTabText(n, settings_dict["Sensor Node " + str(n + 1)]["nickname"])
        if ignore_nickname is True:
            HWSelect.tabWidget_nodes.setTabText(n, "")

        # Location
        location_widgets[n].setPlainText(settings_dict["Sensor Node " + str(n + 1)]["location"])

        # Notes
        notes_widgets[n].setPlainText(settings_dict["Sensor Node " + str(n + 1)]["notes"])

        # Autorun Details
        autorun_widgets[n].setText(str(settings_dict["Sensor Node " + str(n + 1)]["autorun"]))
        autorun_delay_widgets[n].setText(str(settings_dict["Sensor Node " + str(n + 1)]["autorun_delay_seconds"]))

        # Logging Details
        console_logging_level_widgets[n].setText(str(settings_dict['Sensor Node ' + str(n+1)]['console_logging_level']))
        file_logging_level_widgets[n].setText(str(settings_dict['Sensor Node ' + str(n+1)]['file_logging_level']))

        # IP Address
        ip_widgets[n].setPlainText(settings_dict["Sensor Node " + str(n + 1)]["ip_address"])

        # Ports
        msg_port_widgets[n].setPlainText(str(settings_dict["Sensor Node " + str(n + 1)]["msg_port"]))
        hb_port_widgets[n].setPlainText(str(settings_dict["Sensor Node " + str(n + 1)]["hb_port"]))

        # Clear the Tables
        hardware_tsi_widgets[n].setRowCount(0)
        hardware_pd_widgets[n].setRowCount(0)
        hardware_attack_widgets[n].setRowCount(0)
        hardware_iq_widgets[n].setRowCount(0)
        hardware_archive_widgets[n].setRowCount(0)

        # TSI
        for row_key in settings_dict["Sensor Node " + str(n + 1)]["tsi"]:
            hardware_tsi_widgets[n].setRowCount(hardware_tsi_widgets[n].rowCount() + 1)
            type_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["tsi"][row_key]["type"]
            )
            type_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_tsi_widgets[n].setItem(hardware_tsi_widgets[n].rowCount() - 1, 0, type_item)
            uid_item = QtWidgets.QTableWidgetItem(settings_dict["Sensor Node " + str(n + 1)]["tsi"][row_key]["uid"])
            uid_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_tsi_widgets[n].setItem(hardware_tsi_widgets[n].rowCount() - 1, 1, uid_item)
            radio_name_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["tsi"][row_key]["radio_name"]
            )
            radio_name_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_tsi_widgets[n].setItem(hardware_tsi_widgets[n].rowCount() - 1, 2, radio_name_item)
            serial_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["tsi"][row_key]["serial"]
            )
            serial_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_tsi_widgets[n].setItem(hardware_tsi_widgets[n].rowCount() - 1, 3, serial_item)
            network_interface_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["tsi"][row_key]["network_interface"]
            )
            network_interface_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_tsi_widgets[n].setItem(hardware_tsi_widgets[n].rowCount() - 1, 4, network_interface_item)
            ip_address_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["tsi"][row_key]["ip_address"]
            )
            ip_address_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_tsi_widgets[n].setItem(hardware_tsi_widgets[n].rowCount() - 1, 5, ip_address_item)
            daughterboard_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["tsi"][row_key]["daughterboard"]
            )
            daughterboard_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_tsi_widgets[n].setItem(hardware_tsi_widgets[n].rowCount() - 1, 6, daughterboard_item)

        # PD
        for row_key in settings_dict["Sensor Node " + str(n + 1)]["pd"]:
            hardware_pd_widgets[n].setRowCount(hardware_pd_widgets[n].rowCount() + 1)
            type_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["pd"][row_key]["type"]
            )
            type_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_pd_widgets[n].setItem(hardware_pd_widgets[n].rowCount() - 1, 0, type_item)
            uid_item = QtWidgets.QTableWidgetItem(settings_dict["Sensor Node " + str(n + 1)]["pd"][row_key]["uid"])
            uid_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_pd_widgets[n].setItem(hardware_pd_widgets[n].rowCount() - 1, 1, uid_item)
            radio_name_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["pd"][row_key]["radio_name"]
            )
            radio_name_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_pd_widgets[n].setItem(hardware_pd_widgets[n].rowCount() - 1, 2, radio_name_item)
            serial_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["pd"][row_key]["serial"]
            )
            serial_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_pd_widgets[n].setItem(hardware_pd_widgets[n].rowCount() - 1, 3, serial_item)
            network_interface_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["pd"][row_key]["network_interface"]
            )
            network_interface_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_pd_widgets[n].setItem(hardware_pd_widgets[n].rowCount() - 1, 4, network_interface_item)
            ip_address_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["pd"][row_key]["ip_address"]
            )
            ip_address_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_pd_widgets[n].setItem(hardware_pd_widgets[n].rowCount() - 1, 5, ip_address_item)
            daughterboard_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["pd"][row_key]["daughterboard"]
            )
            daughterboard_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_pd_widgets[n].setItem(hardware_pd_widgets[n].rowCount() - 1, 6, daughterboard_item)

        # Attack
        for row_key in settings_dict["Sensor Node " + str(n + 1)]["attack"]:
            hardware_attack_widgets[n].setRowCount(hardware_attack_widgets[n].rowCount() + 1)
            type_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["attack"][row_key]["type"]
            )
            type_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_attack_widgets[n].setItem(hardware_attack_widgets[n].rowCount() - 1, 0, type_item)
            uid_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["attack"][row_key]["uid"]
            )
            uid_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_attack_widgets[n].setItem(hardware_attack_widgets[n].rowCount() - 1, 1, uid_item)
            radio_name_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["attack"][row_key]["radio_name"]
            )
            radio_name_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_attack_widgets[n].setItem(hardware_attack_widgets[n].rowCount() - 1, 2, radio_name_item)
            serial_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["attack"][row_key]["serial"]
            )
            serial_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_attack_widgets[n].setItem(hardware_attack_widgets[n].rowCount() - 1, 3, serial_item)
            network_interface_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["attack"][row_key]["network_interface"]
            )
            network_interface_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_attack_widgets[n].setItem(hardware_attack_widgets[n].rowCount() - 1, 4, network_interface_item)
            ip_address_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["attack"][row_key]["ip_address"]
            )
            ip_address_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_attack_widgets[n].setItem(hardware_attack_widgets[n].rowCount() - 1, 5, ip_address_item)
            daughterboard_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["attack"][row_key]["daughterboard"]
            )
            daughterboard_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_attack_widgets[n].setItem(hardware_attack_widgets[n].rowCount() - 1, 6, daughterboard_item)

        # IQ
        for row_key in settings_dict["Sensor Node " + str(n + 1)]["iq"]:
            hardware_iq_widgets[n].setRowCount(hardware_iq_widgets[n].rowCount() + 1)
            type_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["iq"][row_key]["type"]
            )
            type_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_iq_widgets[n].setItem(hardware_iq_widgets[n].rowCount() - 1, 0, type_item)
            uid_item = QtWidgets.QTableWidgetItem(settings_dict["Sensor Node " + str(n + 1)]["iq"][row_key]["uid"])
            uid_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_iq_widgets[n].setItem(hardware_iq_widgets[n].rowCount() - 1, 1, uid_item)
            radio_name_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["iq"][row_key]["radio_name"]
            )
            radio_name_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_iq_widgets[n].setItem(hardware_iq_widgets[n].rowCount() - 1, 2, radio_name_item)
            serial_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["iq"][row_key]["serial"]
            )
            serial_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_iq_widgets[n].setItem(hardware_iq_widgets[n].rowCount() - 1, 3, serial_item)
            network_interface_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["iq"][row_key]["network_interface"]
            )
            network_interface_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_iq_widgets[n].setItem(hardware_iq_widgets[n].rowCount() - 1, 4, network_interface_item)
            ip_address_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["iq"][row_key]["ip_address"]
            )
            ip_address_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_iq_widgets[n].setItem(hardware_iq_widgets[n].rowCount() - 1, 5, ip_address_item)
            daughterboard_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["iq"][row_key]["daughterboard"]
            )
            daughterboard_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_iq_widgets[n].setItem(hardware_iq_widgets[n].rowCount() - 1, 6, daughterboard_item)

        # Archive
        for row_key in settings_dict["Sensor Node " + str(n + 1)]["archive"]:
            hardware_archive_widgets[n].setRowCount(hardware_archive_widgets[n].rowCount() + 1)
            type_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["archive"][row_key]["type"]
            )
            type_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_archive_widgets[n].setItem(hardware_archive_widgets[n].rowCount() - 1, 0, type_item)
            uid_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["archive"][row_key]["uid"]
            )
            uid_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_archive_widgets[n].setItem(hardware_archive_widgets[n].rowCount() - 1, 1, uid_item)
            radio_name_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["archive"][row_key]["radio_name"]
            )
            radio_name_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_archive_widgets[n].setItem(hardware_archive_widgets[n].rowCount() - 1, 2, radio_name_item)
            serial_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["archive"][row_key]["serial"]
            )
            serial_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_archive_widgets[n].setItem(hardware_archive_widgets[n].rowCount() - 1, 3, serial_item)
            network_interface_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["archive"][row_key]["network_interface"]
            )
            network_interface_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_archive_widgets[n].setItem(
                hardware_archive_widgets[n].rowCount() - 1, 4, network_interface_item
            )
            ip_address_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["archive"][row_key]["ip_address"]
            )
            ip_address_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_archive_widgets[n].setItem(hardware_archive_widgets[n].rowCount() - 1, 5, ip_address_item)
            daughterboard_item = QtWidgets.QTableWidgetItem(
                settings_dict["Sensor Node " + str(n + 1)]["archive"][row_key]["daughterboard"]
            )
            daughterboard_item.setTextAlignment(QtCore.Qt.AlignCenter)
            hardware_archive_widgets[n].setItem(hardware_archive_widgets[n].rowCount() - 1, 6, daughterboard_item)

        # Resize the Tables
        hardware_tsi_widgets[n].resizeColumnsToContents()
        hardware_tsi_widgets[n].resizeRowsToContents()
        hardware_tsi_widgets[n].horizontalHeader().setStretchLastSection(False)
        hardware_tsi_widgets[n].horizontalHeader().setStretchLastSection(True)
        hardware_pd_widgets[n].resizeColumnsToContents()
        hardware_pd_widgets[n].resizeRowsToContents()
        hardware_pd_widgets[n].horizontalHeader().setStretchLastSection(False)
        hardware_pd_widgets[n].horizontalHeader().setStretchLastSection(True)
        hardware_attack_widgets[n].resizeColumnsToContents()
        hardware_attack_widgets[n].resizeRowsToContents()
        hardware_attack_widgets[n].horizontalHeader().setStretchLastSection(False)
        hardware_attack_widgets[n].horizontalHeader().setStretchLastSection(True)
        hardware_iq_widgets[n].resizeColumnsToContents()
        hardware_iq_widgets[n].resizeRowsToContents()
        hardware_iq_widgets[n].horizontalHeader().setStretchLastSection(False)
        hardware_iq_widgets[n].horizontalHeader().setStretchLastSection(True)
        hardware_archive_widgets[n].resizeColumnsToContents()
        hardware_archive_widgets[n].resizeRowsToContents()
        hardware_archive_widgets[n].horizontalHeader().setStretchLastSection(False)
        hardware_archive_widgets[n].horizontalHeader().setStretchLastSection(True)

    # Enable/Disable Local and Remote Radio Buttons
    for k in range(sensor_index_start, sensor_index_end):
        if (local_assigned is True) and (remote_widgets[k].isChecked() is True):
            local_widgets[k].setEnabled(False)


@QtCore.pyqtSlot(QtCore.QObject)
def export(HWSelect: QtCore.QObject):
    """
    Exports all the sensor node information to a csv file.
    """
    # Choose File Location
    get_archive_folder = os.path.join(fissure.utils.SENSOR_NODE_DIR, "Import_Export_Files")
    path = QtWidgets.QFileDialog.getSaveFileName(HWSelect, "Save YAML", get_archive_folder, filter="YAML (*.yaml)")
    get_path = path[0]

    # Add Extension
    if get_path.endswith(".yaml") is False:
        get_path = get_path + ".yaml"

    # Save Values
    if len(path[0]) > 0:
        nickname_widgets = [
            HWSelect.textEdit_nickname_1,
            HWSelect.textEdit_nickname_2,
            HWSelect.textEdit_nickname_3,
            HWSelect.textEdit_nickname_4,
            HWSelect.textEdit_nickname_5,
        ]
        location_widgets = [
            HWSelect.textEdit_location_1,
            HWSelect.textEdit_location_2,
            HWSelect.textEdit_location_3,
            HWSelect.textEdit_location_4,
            HWSelect.textEdit_location_5,
        ]
        notes_widgets = [
            HWSelect.textEdit_notes_1,
            HWSelect.textEdit_notes_2,
            HWSelect.textEdit_notes_3,
            HWSelect.textEdit_notes_4,
            HWSelect.textEdit_notes_5,
        ]
        ip_widgets = [
            HWSelect.textEdit_ip_addr_1, 
            HWSelect.textEdit_ip_addr_2, 
            HWSelect.textEdit_ip_addr_3, 
            HWSelect.textEdit_ip_addr_4, 
            HWSelect.textEdit_ip_addr_5
        ]
        msg_port_widgets = [
            HWSelect.textEdit_msg_port_1,
            HWSelect.textEdit_msg_port_2,
            HWSelect.textEdit_msg_port_3,
            HWSelect.textEdit_msg_port_4,
            HWSelect.textEdit_msg_port_5,
        ]
        hb_port_widgets = [
            HWSelect.textEdit_hb_port_1,
            HWSelect.textEdit_hb_port_2,
            HWSelect.textEdit_hb_port_3,
            HWSelect.textEdit_hb_port_4,
            HWSelect.textEdit_hb_port_5,
        ]
        local_widgets = [
            HWSelect.radioButton_local_1,
            HWSelect.radioButton_local_2,
            HWSelect.radioButton_local_3,
            HWSelect.radioButton_local_4,
            HWSelect.radioButton_local_5,
        ]
        # remote_widgets = [
        #     HWSelect.radioButton_remote_1,
        #     HWSelect.radioButton_remote_2,
        #     HWSelect.radioButton_remote_3,
        #     HWSelect.radioButton_remote_4,
        #     HWSelect.radioButton_remote_5,
        # ]
        hardware_tsi_widgets = [
            HWSelect.tableWidget_tsi_1,
            HWSelect.tableWidget_tsi_2,
            HWSelect.tableWidget_tsi_3,
            HWSelect.tableWidget_tsi_4,
            HWSelect.tableWidget_tsi_5,
        ]
        hardware_pd_widgets = [
            HWSelect.tableWidget_pd_1,
            HWSelect.tableWidget_pd_2,
            HWSelect.tableWidget_pd_3,
            HWSelect.tableWidget_pd_4,
            HWSelect.tableWidget_pd_5,
        ]
        hardware_attack_widgets = [
            HWSelect.tableWidget_attack_1,
            HWSelect.tableWidget_attack_2,
            HWSelect.tableWidget_attack_3,
            HWSelect.tableWidget_attack_4,
            HWSelect.tableWidget_attack_5,
        ]
        hardware_iq_widgets = [
            HWSelect.tableWidget_iq_1,
            HWSelect.tableWidget_iq_2,
            HWSelect.tableWidget_iq_3,
            HWSelect.tableWidget_iq_4,
            HWSelect.tableWidget_iq_5,
        ]
        hardware_archive_widgets = [
            HWSelect.tableWidget_archive_1,
            HWSelect.tableWidget_archive_2,
            HWSelect.tableWidget_archive_3,
            HWSelect.tableWidget_archive_4,
            HWSelect.tableWidget_archive_5,
        ]
        autorun_widgets = [
            HWSelect.label2_autorun_value_1,
            HWSelect.label2_autorun_value_2,
            HWSelect.label2_autorun_value_3,
            HWSelect.label2_autorun_value_4,
            HWSelect.label2_autorun_value_5,
        ]
        autorun_delay_widgets = [
            HWSelect.label2_autorun_delay_value_1,
            HWSelect.label2_autorun_delay_value_2,
            HWSelect.label2_autorun_delay_value_3,
            HWSelect.label2_autorun_delay_value_4,
            HWSelect.label2_autorun_delay_value_5,
        ]
        console_logging_level_widgets = [
            HWSelect.label2_console_logging_level_value_1,
            HWSelect.label2_console_logging_level_value_2,
            HWSelect.label2_console_logging_level_value_3,
            HWSelect.label2_console_logging_level_value_4,
            HWSelect.label2_console_logging_level_value_5
        ]
        file_logging_level_widgets = [
            HWSelect.label2_file_logging_level_value_1,
            HWSelect.label2_file_logging_level_value_2,
            HWSelect.label2_file_logging_level_value_3,
            HWSelect.label2_file_logging_level_value_4,
            HWSelect.label2_file_logging_level_value_5
        ]

        settings_dict = {}
        for n in range(0, len(nickname_widgets)):
            sensor_dict = {}

            if len(HWSelect.tabWidget_nodes.tabText(n)) == 0:
                sensor_dict["enabled_disabled"] = "disabled"
            else:
                sensor_dict["enabled_disabled"] = "enabled"

            if local_widgets[n].isChecked() is True:
                sensor_dict["local_remote"] = "local"
            else:
                sensor_dict["local_remote"] = "remote"

            sensor_dict["nickname"] = str(nickname_widgets[n].toPlainText())
            sensor_dict["location"] = str(location_widgets[n].toPlainText())
            sensor_dict["notes"] = str(notes_widgets[n].toPlainText())
            sensor_dict["ip_address"] = str(ip_widgets[n].toPlainText())
            sensor_dict["msg_port"] = str(msg_port_widgets[n].toPlainText())
            sensor_dict["hb_port"] = str(hb_port_widgets[n].toPlainText())
            sensor_dict["autorun"] = bool(autorun_widgets[n].text())
            try:
                sensor_dict["autorun_delay_seconds"] = float(autorun_delay_widgets[n].text())
            except:
                sensor_dict["autorun_delay_seconds"] = ""
            sensor_dict['console_logging_level'] = console_logging_level_widgets[n].text()
            sensor_dict['file_logging_level'] = file_logging_level_widgets[n].text()

            # TSI
            tsi_dict = {}
            for row in range(hardware_tsi_widgets[n].rowCount()):
                row_dict = {}
                try:
                    row_dict["type"] = str(hardware_tsi_widgets[n].item(row, 0).text())
                except:
                    row_dict["type"] = ""
                try:
                    row_dict["uid"] = str(hardware_tsi_widgets[n].item(row, 1).text())
                except:
                    row_dict["uid"] = ""
                try:
                    row_dict["radio_name"] = str(hardware_tsi_widgets[n].item(row, 2).text())
                except:
                    row_dict["radio_name"] = ""
                try:
                    row_dict["serial"] = str(hardware_tsi_widgets[n].item(row, 3).text())
                except:
                    row_dict["serial"] = ""
                try:
                    row_dict["network_interface"] = str(hardware_tsi_widgets[n].item(row, 4).text())
                except:
                    row_dict["network_interface"] = ""
                try:
                    row_dict["ip_address"] = str(hardware_tsi_widgets[n].item(row, 5).text())
                except:
                    row_dict["ip_address"] = ""
                try:
                    row_dict["daughterboard"] = str(hardware_tsi_widgets[n].item(row, 6).text())
                except:
                    row_dict["daughterboard"] = ""
                tsi_dict[row] = row_dict
            sensor_dict["tsi"] = tsi_dict

            # PD
            pd_dict = {}
            for row in range(hardware_pd_widgets[n].rowCount()):
                row_dict = {}
                try:
                    row_dict["type"] = str(hardware_pd_widgets[n].item(row, 0).text())
                except:
                    row_dict["type"] = ""
                try:
                    row_dict["uid"] = str(hardware_pd_widgets[n].item(row, 1).text())
                except:
                    row_dict["uid"] = ""
                try:
                    row_dict["radio_name"] = str(hardware_pd_widgets[n].item(row, 2).text())
                except:
                    row_dict["radio_name"] = ""
                try:
                    row_dict["serial"] = str(hardware_pd_widgets[n].item(row, 3).text())
                except:
                    row_dict["serial"] = ""
                try:
                    row_dict["network_interface"] = str(hardware_pd_widgets[n].item(row, 4).text())
                except:
                    row_dict["network_interface"] = ""
                try:
                    row_dict["ip_address"] = str(hardware_pd_widgets[n].item(row, 5).text())
                except:
                    row_dict["ip_address"] = ""
                try:
                    row_dict["daughterboard"] = str(hardware_pd_widgets[n].item(row, 6).text())
                except:
                    row_dict["daughterboard"] = ""
                pd_dict[row] = row_dict
            sensor_dict["pd"] = pd_dict

            # Attack
            attack_dict = {}
            for row in range(hardware_attack_widgets[n].rowCount()):
                row_dict = {}
                try:
                    row_dict["type"] = str(hardware_attack_widgets[n].item(row, 0).text())
                except:
                    row_dict["type"] = ""
                try:
                    row_dict["uid"] = str(hardware_attack_widgets[n].item(row, 1).text())
                except:
                    row_dict["uid"] = ""
                try:
                    row_dict["radio_name"] = str(hardware_attack_widgets[n].item(row, 2).text())
                except:
                    row_dict["radio_name"] = ""
                try:
                    row_dict["serial"] = str(hardware_attack_widgets[n].item(row, 3).text())
                except:
                    row_dict["serial"] = ""
                try:
                    row_dict["network_interface"] = str(hardware_attack_widgets[n].item(row, 4).text())
                except:
                    row_dict["network_interface"] = ""
                try:
                    row_dict["ip_address"] = str(hardware_attack_widgets[n].item(row, 5).text())
                except:
                    row_dict["ip_address"] = ""
                try:
                    row_dict["daughterboard"] = str(hardware_attack_widgets[n].item(row, 6).text())
                except:
                    row_dict["daughterboard"] = ""
                attack_dict[row] = row_dict
            sensor_dict["attack"] = attack_dict

            # IQ
            iq_dict = {}
            for row in range(hardware_iq_widgets[n].rowCount()):
                row_dict = {}
                try:
                    row_dict["type"] = str(hardware_iq_widgets[n].item(row, 0).text())
                except:
                    row_dict["type"] = ""
                try:
                    row_dict["uid"] = str(hardware_iq_widgets[n].item(row, 1).text())
                except:
                    row_dict["uid"] = ""
                try:
                    row_dict["radio_name"] = str(hardware_iq_widgets[n].item(row, 2).text())
                except:
                    row_dict["radio_name"] = ""
                try:
                    row_dict["serial"] = str(hardware_iq_widgets[n].item(row, 3).text())
                except:
                    row_dict["serial"] = ""
                try:
                    row_dict["network_interface"] = str(hardware_iq_widgets[n].item(row, 4).text())
                except:
                    row_dict["network_interface"] = ""
                try:
                    row_dict["ip_address"] = str(hardware_iq_widgets[n].item(row, 5).text())
                except:
                    row_dict["ip_address"] = ""
                try:
                    row_dict["daughterboard"] = str(hardware_iq_widgets[n].item(row, 6).text())
                except:
                    row_dict["daughterboard"] = ""
                iq_dict[row] = row_dict
            sensor_dict["iq"] = iq_dict

            # Archive
            archive_dict = {}
            for row in range(hardware_archive_widgets[n].rowCount()):
                row_dict = {}
                try:
                    row_dict["type"] = str(hardware_archive_widgets[n].item(row, 0).text())
                except:
                    row_dict["type"] = ""
                try:
                    row_dict["uid"] = str(hardware_archive_widgets[n].item(row, 1).text())
                except:
                    row_dict["uid"] = ""
                try:
                    row_dict["radio_name"] = str(hardware_archive_widgets[n].item(row, 2).text())
                except:
                    row_dict["radio_name"] = ""
                try:
                    row_dict["serial"] = str(hardware_archive_widgets[n].item(row, 3).text())
                except:
                    row_dict["serial"] = ""
                try:
                    row_dict["network_interface"] = str(hardware_archive_widgets[n].item(row, 4).text())
                except:
                    row_dict["network_interface"] = ""
                try:
                    row_dict["ip_address"] = str(hardware_archive_widgets[n].item(row, 5).text())
                except:
                    row_dict["ip_address"] = ""
                try:
                    row_dict["daughterboard"] = str(hardware_archive_widgets[n].item(row, 6).text())
                except:
                    row_dict["daughterboard"] = ""
                archive_dict[row] = row_dict
            sensor_dict["archive"] = archive_dict

            # Save Sensor Node
            settings_dict["Sensor Node " + str(n + 1)] = sensor_dict

        # Dump Dictionary to File
        stream = open(get_path, "w")
        yaml.dump(settings_dict, stream, default_flow_style=False, indent=5)


@qasync.asyncSlot(QtCore.QObject)
async def guess(HWSelect: QtCore.QObject):
    """
    Cycles through possible values for the selected row in the scan results table.
    """
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    scan_results_table = getattr(HWSelect, f"tableWidget_scan_results_{tab_index+1}")
    get_row = scan_results_table.currentRow()
    get_row_text = []
    for n in range(0, scan_results_table.columnCount()):
        get_row_text.append(str(scan_results_table.item(get_row, n).text()))

    # Send Message for HIPRFISR to Sensor Node Connections
    local_remote_stacked_widget = getattr(HWSelect, f"stackedWidget_local_remote_{tab_index+1}")
    if local_remote_stacked_widget.currentIndex() == 2:
        await HWSelect.dashboard.backend.guessHardware(str(tab_index), get_row, get_row_text, HWSelect.guess_index)
    elif local_remote_stacked_widget.currentIndex() == 4:
        await HWSelect.dashboard.backend.guessHardwareLT(str(tab_index), get_row, get_row_text, HWSelect.guess_index)


@qasync.asyncSlot(QtCore.QObject)
async def probe(HWSelect: QtCore.QObject):
    """
    Probes the selected radio in the scan results table.
    """
    # Row Number and Text
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    scan_results_table = getattr(HWSelect, f"tableWidget_scan_results_{tab_index+1}")
    get_row = scan_results_table.currentRow()
    get_row_text = []
    for n in range(0, scan_results_table.columnCount()):
        get_row_text.append(str(scan_results_table.item(get_row, n).text()))

    # Show Label
    scan_results_label =  getattr(HWSelect, f"label2_scan_results_probe_{tab_index+1}")
    scan_results_label.setVisible(True)

    # Disable Probe Button
    probe_button = getattr(HWSelect, f"pushButton_scan_results_probe_{tab_index+1}")

    # Send Message for HIPRFISR to Sensor Node Connections
    local_remote_stacked_widget = getattr(HWSelect, f"stackedWidget_local_remote_{tab_index+1}")

    if local_remote_stacked_widget.currentIndex() == 2:
        probe_button.setEnabled(False)
        await HWSelect.dashboard.backend.probeHardware(str(tab_index), get_row_text)
    elif local_remote_stacked_widget.currentIndex() == 4:
        await HWSelect.dashboard.backend.probeHardwareLT(str(tab_index), get_row_text)


@qasync.asyncSlot(QtCore.QObject)
async def scan(HWSelect: QtCore.QObject):
    """
    Performs a mass hardware scan on the local/remote sensor node and returns the results.
    """
    # Save Checked Items in Current Tab
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    list_widget = getattr(HWSelect, f"listWidget_scan_{tab_index+1}")
    hardware_list = []
    for n in range(0, list_widget.count()):
        if list_widget.item(n).checkState() == QtCore.Qt.Checked:
            hardware_list.append(str(list_widget.item(n).text()))

    # Send Message for HIPRFISR to Sensor Node Connections
    local_remote_stacked_widget = getattr(HWSelect, f"stackedWidget_local_remote_{tab_index+1}")
    if local_remote_stacked_widget.currentIndex() == 2:
        await HWSelect.dashboard.backend.scanHardware(str(tab_index), hardware_list)
    elif local_remote_stacked_widget.currentIndex() == 4:
        await HWSelect.dashboard.backend.scanHardwareLT(str(tab_index), hardware_list)


@QtCore.pyqtSlot(QtCore.QObject)
def tsi(HWSelect: QtCore.QObject, add_to_all=False):
    """
    Adds the selected row in the scan results table to the TSI table.
    """
    # Copy Scan Result to TSI Table
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    scan_results_widget = getattr(HWSelect, f"tableWidget_scan_results_{tab_index+1}")
    hardware_tsi_widget = getattr(HWSelect, f"tableWidget_tsi_{tab_index+1}")
    hardware_tabs_widget = getattr(HWSelect, f"tabWidget_hardware_{tab_index+1}")
    get_row = scan_results_widget.currentRow()
    hardware_id_present = HWSelect.hardwareID_Present(scan_results_widget, get_row)

    if hardware_id_present:
        hardware_tsi_widget.setRowCount(hardware_tsi_widget.rowCount() + 1)
        for col in range(0, scan_results_widget.columnCount()):
            if scan_results_widget.item(get_row, col) is not None:
                table_item = QtWidgets.QTableWidgetItem(str(scan_results_widget.item(get_row, col).text()))
                table_item.setTextAlignment(QtCore.Qt.AlignCenter)
                hardware_tsi_widget.setItem(hardware_tsi_widget.rowCount() - 1, col, table_item)
        hardware_tsi_widget.resizeColumnsToContents()
        hardware_tsi_widget.resizeRowsToContents()
        hardware_tsi_widget.horizontalHeader().setStretchLastSection(False)
        hardware_tsi_widget.horizontalHeader().setStretchLastSection(True)
        hardware_tabs_widget.setCurrentIndex(0)
    else:
        # Provide Warning
        if add_to_all == False:
            fissure.Dashboard.UI_Components.Qt5.errorMessage("Provide hardware ID in scan results table.")


@QtCore.pyqtSlot(QtCore.QObject)
def pd(HWSelect: QtCore.QObject, add_to_all=False):
    """
    Adds the selected row in the scan results table to the PD table.
    """
    # Copy Scan Result to PD Table
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    scan_results_widget = getattr(HWSelect, f"tableWidget_scan_results_{tab_index+1}")
    hardware_pd_widget = getattr(HWSelect, f"tableWidget_pd_{tab_index+1}")
    hardware_tabs_widget = getattr(HWSelect, f"tabWidget_hardware_{tab_index+1}")
    get_row = scan_results_widget.currentRow()
    hardware_id_present = HWSelect.hardwareID_Present(scan_results_widget, get_row)

    if hardware_id_present:
        hardware_pd_widget.setRowCount(hardware_pd_widget.rowCount() + 1)
        for col in range(0, scan_results_widget.columnCount()):
            if scan_results_widget.item(get_row, col) is not None:
                table_item = QtWidgets.QTableWidgetItem(str(scan_results_widget.item(get_row, col).text()))
                table_item.setTextAlignment(QtCore.Qt.AlignCenter)
                hardware_pd_widget.setItem(hardware_pd_widget.rowCount() - 1, col, table_item)
        hardware_pd_widget.resizeColumnsToContents()
        hardware_pd_widget.resizeRowsToContents()
        hardware_pd_widget.horizontalHeader().setStretchLastSection(False)
        hardware_pd_widget.horizontalHeader().setStretchLastSection(True)
        hardware_tabs_widget.setCurrentIndex(1)
    else:
        # Provide Warning
        if add_to_all == False:
            fissure.Dashboard.UI_Components.Qt5.errorMessage("Provide hardware ID in scan results table.")


@QtCore.pyqtSlot(QtCore.QObject)
def attack(HWSelect: QtCore.QObject, add_to_all=False):
    """
    Adds the selected row in the scan results table to the Attack table.
    """
    # Copy Scan Result to Attack Table
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    scan_results_widget = getattr(HWSelect, f"tableWidget_scan_results_{tab_index+1}")
    hardware_attack_widget = getattr(HWSelect, f"tableWidget_attack_{tab_index+1}")
    hardware_tabs_widget = getattr(HWSelect, f"tabWidget_hardware_{tab_index+1}")
    get_row = scan_results_widget.currentRow()
    hardware_id_present = HWSelect.hardwareID_Present(scan_results_widget, get_row)

    if hardware_id_present:
        hardware_attack_widget.setRowCount(hardware_attack_widget.rowCount() + 1)
        for col in range(0, scan_results_widget.columnCount()):
            if scan_results_widget.item(get_row, col) is not None:
                table_item = QtWidgets.QTableWidgetItem(str(scan_results_widget.item(get_row, col).text()))
                table_item.setTextAlignment(QtCore.Qt.AlignCenter)
                hardware_attack_widget.setItem(
                    hardware_attack_widget.rowCount() - 1, col, table_item
                )
        hardware_attack_widget.resizeColumnsToContents()
        hardware_attack_widget.resizeRowsToContents()
        hardware_attack_widget.horizontalHeader().setStretchLastSection(False)
        hardware_attack_widget.horizontalHeader().setStretchLastSection(True)
        hardware_tabs_widget.setCurrentIndex(2)
    else:
        # Provide Warning
        if add_to_all == False:
            fissure.Dashboard.UI_Components.Qt5.errorMessage("Provide hardware ID in scan results table.")


@QtCore.pyqtSlot(QtCore.QObject)
def iq(HWSelect: QtCore.QObject, add_to_all=False):
    """
    Adds the selected row in the scan results table to the IQ table.
    """
    # Copy Scan Result to IQ Table
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    scan_results_widget = getattr(HWSelect, f"tableWidget_scan_results_{tab_index+1}")
    hardware_iq_widget = getattr(HWSelect, f"tableWidget_iq_{tab_index+1}")
    hardware_tabs_widget = getattr(HWSelect, f"tabWidget_hardware_{tab_index+1}")
    get_row = scan_results_widget.currentRow()
    hardware_id_present = HWSelect.hardwareID_Present(scan_results_widget, get_row)

    if hardware_id_present:
        hardware_iq_widget.setRowCount(hardware_iq_widget.rowCount() + 1)
        for col in range(0, scan_results_widget.columnCount()):
            if scan_results_widget.item(get_row, col) is not None:
                table_item = QtWidgets.QTableWidgetItem(str(scan_results_widget.item(get_row, col).text()))
                table_item.setTextAlignment(QtCore.Qt.AlignCenter)
                hardware_iq_widget.setItem(hardware_iq_widget.rowCount() - 1, col, table_item)
        hardware_iq_widget.resizeColumnsToContents()
        hardware_iq_widget.resizeRowsToContents()
        hardware_iq_widget.horizontalHeader().setStretchLastSection(False)
        hardware_iq_widget.horizontalHeader().setStretchLastSection(True)
        hardware_tabs_widget.setCurrentIndex(3)
    else:
        # Provide Warning
        if add_to_all == False:
            fissure.Dashboard.UI_Components.Qt5.errorMessage("Provide hardware ID in scan results table.")


@QtCore.pyqtSlot(QtCore.QObject)
def archive(HWSelect: QtCore.QObject, add_to_all=False):
    """
    Adds the selected row in the scan results table to the Archive table.
    """
    # Copy Scan Result to Archive Table
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    scan_results_widget = getattr(HWSelect, f"tableWidget_scan_results_{tab_index+1}")
    hardware_archive_widget = getattr(HWSelect, f"tableWidget_archive_{tab_index+1}")
    hardware_tabs_widget = getattr(HWSelect, f"tabWidget_hardware_{tab_index+1}")
    get_row = scan_results_widget.currentRow()
    hardware_id_present = HWSelect.hardwareID_Present(scan_results_widget, get_row)

    if hardware_id_present:
        hardware_archive_widget.setRowCount(hardware_archive_widget.rowCount() + 1)
        for col in range(0, scan_results_widget.columnCount()):
            if scan_results_widget.item(get_row, col) is not None:
                table_item = QtWidgets.QTableWidgetItem(str(scan_results_widget.item(get_row, col).text()))
                table_item.setTextAlignment(QtCore.Qt.AlignCenter)
                hardware_archive_widget.setItem(
                    hardware_archive_widget.rowCount() - 1, col, table_item
                )
        hardware_archive_widget.resizeColumnsToContents()
        hardware_archive_widget.resizeRowsToContents()
        hardware_archive_widget.horizontalHeader().setStretchLastSection(False)
        hardware_archive_widget.horizontalHeader().setStretchLastSection(True)
        hardware_tabs_widget.setCurrentIndex(4)
    else:
        # Provide Warning
        if add_to_all == False:
            fissure.Dashboard.UI_Components.Qt5.errorMessage("Provide hardware ID in scan results table.")


@QtCore.pyqtSlot(QtCore.QObject)
def remove_tsi(HWSelect: QtCore.QObject):
    """
    Removes a row from the TSI table.
    """
    # Remove Row
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    tsi_table = getattr(HWSelect, f"tableWidget_tsi_{tab_index+1}")
    get_row = tsi_table.currentRow()
    tsi_table.removeRow(get_row)
    if get_row == tsi_table.rowCount():
        tsi_table.setCurrentCell(tsi_table.rowCount() - 1, 0)
    elif get_row >= 0:
        tsi_table.setCurrentCell(get_row, 0)


@QtCore.pyqtSlot(QtCore.QObject)
def remove_pd(HWSelect: QtCore.QObject):
    """
    Removes a row from the PD table.
    """
    # Remove Row
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    pd_table = getattr(HWSelect, f"tableWidget_pd_{tab_index+1}")
    get_row = pd_table.currentRow()
    pd_table.removeRow(get_row)
    if get_row == pd_table.rowCount():
        pd_table.setCurrentCell(pd_table.rowCount() - 1, 0)
    elif get_row >= 0:
        pd_table.setCurrentCell(get_row, 0)


@QtCore.pyqtSlot(QtCore.QObject)
def remove_attack(HWSelect: QtCore.QObject):
    """
    Removes a row from the Attack table.
    """
    # Remove Row
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    attack_table = getattr(HWSelect, f"tableWidget_attack_{tab_index+1}")
    get_row = attack_table.currentRow()
    attack_table.removeRow(get_row)
    if get_row == attack_table.rowCount():
        attack_table.setCurrentCell(attack_table.rowCount() - 1, 0)
    elif get_row >= 0:
        attack_table.setCurrentCell(get_row, 0)


@QtCore.pyqtSlot(QtCore.QObject)
def remove_iq(HWSelect: QtCore.QObject):
    """
    Removes a row from the IQ table.
    """
    # Remove Row
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    iq_table = getattr(HWSelect, f"tableWidget_iq_{tab_index+1}")
    get_row = iq_table.currentRow()
    iq_table.removeRow(get_row)
    if get_row == iq_table.rowCount():
        iq_table.setCurrentCell(iq_table.rowCount() - 1, 0)
    elif get_row >= 0:
        iq_table.setCurrentCell(get_row, 0)


@QtCore.pyqtSlot(QtCore.QObject)
def remove_archive(HWSelect: QtCore.QObject):
    """
    Removes a row from the Archive table.
    """
    # Remove Row
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    archive_table = getattr(HWSelect, f"tableWidget_archive_{tab_index+1}")
    get_row = archive_table.currentRow()
    archive_table.removeRow(get_row)
    if get_row == archive_table.rowCount():
        archive_table.setCurrentCell(archive_table.rowCount() - 1, 0)
    elif get_row >= 0:
        archive_table.setCurrentCell(get_row, 0)


@QtCore.pyqtSlot(QtCore.QObject)
def add_to_all(HWSelect: QtCore.QObject):
    """
    Adds the selected row in the scan results table to all the tables.
    """
    tsi(HWSelect, True)
    pd(HWSelect, True)
    attack(HWSelect, True)
    iq(HWSelect, True)
    archive(HWSelect, False)  # False allows error message to show up once


@QtCore.pyqtSlot(QtCore.QObject)
def rows_to_all(HWSelect: QtCore.QObject):
    """
    Adds the all the rows in the scan results table to all the tables.
    """
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    scan_results_widgets = [
        HWSelect.tableWidget_scan_results_1,
        HWSelect.tableWidget_scan_results_2,
        HWSelect.tableWidget_scan_results_3,
        HWSelect.tableWidget_scan_results_4,
        HWSelect.tableWidget_scan_results_5,
    ]

    scan_results_table = scan_results_widgets[tab_index]
    total_rows = scan_results_table.rowCount()

    for row in range(total_rows):
        scan_results_widgets[tab_index].setCurrentCell(row,0)  # Set the current row to simulate selection
        tsi(HWSelect, True)
        pd(HWSelect, True)
        attack(HWSelect, True)
        iq(HWSelect, True)
        archive(HWSelect, False)  # False allows error message to show up once


@QtCore.pyqtSlot(QtCore.QObject)
def scan_results_remove(HWSelect: QtCore.QObject):
    """
    Removes a row from the scan results table.
    """
    # Get current tab index (0-based, so add 1 to match widget numbering)
    tab_index = HWSelect.tabWidget_nodes.currentIndex() + 1

    # Dynamically retrieve widgets
    get_tableWidget = getattr(HWSelect, f"tableWidget_scan_results_{tab_index}")
    get_line3_scan_results = getattr(HWSelect, f"line3_scan_results_{tab_index}")

    # List of push button names
    button_names = [
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
    
    # Dynamically retrieve all relevant push buttons
    push_buttons = [getattr(HWSelect, f"{name}_{tab_index}") for name in button_names]

    # Remove the selected row
    get_row = get_tableWidget.currentRow()
    get_tableWidget.removeRow(get_row)

    # Select a new row after deletion
    if get_tableWidget.rowCount() > 0:
        new_row = min(get_row, get_tableWidget.rowCount() - 1)  # Ensure valid row index
        get_tableWidget.setCurrentCell(new_row, 0)

    # Disable buttons if table is empty
    if get_tableWidget.rowCount() == 0:
        for btn in push_buttons:
            btn.setEnabled(False)
        get_tableWidget.setEnabled(False)
        get_line3_scan_results.setEnabled(False)


@QtCore.pyqtSlot(QtCore.QObject)
def scan_results_remove_all(HWSelect: QtCore.QObject):
    """
    Removes all rows from the scan results table.
    """
    # Get current tab index (0-based, so add 1 to match widget numbering)
    tab_index = HWSelect.tabWidget_nodes.currentIndex() + 1

    # Dynamically retrieve table and line widget
    get_tableWidget = getattr(HWSelect, f"tableWidget_scan_results_{tab_index}")
    get_line3_scan_results = getattr(HWSelect, f"line3_scan_results_{tab_index}")

    # List of push button names
    button_names = [
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

    # Dynamically retrieve all relevant push buttons
    push_buttons = [getattr(HWSelect, f"{name}_{tab_index}") for name in button_names]

    # Remove all rows
    get_tableWidget.setRowCount(0)

    # Disable buttons when table is empty
    for btn in push_buttons:
        btn.setEnabled(False)
    
    get_tableWidget.setEnabled(False)
    get_line3_scan_results.setEnabled(False)


@QtCore.pyqtSlot(QtCore.QObject)
def manual(HWSelect: QtCore.QObject):
    """
    Manually adds the checked hardware to the scan results table.
    """
        # Retrieve current tab index (0-based)
    tab_index = HWSelect.tabWidget_nodes.currentIndex() + 1  # +1 to match widget numbering

    # Dynamically retrieve widgets based on tab index
    get_listWidget = getattr(HWSelect, f"listWidget_scan_{tab_index}")
    get_tableWidget = getattr(HWSelect, f"tableWidget_scan_results_{tab_index}")
    get_line3_scan_results = getattr(HWSelect, f"line3_scan_results_{tab_index}")

    # Get all relevant push buttons
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
    get_pushButtons = [getattr(HWSelect, f"{name}_{tab_index}") for name in push_button_names]

    # Fill Scan Results Table with Checked Items
    for n in range(get_listWidget.count()):
        if get_listWidget.item(n).checkState() == QtCore.Qt.Checked:
            rows = get_tableWidget.rowCount()
            get_tableWidget.setRowCount(rows + 1)
            table_item = QtWidgets.QTableWidgetItem(str(get_listWidget.item(n).text()))
            table_item.setTextAlignment(QtCore.Qt.AlignCenter)
            get_tableWidget.setItem(rows, 0, table_item)
            
            for m in range(1, get_tableWidget.columnCount()):
                empty_table_item = QtWidgets.QTableWidgetItem("")
                empty_table_item.setTextAlignment(QtCore.Qt.AlignCenter)
                get_tableWidget.setItem(rows, m, empty_table_item)

            HWSelect.highlight_hardware_id(get_tableWidget, rows)

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
        get_tableWidget.setEnabled(True)
        get_line3_scan_results.setEnabled(True)


@QtCore.pyqtSlot(QtCore.QObject)
def more(HWSelect: QtCore.QObject):
    """ 
    Moves the sensor node details stacked widget to the next page.
    """
    # Move Page to the Right
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    stacked_widgets = [HWSelect.stackedWidget_details_1, HWSelect.stackedWidget_details_2, HWSelect.stackedWidget_details_3, HWSelect.stackedWidget_details_4, HWSelect.stackedWidget_details_5]
    new_index = stacked_widgets[tab_index].currentIndex() + 1
    get_count = stacked_widgets[tab_index].count()

    if new_index >= get_count:
        stacked_widgets[tab_index].setCurrentIndex(0)
    else:
        stacked_widgets[tab_index].setCurrentIndex(new_index)


@QtCore.pyqtSlot(QtCore.QObject)
def local(HWSelect: QtCore.QObject, tab_index=0):
    """
    Switch to the Local Sensor Node configuration page dynamically.
    """
    # Radio button will only be enabled when disconnected
    HWSelect.sensorNodeDisconnected(tab_index)
        

@QtCore.pyqtSlot(QtCore.QObject)
def remote(HWSelect: QtCore.QObject, tab_index=0):
    """
    Switch to the Remote Sensor Node configuration page dynamically.
    """
    # Radio button will only be enabled when disconnected
    HWSelect.sensorNodeDisconnected(tab_index)


@qasync.asyncSlot(QtCore.QObject)
async def launch(HWSelect: QtCore.QObject):
    """ 
    Launches and then connects to a local sensor node.
    """
    # Get Widgets and Values
    tab_index = HWSelect.tabWidget_nodes.currentIndex()

    recall_settings_widgets = getattr(HWSelect, f"checkBox_recall_settings_local_{tab_index+1}")
    launch_widgets = getattr(HWSelect, f"pushButton_launch_{tab_index+1}")
    local_button = getattr(HWSelect, f"radioButton_local_{tab_index+1}")
    remote_button = getattr(HWSelect, f"radioButton_remote_{tab_index+1}")
    
    # Disable Buttons
    launch_widgets.setEnabled(False)
    recall_settings_widgets.setEnabled(False)
    local_button.setEnabled(False)
    remote_button.setEnabled(False)
    QtWidgets.QApplication.processEvents()
    
    # Connect
    os.system('python3 "' + os.path.join(fissure.utils.SENSOR_NODE_DIR, "SensorNode.py") + '" --local &')
    HWSelect.dashboard.logger.info("Launching local sensor node, please wait...")
 
    # Set the New Connection Flag for the Warning on Cancel
    HWSelect.new_local_connection[tab_index] = True


@qasync.asyncSlot(QtCore.QObject)
async def ping(HWSelect: QtCore.QObject):
    """
    Send command to HiprFisr to ping the host running the Sensor Node and await response.
    # TODO: ping needs to happen at HIPRFISR
    """
    # Ping IP Address
    if HWSelect.tabWidget_nodes.currentIndex() == 0:
        get_ip = str(HWSelect.textEdit_ip_addr_1.toPlainText())
    elif HWSelect.tabWidget_nodes.currentIndex() == 1:
        get_ip = str(HWSelect.textEdit_ip_addr_2.toPlainText())
    elif HWSelect.tabWidget_nodes.currentIndex() == 2:
        get_ip = str(HWSelect.textEdit_ip_addr_3.toPlainText())
    elif HWSelect.tabWidget_nodes.currentIndex() == 3:
        get_ip = str(HWSelect.textEdit_ip_addr_4.toPlainText())
    elif HWSelect.tabWidget_nodes.currentIndex() == 4:
        get_ip = str(HWSelect.textEdit_ip_addr_5.toPlainText())
    response = os.system("ping -c 1 " + get_ip)
    if response == 0:
        HWSelect.dashboard.logger.info(get_ip + " is up!")
        ret = await fissure.Dashboard.UI_Components.Qt5.async_ok_dialog(HWSelect, get_ip + " is up!")
    else:
        HWSelect.dashboard.logger.info(get_ip + " is down!")
        ret = await fissure.Dashboard.UI_Components.Qt5.async_ok_dialog(HWSelect, get_ip + " is down!")


@qasync.asyncSlot(QtCore.QObject)
async def connect(HWSelect: QtCore.QObject):
    """
    Connects to the remote sensor node using the IP address and ports.
    """
    # Connect
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    ip_widget = getattr(HWSelect, f"textEdit_ip_addr_{tab_index+1}")
    msg_port_widget = getattr(HWSelect, f"textEdit_msg_port_{tab_index+1}")
    hb_port_widget = getattr(HWSelect, f"textEdit_hb_port_{tab_index+1}")
    recall_settings_widget = getattr(HWSelect, f"checkBox_recall_settings_remote_{tab_index+1}")
    connect_widget = getattr(HWSelect, f"pushButton_connect_{tab_index+1}")
    network_type_widget = getattr(HWSelect, f"comboBox_network_type_{tab_index+1}")

    get_ip = str(ip_widget.toPlainText())
    get_msg_port = str(msg_port_widget.toPlainText())
    get_hb_port = str(hb_port_widget.toPlainText())
    get_recall_settings = str(recall_settings_widget.isChecked())

    # Check Existing IPs
    get_sensor_node = ["sensor_node1", "sensor_node2", "sensor_node3", "sensor_node4", "sensor_node5"]
    for n in range(0, len(get_sensor_node)):
        if (get_ip == HWSelect.dashboard.backend.settings[get_sensor_node[n]]["ip_address"]) and (n != tab_index):
            ret = await fissure.Dashboard.UI_Components.Qt5.async_ok_dialog(HWSelect, "IP address already in use.")
            return

    # Disable Buttons
    connect_widget.setEnabled(False)
    recall_settings_widget.setEnabled(False)
    ip_widget.setEnabled(False)
    msg_port_widget.setEnabled(False)
    hb_port_widget.setEnabled(False)
    network_type_widget.setEnabled(False)
    QtWidgets.QApplication.processEvents()

    # Send Message for HIPRFISR to Sensor Node Connections
    await HWSelect.dashboard.backend.connect_remote_sensor_node(str(tab_index), get_ip, get_msg_port, get_hb_port, get_recall_settings)


@qasync.asyncSlot(QtCore.QObject)
async def disconnect(HWSelect: QtCore.QObject, delete_node=False):
    """
    Send command to HiprFisr to disconnect from remote Sensor Node
    OR
    Disconnect/Shutdown local Sensor Node
    """
    # Disconnect
    tab_index = HWSelect.tabWidget_nodes.currentIndex()

    stacked_widgets = getattr(HWSelect, f"stackedWidget_local_remote_{tab_index+1}")
    network_type_widgets = getattr(HWSelect, f"comboBox_network_type_{tab_index+1}")
    local_button = getattr(HWSelect, f"radioButton_local_{tab_index+1}")
    remote_button = getattr(HWSelect, f"radioButton_remote_{tab_index+1}")

    if (str(network_type_widgets.currentText()) == "IP") or local_button.isChecked():

        local_button.setEnabled(False)        
        remote_button.setEnabled(False)        

        stacked_widgets.setCurrentIndex(6)

        # Send Message for HIPRFISR to Sensor Node Connections
        await HWSelect.dashboard.backend.disconnect_remote_sensor_node(str(tab_index), delete_node)

    elif str(network_type_widgets.currentText()) == "Meshtastic":

        stacked_widgets.setCurrentIndex(3)

        # Send Message for HIPRFISR to end Meshtastic Serial Connection
        await HWSelect.dashboard.backend.disconnectFromMeshtastic(str(tab_index))


@QtCore.pyqtSlot(QtCore.QObject)
def remove_all(HWSelect: QtCore.QObject):
    """
    Removes all rows from the Default Hardware Assignments tables.
    """
    node_idx = HWSelect.tabWidget_nodes.currentIndex() + 1
    HWSelect.dashboard.logger.debug(f"removing all table rows from 'Sensor Node {node_idx}")
    for table in ["tsi", "pd", "attack", "iq", "archive"]:
        table_widget = getattr(HWSelect, f"tableWidget_{table}_{node_idx}")
        table_widget.setRowCount(0)


@QtCore.pyqtSlot(QtCore.QObject)
def apply(HWSelect: QtCore.QObject):
    """
    Save Sensor Node chages and close the window
    """
    button: QtWidgets.QPushButton = HWSelect.pushButton_apply
    button.setCheckable(True)

    button.setChecked(True)
    HWSelect.dashboard.logger.debug("[Apply] Clicked")
    time.sleep(0.5)
    button.setChecked(False)

    top_button_widgets = [
        HWSelect.dashboard.ui.pushButton_top_node1,
        HWSelect.dashboard.ui.pushButton_top_node2,
        HWSelect.dashboard.ui.pushButton_top_node3,
        HWSelect.dashboard.ui.pushButton_top_node4,
        HWSelect.dashboard.ui.pushButton_top_node5
        ]

    for node_idx in range(1, 6):
        if len(HWSelect.tabWidget_nodes.tabText(node_idx-1)) > 0:
            nickname_widget: QtWidgets.QTextEdit = getattr(HWSelect, f"textEdit_nickname_{node_idx}")
            location_widget: QtWidgets.QTextEdit = getattr(HWSelect, f"textEdit_location_{node_idx}")
            notes_widget: QtWidgets.QTextEdit = getattr(HWSelect, f"textEdit_notes_{node_idx}")
            ip_addr_widget: QtWidgets.QTextEdit = getattr(HWSelect, f"textEdit_ip_addr_{node_idx}")
            hb_port_widget: QtWidgets.QTextEdit = getattr(HWSelect, f"textEdit_hb_port_{node_idx}")
            msg_port_widget: QtWidgets.QTextEdit = getattr(HWSelect, f"textEdit_msg_port_{node_idx}")
            local_button: QtWidgets.QRadioButton = getattr(HWSelect, f"radioButton_local_{node_idx}")
            tsi_widget: QtWidgets.QTableWidget = getattr(HWSelect, f"tableWidget_tsi_{node_idx}")
            pd_widget: QtWidgets.QTableWidget = getattr(HWSelect, f"tableWidget_pd_{node_idx}")
            attack_widget: QtWidgets.QTableWidget = getattr(HWSelect, f"tableWidget_attack_{node_idx}")
            iq_widget: QtWidgets.QTableWidget = getattr(HWSelect, f"tableWidget_iq_{node_idx}")
            archive_widget: QtWidgets.QTableWidget = getattr(HWSelect, f"tableWidget_archive_{node_idx}")
            autorun_widget: QtWidgets.QLabel = getattr(HWSelect, f"label2_autorun_value_{node_idx}")
            autorun_delay_widget: QtWidgets.QLabel = getattr(HWSelect, f"label2_autorun_delay_value_{node_idx}")
            console_logging_level_widget: QtWidgets.QLabel = getattr(HWSelect, f"label2_console_logging_level_value_{node_idx}")
            file_logging_level_widget: QtWidgets.QLabel = getattr(HWSelect, f"label2_file_logging_level_value_{node_idx}")
            meshtastic_serial_port_widget: QtWidgets.QLabel = getattr(HWSelect, f"comboBox_meshtastic_port_{node_idx}")
            meshtastic_serial_baud_rate_widget: QtWidgets.QLabel = getattr(HWSelect, f"comboBox_meshtastic_baud_rate_{node_idx}")
            network_type_widget: QtWidgets.QLabel = getattr(HWSelect, f"comboBox_network_type_{node_idx}")
            
            # Check for Valid Values Before Saving
            if local_button.isChecked() is False:
                if len(nickname_widget.toPlainText()) == 0:
                    fissure.Dashboard.UI_Components.Qt5.errorMessage("Enter a nickname for the remote sensor node.")
                    return
                if len(ip_addr_widget.toPlainText()) == 0:
                    fissure.Dashboard.UI_Components.Qt5.errorMessage("Enter an IP Address for the remote sensor node.")
                    return
                if len(msg_port_widget.toPlainText()) == 0:
                    fissure.Dashboard.UI_Components.Qt5.errorMessage("Enter a message port for the remote sensor node.")
                    return
                if len(hb_port_widget.toPlainText()) == 0:
                    fissure.Dashboard.UI_Components.Qt5.errorMessage("Enter a heartbeat port for the remote sensor node.")
                    return
                get_msg_port = msg_port_widget.toPlainText()
                if not (get_msg_port.isdigit() and 1 <= int(get_msg_port) <= 65535):
                    fissure.Dashboard.UI_Components.Qt5.errorMessage("Enter a valid message port (1-65535).")
                    return
                get_hb_port = hb_port_widget.toPlainText()
                if not (get_hb_port.isdigit() and 1 <= int(get_hb_port) <= 65535):
                    fissure.Dashboard.UI_Components.Qt5.errorMessage("Enter a valid heartbeat port (1-65535).")
                    return

            # TSI Default Hardware Assignments
            columns = range(tsi_widget.columnCount())
            tsi_info = []
            for row in range(tsi_widget.rowCount()):
                row_text = []
                for column in columns:
                    try:
                        get_text = str(tsi_widget.item(row, column).text())
                    except:
                        get_text = ""
                    row_text.append(get_text)
                tsi_info.append(row_text)

            # PD Default Hardware Assignments
            columns = range(pd_widget.columnCount())
            pd_info = []
            for row in range(pd_widget.rowCount()):
                row_text = []
                for column in columns:
                    try:
                        get_text = str(pd_widget.item(row, column).text())
                    except:
                        get_text = ""
                    row_text.append(get_text)
                pd_info.append(row_text)

            # Attack Default Hardware Assignments
            columns = range(attack_widget.columnCount())
            attack_info = []
            for row in range(attack_widget.rowCount()):
                row_text = []
                for column in columns:
                    try:
                        get_text = str(attack_widget.item(row, column).text())
                    except:
                        get_text = ""
                    row_text.append(get_text)
                attack_info.append(row_text)

            # IQ Default Hardware Assignments
            columns = range(iq_widget.columnCount())
            iq_info = []
            for row in range(iq_widget.rowCount()):
                row_text = []
                for column in columns:
                    try:
                        get_text = str(iq_widget.item(row, column).text())
                    except:
                        get_text = ""
                    row_text.append(get_text)
                iq_info.append(row_text)

            # Archive Default Hardware Assignments
            columns = range(archive_widget.columnCount())
            archive_info = []
            for row in range(archive_widget.rowCount()):
                row_text = []
                for column in columns:
                    try:
                        get_text = str(archive_widget.item(row, column).text())
                    except:
                        get_text = ""
                    row_text.append(get_text)
                archive_info.append(row_text)

            node_config = {
                "nickname": nickname_widget.toPlainText(),
                "location": location_widget.toPlainText(),
                "notes": notes_widget.toPlainText(),
                "ip_address": ip_addr_widget.toPlainText(),
                "hb_port": hb_port_widget.toPlainText(),
                "msg_port": msg_port_widget.toPlainText(),
                "local_remote": "local" if local_button.isChecked() else "remote",
                "autorun": autorun_widget.text(),
                "autorun_delay_seconds": autorun_delay_widget.text(),
                "console_logging_level": console_logging_level_widget.text(),
                "file_logging_level": file_logging_level_widget.text(),
                "tsi": tsi_info,
                "pd": pd_info,
                "attack": attack_info,
                "iq": iq_info,
                "archive": archive_info,
                "meshtastic_serial_port": str(meshtastic_serial_port_widget.currentText()),
                "meshtastic_serial_baud_rate": str(meshtastic_serial_baud_rate_widget.currentText()),
                "network_type": str(network_type_widget.currentText())
            }

            HWSelect.dashboard.backend.settings.update({f"sensor_node{node_idx}": node_config})

            # Update Top Bar
            if local_button.isChecked() == True:
                top_button_widgets[node_idx-1].setText("Local Sensor Node")
            else:
                top_button_widgets[node_idx-1].setText(nickname_widget.toPlainText())
            top_button_widgets[node_idx-1].setVisible(True)

    # Enable the Next Top Button
    if len(HWSelect.dashboard.backend.settings['sensor_node2']['nickname']) == 0:
        HWSelect.dashboard.ui.pushButton_top_node2.setVisible(True)
        HWSelect.dashboard.ui.pushButton_top_node2.setText("New Sensor Node")
        HWSelect.dashboard.ui.pushButton_top_node3.setVisible(False)
        HWSelect.dashboard.ui.pushButton_top_node4.setVisible(False)
        HWSelect.dashboard.ui.pushButton_top_node5.setVisible(False)        
        fissure.Dashboard.Slots.TopBarSlots.sensor_node_rightClick(HWSelect.dashboard, node_idx=0)
    elif len(HWSelect.dashboard.backend.settings['sensor_node3']['nickname']) == 0:
        HWSelect.dashboard.ui.pushButton_top_node3.setVisible(True)
        HWSelect.dashboard.ui.pushButton_top_node3.setText("New Sensor Node")
        HWSelect.dashboard.ui.pushButton_top_node4.setVisible(False)
        HWSelect.dashboard.ui.pushButton_top_node5.setVisible(False)
    elif len(HWSelect.dashboard.backend.settings['sensor_node4']['nickname']) == 0:
        HWSelect.dashboard.ui.pushButton_top_node4.setVisible(True)
        HWSelect.dashboard.ui.pushButton_top_node4.setText("New Sensor Node")
        HWSelect.dashboard.ui.pushButton_top_node5.setVisible(False)          
    elif len(HWSelect.dashboard.backend.settings['sensor_node5']['nickname']) == 0:
        HWSelect.dashboard.ui.pushButton_top_node5.setVisible(True)
        HWSelect.dashboard.ui.pushButton_top_node5.setText("New Sensor Node")

    # Close Window
    HWSelect.accept()


@QtCore.pyqtSlot(QtCore.QObject)
def delete(HWSelect: QtCore.QObject):
    """
    Deletes all saved Sensor Node info for the current tab
    """
    # button: QtWidgets.QPushButton = HWSelect.pushButton_delete
    # button.setCheckable(True)

    # button.setChecked(True)
    # HWSelect.dashboard.logger.info("[Delete] Clicked")
    # time.sleep(0.5)
    # button.setChecked(False)

    # node_idx = HWSelect.tabWidget_nodes.currentIndex() + 1
    # stacked_widget: QtWidgets.QStackedWidget = getattr(HWSelect, f"stackedWidget_local_remote_{node_idx}")
    # sensor_node_settings: dict = HWSelect.dashboard.backend.settings.get("sensor_nodes")

    # # Disconnect first if currently connected
    # if stacked_widget.currentIndex() == 2:
    #     # disconnect_node()
    #     pass

    # # Shift Sensor Node entries
    # for idx in range(node_idx, 5):
    #     sensor_node_settings[f"sensor_node{idx}"] = sensor_node_settings.get(f"sensor_node{idx+1}")

    # sensor_node_settings["sensor_node5"] = {}

    # # Store updated settings
    # HWSelect.dashboard.backend.settings.update({"sensor_nodes": sensor_node_settings})

    # Yes/No Dialog
    qm = QtWidgets.QMessageBox
    ret = qm.question(
        HWSelect,
        "",
        "Delete all saved information for this sensor node?"
        "Any outstanding changes to other sensor nodes will not be saved.",
        qm.Yes | qm.No,
    )
    if ret == qm.Yes:
        # Shift Nodes to the Left
        deleted_node = HWSelect.tabWidget_nodes.currentIndex()

        # Disconnect
        stacked_widgets = [
            HWSelect.stackedWidget_local_remote_1,
            HWSelect.stackedWidget_local_remote_2,
            HWSelect.stackedWidget_local_remote_3,
            HWSelect.stackedWidget_local_remote_4,
            HWSelect.stackedWidget_local_remote_5,
        ]
        if stacked_widgets[deleted_node].currentIndex() == 2:
            disconnect(HWSelect, True)  # Disconnect

        if deleted_node == 0:
            HWSelect.dashboard.backend.settings["sensor_node1"]["local_remote"] = (
                HWSelect.dashboard.backend.settings["sensor_node2"]["local_remote"]
            )
            HWSelect.dashboard.backend.settings["sensor_node1"]["nickname"] = (
                HWSelect.dashboard.backend.settings["sensor_node2"]["nickname"]
            )
            HWSelect.dashboard.backend.settings["sensor_node1"]["location"] = (
                HWSelect.dashboard.backend.settings["sensor_node2"]["location"]
            )
            HWSelect.dashboard.backend.settings["sensor_node1"]["notes"] = (
                HWSelect.dashboard.backend.settings["sensor_node2"]["notes"]
            )
            HWSelect.dashboard.backend.settings["sensor_node1"]["ip_address"] = (
                HWSelect.dashboard.backend.settings["sensor_node2"]["ip_address"]
            )
            HWSelect.dashboard.backend.settings["sensor_node1"]["msg_port"] = (
                HWSelect.dashboard.backend.settings["sensor_node2"]["msg_port"]
            )
            HWSelect.dashboard.backend.settings["sensor_node1"]["hb_port"] = (
                HWSelect.dashboard.backend.settings["sensor_node2"]["hb_port"]
            )
            HWSelect.dashboard.backend.settings["sensor_node1"]["tsi"] = (
                HWSelect.dashboard.backend.settings["sensor_node2"]["tsi"]
            )
            HWSelect.dashboard.backend.settings["sensor_node1"]["pd"] = (
                HWSelect.dashboard.backend.settings["sensor_node2"]["pd"]
            )
            HWSelect.dashboard.backend.settings["sensor_node1"]["attack"] = (
                HWSelect.dashboard.backend.settings["sensor_node2"]["attack"]
            )
            HWSelect.dashboard.backend.settings["sensor_node1"]["iq"] = (
                HWSelect.dashboard.backend.settings["sensor_node2"]["iq"]
            )
            HWSelect.dashboard.backend.settings["sensor_node1"]["archive"] = (
                HWSelect.dashboard.backend.settings["sensor_node2"]["archive"]
            )

        if deleted_node <= 1:
            HWSelect.dashboard.backend.settings["sensor_node2"]["local_remote"] = (
                HWSelect.dashboard.backend.settings["sensor_node3"]["local_remote"]
            )
            HWSelect.dashboard.backend.settings["sensor_node2"]["nickname"] = (
                HWSelect.dashboard.backend.settings["sensor_node3"]["nickname"]
            )
            HWSelect.dashboard.backend.settings["sensor_node2"]["location"] = (
                HWSelect.dashboard.backend.settings["sensor_node3"]["location"]
            )
            HWSelect.dashboard.backend.settings["sensor_node2"]["notes"] = (
                HWSelect.dashboard.backend.settings["sensor_node3"]["notes"]
            )
            HWSelect.dashboard.backend.settings["sensor_node2"]["ip_address"] = (
                HWSelect.dashboard.backend.settings["sensor_node3"]["ip_address"]
            )
            HWSelect.dashboard.backend.settings["sensor_node2"]["msg_port"] = (
                HWSelect.dashboard.backend.settings["sensor_node3"]["msg_port"]
            )
            HWSelect.dashboard.backend.settings["sensor_node2"]["hb_port"] = (
                HWSelect.dashboard.backend.settings["sensor_node3"]["hb_port"]
            )
            HWSelect.dashboard.backend.settings["sensor_node2"]["tsi"] = (
                HWSelect.dashboard.backend.settings["sensor_node3"]["tsi"]
            )
            HWSelect.dashboard.backend.settings["sensor_node2"]["pd"] = (
                HWSelect.dashboard.backend.settings["sensor_node3"]["pd"]
            )
            HWSelect.dashboard.backend.settings["sensor_node2"]["attack"] = (
                HWSelect.dashboard.backend.settings["sensor_node3"]["attack"]
            )
            HWSelect.dashboard.backend.settings["sensor_node2"]["iq"] = (
                HWSelect.dashboard.backend.settings["sensor_node3"]["iq"]
            )
            HWSelect.dashboard.backend.settings["sensor_node2"]["archive"] = (
                HWSelect.dashboard.backend.settings["sensor_node3"]["archive"]
            )

        if deleted_node <= 2:
            HWSelect.dashboard.backend.settings["sensor_node3"]["local_remote"] = (
                HWSelect.dashboard.backend.settings["sensor_node4"]["local_remote"]
            )
            HWSelect.dashboard.backend.settings["sensor_node3"]["nickname"] = (
                HWSelect.dashboard.backend.settings["sensor_node4"]["nickname"]
            )
            HWSelect.dashboard.backend.settings["sensor_node3"]["location"] = (
                HWSelect.dashboard.backend.settings["sensor_node4"]["location"]
            )
            HWSelect.dashboard.backend.settings["sensor_node3"]["notes"] = (
                HWSelect.dashboard.backend.settings["sensor_node4"]["notes"]
            )
            HWSelect.dashboard.backend.settings["sensor_node3"]["ip_address"] = (
                HWSelect.dashboard.backend.settings["sensor_node4"]["ip_address"]
            )
            HWSelect.dashboard.backend.settings["sensor_node3"]["msg_port"] = (
                HWSelect.dashboard.backend.settings["sensor_node4"]["msg_port"]
            )
            HWSelect.dashboard.backend.settings["sensor_node3"]["hb_port"] = (
                HWSelect.dashboard.backend.settings["sensor_node4"]["hb_port"]
            )
            HWSelect.dashboard.backend.settings["sensor_node3"]["tsi"] = (
                HWSelect.dashboard.backend.settings["sensor_node4"]["tsi"]
            )
            HWSelect.dashboard.backend.settings["sensor_node3"]["pd"] = (
                HWSelect.dashboard.backend.settings["sensor_node4"]["pd"]
            )
            HWSelect.dashboard.backend.settings["sensor_node3"]["attack"] = (
                HWSelect.dashboard.backend.settings["sensor_node4"]["attack"]
            )
            HWSelect.dashboard.backend.settings["sensor_node3"]["iq"] = (
                HWSelect.dashboard.backend.settings["sensor_node4"]["iq"]
            )
            HWSelect.dashboard.backend.settings["sensor_node3"]["archive"] = (
                HWSelect.dashboard.backend.settings["sensor_node4"]["archive"]
            )

        if deleted_node <= 3:
            HWSelect.dashboard.backend.settings["sensor_node4"]["local_remote"] = (
                HWSelect.dashboard.backend.settings["sensor_node5"]["local_remote"]
            )
            HWSelect.dashboard.backend.settings["sensor_node4"]["nickname"] = (
                HWSelect.dashboard.backend.settings["sensor_node5"]["nickname"]
            )
            HWSelect.dashboard.backend.settings["sensor_node4"]["location"] = (
                HWSelect.dashboard.backend.settings["sensor_node5"]["location"]
            )
            HWSelect.dashboard.backend.settings["sensor_node4"]["notes"] = (
                HWSelect.dashboard.backend.settings["sensor_node5"]["notes"]
            )
            HWSelect.dashboard.backend.settings["sensor_node4"]["ip_address"] = (
                HWSelect.dashboard.backend.settings["sensor_node5"]["ip_address"]
            )
            HWSelect.dashboard.backend.settings["sensor_node4"]["msg_port"] = (
                HWSelect.dashboard.backend.settings["sensor_node5"]["msg_port"]
            )
            HWSelect.dashboard.backend.settings["sensor_node4"]["hb_port"] = (
                HWSelect.dashboard.backend.settings["sensor_node5"]["hb_port"]
            )
            HWSelect.dashboard.backend.settings["sensor_node4"]["tsi"] = (
                HWSelect.dashboard.backend.settings["sensor_node5"]["tsi"]
            )
            HWSelect.dashboard.backend.settings["sensor_node4"]["pd"] = (
                HWSelect.dashboard.backend.settings["sensor_node5"]["pd"]
            )
            HWSelect.dashboard.backend.settings["sensor_node4"]["attack"] = (
                HWSelect.dashboard.backend.settings["sensor_node5"]["attack"]
            )
            HWSelect.dashboard.backend.settings["sensor_node4"]["iq"] = (
                HWSelect.dashboard.backend.settings["sensor_node5"]["iq"]
            )
            HWSelect.dashboard.backend.settings["sensor_node4"]["archive"] = (
                HWSelect.dashboard.backend.settings["sensor_node5"]["archive"]
            )

        HWSelect.dashboard.backend.settings["sensor_node5"]["local_remote"] = ""
        HWSelect.dashboard.backend.settings["sensor_node5"]["nickname"] = ""
        HWSelect.dashboard.backend.settings["sensor_node5"]["location"] = ""
        HWSelect.dashboard.backend.settings["sensor_node5"]["notes"] = ""
        HWSelect.dashboard.backend.settings["sensor_node5"]["ip_address"] = ""
        HWSelect.dashboard.backend.settings["sensor_node5"]["msg_port"] = ""
        HWSelect.dashboard.backend.settings["sensor_node5"]["hb_port"] = ""
        HWSelect.dashboard.backend.settings["sensor_node5"]["tsi"] = ""
        HWSelect.dashboard.backend.settings["sensor_node5"]["pd"] = ""
        HWSelect.dashboard.backend.settings["sensor_node5"]["attack"] = ""
        HWSelect.dashboard.backend.settings["sensor_node5"]["iq"] = ""
        HWSelect.dashboard.backend.settings["sensor_node5"]["archive"] = ""

        # Update Top Buttons
        HWSelect.dashboard.ui.pushButton_top_node2.setVisible(False)
        HWSelect.dashboard.ui.pushButton_top_node3.setVisible(False)
        HWSelect.dashboard.ui.pushButton_top_node4.setVisible(False)
        HWSelect.dashboard.ui.pushButton_top_node5.setVisible(False)
        HWSelect.dashboard.ui.pushButton_top_node1.setText(
            HWSelect.dashboard.backend.settings["sensor_node1"]["nickname"]
        )
        HWSelect.dashboard.ui.pushButton_top_node2.setText(
            HWSelect.dashboard.backend.settings["sensor_node2"]["nickname"]
        )
        HWSelect.dashboard.ui.pushButton_top_node3.setText(
            HWSelect.dashboard.backend.settings["sensor_node3"]["nickname"]
        )
        HWSelect.dashboard.ui.pushButton_top_node4.setText(
            HWSelect.dashboard.backend.settings["sensor_node4"]["nickname"]
        )
        if HWSelect.dashboard.backend.settings["sensor_node1"]["nickname"] == "":
            HWSelect.dashboard.ui.pushButton_top_node1.setText("New Sensor Node")
            HWSelect.dashboard.statusBar().sensor_nodes[0].setText("SN1: --")
            fissure.Dashboard.Slots.TopBarSlots.sensor_node_rightClick(HWSelect.dashboard, node_idx=-1)
        elif HWSelect.dashboard.backend.settings["sensor_node2"]["nickname"] == "":
            HWSelect.dashboard.ui.pushButton_top_node1.setText(
                HWSelect.dashboard.backend.settings["sensor_node1"]["nickname"]
            )
            HWSelect.dashboard.ui.pushButton_top_node2.setText("New Sensor Node")
            HWSelect.dashboard.ui.pushButton_top_node2.setVisible(True)
            HWSelect.dashboard.statusBar().sensor_nodes[1].setText("SN2: --")
            if HWSelect.dashboard.active_sensor_node <= 1:
                fissure.Dashboard.Slots.TopBarSlots.sensor_node_rightClick(HWSelect.dashboard, node_idx=0)
        elif HWSelect.dashboard.backend.settings["sensor_node3"]["nickname"] == "":
            HWSelect.dashboard.ui.pushButton_top_node2.setText(
                HWSelect.dashboard.backend.settings["sensor_node2"]["nickname"]
            )
            HWSelect.dashboard.ui.pushButton_top_node3.setText("New Sensor Node")
            HWSelect.dashboard.ui.pushButton_top_node2.setVisible(True)
            HWSelect.dashboard.ui.pushButton_top_node3.setVisible(True)
            HWSelect.dashboard.statusBar().sensor_nodes[3].setText("SN3: --")
            if HWSelect.dashboard.active_sensor_node == 2:
                fissure.Dashboard.Slots.TopBarSlots.sensor_node_rightClick(HWSelect.dashboard, node_idx=1)
            elif HWSelect.dashboard.active_sensor_node == 0:
                fissure.Dashboard.Slots.TopBarSlots.sensor_node_rightClick(HWSelect.dashboard, node_idx=0)
        elif HWSelect.dashboard.backend.settings["sensor_node4"]["nickname"] == "":
            HWSelect.dashboard.ui.pushButton_top_node3.setText(
                HWSelect.dashboard.backend.settings["sensor_node3"]["nickname"]
            )
            HWSelect.dashboard.ui.pushButton_top_node4.setText("New Sensor Node")
            HWSelect.dashboard.ui.pushButton_top_node2.setVisible(True)
            HWSelect.dashboard.ui.pushButton_top_node3.setVisible(True)
            HWSelect.dashboard.ui.pushButton_top_node4.setVisible(True)
            HWSelect.dashboard.statusBar().sensor_nodes[3].setText("SN4: --")
            if HWSelect.dashboard.active_sensor_node == 3:
                fissure.Dashboard.Slots.TopBarSlots.sensor_node_rightClick(HWSelect.dashboard, node_idx=2)
            elif HWSelect.dashboard.active_sensor_node == 0:
                fissure.Dashboard.Slots.TopBarSlots.sensor_node_rightClick(HWSelect.dashboard, node_idx=0)
        elif HWSelect.dashboard.backend.settings["sensor_node5"]["nickname"] == "":
            HWSelect.dashboard.ui.pushButton_top_node4.setText(
                HWSelect.dashboard.backend.settings["sensor_node4"]["nickname"]
            )
            HWSelect.dashboard.ui.pushButton_top_node5.setText("New Sensor Node")
            HWSelect.dashboard.ui.pushButton_top_node2.setVisible(True)
            HWSelect.dashboard.ui.pushButton_top_node3.setVisible(True)
            HWSelect.dashboard.ui.pushButton_top_node4.setVisible(True)
            HWSelect.dashboard.ui.pushButton_top_node5.setVisible(True)
            HWSelect.dashboard.statusBar().sensor_nodes[4].setText("SN5: --")
            if HWSelect.dashboard.active_sensor_node == 4:
                fissure.Dashboard.Slots.TopBarSlots.sensor_node_rightClick(HWSelect.dashboard, node_idx=3)
            elif HWSelect.dashboard.active_sensor_node == 0:
                fissure.Dashboard.Slots.TopBarSlots.sensor_node_rightClick(HWSelect.dashboard, node_idx=0)

        HWSelect.accept()


@qasync.asyncSlot(QtCore.QObject)
async def find(HWSelect: QtCore.QObject):
    """ 
    Finds the GPS location for the provided method.
    """
    # GPS Data Format
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    format_widget = getattr(HWSelect, f"comboBox_format_{tab_index+1}")
    get_format = str(format_widget.currentText())

    # Detect if Connected
    local_remote_stacked_widget = getattr(HWSelect, f"stackedWidget_local_remote_{tab_index+1}")

    # Send the Message
    gps_source_widget = getattr(HWSelect, f"comboBox_gps_source_{tab_index+1}")
    find_widget = getattr(HWSelect, f"pushButton_find_{tab_index+1}")

    # Local, Connected: IP
    if local_remote_stacked_widget.currentIndex() == 2:
        get_gps_source = str(gps_source_widget.currentText())
        if get_gps_source == "gpsd":
            await HWSelect.dashboard.backend.findGPS_Coordinates(str(tab_index), get_gps_source, get_format)
        elif get_gps_source == "Meshtastic":
            await HWSelect.dashboard.backend.findGPS_Coordinates(str(tab_index), get_gps_source, get_format)
        elif get_gps_source == "Saved":
            await HWSelect.dashboard.backend.findGPS_Coordinates(str(tab_index), get_gps_source, get_format)
        elif get_gps_source == "Internet":
            await HWSelect.dashboard.backend.findGPS_Coordinates(str(tab_index), get_gps_source, get_format)
        else:
            return

        # Disable the Find Button
        find_widget.setEnabled(False)

    # Remote, Connected: Meshtastic
    elif local_remote_stacked_widget.currentIndex() == 4:
        # Send the Message
        get_gps_source = str(gps_source_widget.currentText())
        if get_gps_source == "gpsd":
            await HWSelect.dashboard.backend.findGPS_CoordinatesLT(str(tab_index), get_gps_source, get_format)
        elif get_gps_source == "Meshtastic":
            await HWSelect.dashboard.backend.findGPS_CoordinatesLT(str(tab_index), get_gps_source, get_format)
        elif get_gps_source == "Saved":
            await HWSelect.dashboard.backend.findGPS_CoordinatesLT(str(tab_index), get_gps_source, get_format)
        elif get_gps_source == "Internet":
            await HWSelect.dashboard.backend.findGPS_CoordinatesLT(str(tab_index), get_gps_source, get_format)
        else:
            return

        # Disable the Find Button
        # find_widget.setEnabled(False)
    else:
        HWSelect.dashboard.logger.error("Sensor node not connected. Unable to retrieve GPS location.")


@QtCore.pyqtSlot(QtCore.QObject)
def map(HWSelect: QtCore.QObject):
    """ 
    Maps the GPS location in default KML viewer (likely Google Earth Pro).
    """
    # Gather Location
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    location_widget = getattr(HWSelect, f"textEdit_location_{tab_index+1}")
    format_widget = getattr(HWSelect, f"comboBox_format_{tab_index+1}")

    get_format = str(format_widget.currentText())

    # Convert to DD if needed
    try:
        coord = str(location_widget.toPlainText()).strip()
        
        if get_format == "MGRS":
            lat, lon = fissure.utils.mgrs_to_dd(coord)
        elif get_format == "DMS":
            lat, lon = fissure.utils.dms_to_dd(coord)
        else:  # Already in Decimal Degrees
            parts = coord.split(',')
            if len(parts) != 2:
                raise ValueError(f"Invalid Decimal Degrees format: {coord}")
            
            lat = parts[0].strip()
            lon = parts[1].strip()

        HWSelect.dashboard.logger.debug(f"✅ Converted Coordinates: {lat}, {lon}")  # Debugging output

    except ValueError as e:
        HWSelect.dashboard.logger.error(f"❌ Error in coordinate conversion: {e}")
        return

    except Exception as e:
        HWSelect.dashboard.logger.error(f"❌ Unexpected error: {e}")
        return

    # Construct KML
    kml_content = f"""<?xml version="1.0" encoding="UTF-8"?>
    <kml xmlns="http://www.opengis.net/kml/2.2">
      <Placemark>
        <name>GPS Location</name>
        <Point>
          <coordinates>{lon},{lat},0</coordinates>
        </Point>
      </Placemark>
    </kml>
    """

    # Create a temporary KML file in /tmp/
    with tempfile.NamedTemporaryFile(delete=False, suffix=".kml", dir="/tmp/") as temp_kml:
        temp_kml.write(kml_content.encode('utf-8'))
        temp_kml_path = temp_kml.name

    HWSelect.dashboard.logger.info(f"KML written to: {temp_kml_path}")

    # Open the file using xdg-open (respects user's default KML viewer)
    subprocess.run(["xdg-open", temp_kml_path], check=False)


@QtCore.pyqtSlot(QtCore.QObject)
def network_type_changed(HWSelect: QtCore.QObject):
    """ 
    Changes the stacked widget of connection controls for different network types (IP, Meshtastic, etc.).
    """
    # Handle Stacked Widget Switching
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    stacked_widget = getattr(HWSelect, f"stackedWidget_local_remote_{tab_index + 1}", None)
    select_connect_stacked_widget = getattr(HWSelect, f"stackedWidget_select_connect_{tab_index + 1}", None)
    network_type_widget = getattr(HWSelect, f"comboBox_network_type_{tab_index + 1}", None)
    local_actions_stacked_widget = getattr(HWSelect, f"stackedWidget_local_actions_{tab_index + 1}", None)
    remote_actions_stacked_widget = getattr(HWSelect, f"stackedWidget_remote_actions_{tab_index + 1}", None)
    
    if str(network_type_widget.currentText()) == "IP":
        select_connect_stacked_widget.setCurrentIndex(0)
        stacked_widget.setCurrentIndex(5)
        local_actions_stacked_widget.setCurrentIndex(0)        
        remote_actions_stacked_widget.setCurrentIndex(0)        
    elif str(network_type_widget.currentText()) == "Meshtastic":
        select_connect_stacked_widget.setCurrentIndex(0)
        stacked_widget.setCurrentIndex(3)
        local_actions_stacked_widget.setCurrentIndex(1)        
        remote_actions_stacked_widget.setCurrentIndex(1)  


@QtCore.pyqtSlot(QtCore.QObject)
def meshtastic_refresh(HWSelect: QtCore.QObject):
    """ 
    Refreshes the list of potential serial ports in the combobox.
    """
    # Move Page to the Right
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    serial_port_widget = getattr(HWSelect, f"comboBox_meshtastic_port_{tab_index+1}")

    # ports = [port.device for port in serial.tools.list_ports.comports()]

    # Only include /dev/ttyACM* and /dev/ttyUSB*
    ports = [
        port.device 
        for port in serial.tools.list_ports.comports() 
        if '/dev/ttyACM' in port.device or '/dev/ttyUSB' in port.device
    ]
    ports.sort(key=lambda s: [int(t) if t.isdigit() else t.lower() for t in re.split(r'(\d+)', s)])

    serial_port_widget.clear()
    serial_port_widget.addItems(ports if ports else ["No ports found"])


@qasync.asyncSlot(QtCore.QObject)
async def meshtastic_info(HWSelect: QtCore.QObject):
    """ 
    Opens a pop up with serial port and device information.
    """
    # Issue the Command
    path = "/dev/serial/by-id/"
    if os.path.exists(path):
        output_text = os.popen(f"ls -l {path}").read()
    else:
        output_text = "No serial devices found"

    # Open a Dialog
    ret = await fissure.Dashboard.UI_Components.Qt5.async_ok_dialog(HWSelect, output_text)


@qasync.asyncSlot(QtCore.QObject)
async def meshtastic_connect(HWSelect: QtCore.QObject):
    """
    Connects to the local serial connection that is preconfigured to talk to the remote node.
    """
    # Connect
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    serial_port = getattr(HWSelect, f"comboBox_meshtastic_port_{tab_index+1}")
    serial_baud_rate = getattr(HWSelect, f"comboBox_meshtastic_baud_rate_{tab_index+1}")
    local_remote_stacked_widget = getattr(HWSelect, f"stackedWidget_local_remote_{tab_index+1}")
    network_type_widget = getattr(HWSelect, f"comboBox_network_type_{tab_index+1}")

    get_serial_port = str(serial_port.currentText())
    get_serial_baud_rate = str(serial_baud_rate.currentText())

    # Check Existing IPs
    get_sensor_node = ["sensor_node1", "sensor_node2", "sensor_node3", "sensor_node4", "sensor_node5"]
    for n in range(0, len(get_sensor_node)):
        if (get_serial_port == HWSelect.dashboard.backend.settings[get_sensor_node[n]]["meshtastic_serial_port"]) and (n != tab_index):
            ret = await fissure.Dashboard.UI_Components.Qt5.async_ok_dialog(HWSelect, "Serial port is already in use.")
            return

    # Disable Buttons
    local_remote_stacked_widget.setEnabled(False)
    network_type_widget.setEnabled(False)
    QtWidgets.QApplication.processEvents()

    # Send Message for HIPRFISR to Create Sensor Node Connection
    await HWSelect.dashboard.backend.connectToSensorNodeMeshtastic(str(tab_index), get_serial_port, get_serial_baud_rate)


@qasync.asyncSlot(QtCore.QObject)
async def meshtastic_disconnect(HWSelect: QtCore.QObject):
    """
    Disconnect local serial connection and remove saved network connection from HIPRFISR.
    """
    # Disconnect
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    stacked_widget = getattr(HWSelect, f"stackedWidget_local_remote_{tab_index+1}")
    network_type_widget = getattr(HWSelect, f"comboBox_network_type_{tab_index+1}")

    stacked_widget.setCurrentIndex(3)
    network_type_widget.setEnabled(True)

    # Send Message for HIPRFISR to end Meshtastic Serial Connection
    await HWSelect.dashboard.backend.disconnectFromMeshtastic(str(tab_index))


@qasync.asyncSlot(QtCore.QObject)
async def meshtastic_recall_info(HWSelect: QtCore.QObject):
    """
    Sends a message to the HIPRFISR to retrieve sensor node information from its config file.
    """
    # Send Message to Backend
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    await HWSelect.dashboard.backend.recallInfoMeshtasticLT(str(tab_index))


@qasync.asyncSlot(QtCore.QObject)
async def meshtastic_recall_hardware(HWSelect: QtCore.QObject):
    """
    Sends a message to the HIPRFISR to retrieve sensor node information from its config file.
    """
    # Send Message to Backend
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    await HWSelect.dashboard.backend.recallHardwareMeshtasticLT(str(tab_index))


@qasync.asyncSlot(QtCore.QObject)
async def meshtastic_recall_status(HWSelect: QtCore.QObject):
    """
    Sends a message to the HIPRFISR to retrieve sensor node status.
    """
    # Send Message to Backend
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    await HWSelect.dashboard.backend.recallStatusMeshtasticLT(str(tab_index))


@qasync.asyncSlot(QtCore.QObject)
async def meshtastic_gps_beacon_enable(HWSelect: QtCore.QObject):
    """
    Sends a message to the HIPRFISR to enable the GPS TAK beacon.
    """
    # Send Message to Backend
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    await HWSelect.dashboard.backend.gpsBeaconEnableMeshtasticLT(str(tab_index))


@qasync.asyncSlot(QtCore.QObject)
async def meshtastic_gps_beacon_disable(HWSelect: QtCore.QObject):
    """
    Sends a message to the HIPRFISR to disable the GPS TAK beacon.
    """
    # Send Message to Backend
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    await HWSelect.dashboard.backend.gpsBeaconDisableMeshtasticLT(str(tab_index))


@qasync.asyncSlot(QtCore.QObject)
async def ip_gps_beacon_enable_disable(HWSelect: QtCore.QObject):
    """
    Sends a message to the HIPRFISR to enable/disable the GPS TAK beacon.
    """
    # Send Message to Backend
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    await HWSelect.dashboard.backend.gpsBeaconEnableDisableIP(str(tab_index))


@qasync.asyncSlot(QtCore.QObject)
async def ip_gps_beacon_refresh(HWSelect: QtCore.QObject):
    """
    Sends a message to the HIPRFISR to disable the GPS TAK beacon.
    """
    # Send Message to Backend
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    await HWSelect.dashboard.backend.gpsBeaconRefreshIP(str(tab_index))


@qasync.asyncSlot(QtCore.QObject)
async def meshtastic_reboot(HWSelect: QtCore.QObject):
    """
    Sends a message to the HIPRFISR to reboot the sensor node.
    """
    # Send Message to Backend
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    await HWSelect.dashboard.backend.rebootMeshtasticLT(str(tab_index))


@qasync.asyncSlot(QtCore.QObject)
async def ip_reboot(HWSelect: QtCore.QObject):
    """
    Sends a message to the HIPRFISR to reboot the sensor node.
    """
    # Send Message to Backend
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    await HWSelect.dashboard.backend.rebootIP(str(tab_index))


@qasync.asyncSlot(QtCore.QObject)
async def meshtastic_uptime(HWSelect: QtCore.QObject):
    """
    Sends a message to the HIPRFISR to retrieve the uptime of the sensor node.
    """
    # Send Message to Backend
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    await HWSelect.dashboard.backend.uptimeMeshtasticLT(str(tab_index))


@qasync.asyncSlot(QtCore.QObject)
async def ip_uptime(HWSelect: QtCore.QObject):
    """
    Sends a message to the HIPRFISR to retrieve the uptime of the sensor node.
    """
    # Send Message to Backend
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    await HWSelect.dashboard.backend.uptimeIP(str(tab_index))


@qasync.asyncSlot(QtCore.QObject)
async def meshtastic_memory(HWSelect: QtCore.QObject):
    """
    Sends a message to the HIPRFISR to retrieve the memory usage of the sensor node.
    """
    # Send Message to Backend
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    await HWSelect.dashboard.backend.memoryMeshtasticLT(str(tab_index))


@qasync.asyncSlot(QtCore.QObject)
async def ip_memory(HWSelect: QtCore.QObject):
    """
    Sends a message to the HIPRFISR to retrieve the memory usage of the sensor node.
    """
    # Send Message to Backend
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    await HWSelect.dashboard.backend.memoryIP(str(tab_index))


@qasync.asyncSlot(QtCore.QObject)
async def meshtastic_disk(HWSelect: QtCore.QObject):
    """
    Sends a message to the HIPRFISR to retrieve the disk usage of the sensor node.
    """
    # Send Message to Backend
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    await HWSelect.dashboard.backend.diskMeshtasticLT(str(tab_index))


@qasync.asyncSlot(QtCore.QObject)
async def ip_disk(HWSelect: QtCore.QObject):
    """
    Sends a message to the HIPRFISR to retrieve the disk usage of the sensor node.
    """
    # Send Message to Backend
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    await HWSelect.dashboard.backend.diskIP(str(tab_index))


@qasync.asyncSlot(QtCore.QObject)
async def meshtastic_cpu(HWSelect: QtCore.QObject):
    """
    Sends a message to the HIPRFISR to retrieve the CPU percentage of the sensor node.
    """
    # Send Message to Backend
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    await HWSelect.dashboard.backend.cpuMeshtasticLT(str(tab_index))


@qasync.asyncSlot(QtCore.QObject)
async def ip_cpu(HWSelect: QtCore.QObject):
    """
    Sends a message to the HIPRFISR to retrieve the CPU percentage of the sensor node.
    """
    # Send Message to Backend
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    await HWSelect.dashboard.backend.cpuIP(str(tab_index))


@qasync.asyncSlot(QtCore.QObject)
async def meshtastic_processes(HWSelect: QtCore.QObject):
    """
    Sends a message to the HIPRFISR to retrieve the processes on the sensor node.
    """
    # Send Message to Backend
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    await HWSelect.dashboard.backend.processesMeshtasticLT(str(tab_index))


@qasync.asyncSlot(QtCore.QObject)
async def ip_processes(HWSelect: QtCore.QObject):
    """
    Sends a message to the HIPRFISR to retrieve the processes on the sensor node.
    """
    # Send Message to Backend
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    await HWSelect.dashboard.backend.processesIP(str(tab_index))


@qasync.asyncSlot(QtCore.QObject)
async def meshtastic_ifconfig(HWSelect: QtCore.QObject):
    """
    Sends a message to the HIPRFISR to retrieve the ifconfig output on the sensor node.
    """
    # Send Message to Backend
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    await HWSelect.dashboard.backend.ifconfigMeshtasticLT(str(tab_index))


@qasync.asyncSlot(QtCore.QObject)
async def ip_ifconfig(HWSelect: QtCore.QObject):
    """
    Sends a message to the HIPRFISR to retrieve the ifconfig output on the sensor node.
    """
    # Send Message to Backend
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    await HWSelect.dashboard.backend.ifconfigIP(str(tab_index))  


@qasync.asyncSlot(QtCore.QObject)
async def meshtastic_iwconfig(HWSelect: QtCore.QObject):
    """
    Sends a message to the HIPRFISR to retrieve the iwconfig output on the sensor node.
    """
    # Send Message to Backend
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    await HWSelect.dashboard.backend.iwconfigMeshtasticLT(str(tab_index))


@qasync.asyncSlot(QtCore.QObject)
async def ip_iwconfig(HWSelect: QtCore.QObject):
    """
    Sends a message to the HIPRFISR to retrieve the iwconfig output on the sensor node.
    """
    # Send Message to Backend
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    await HWSelect.dashboard.backend.iwconfigIP(str(tab_index))  


@qasync.asyncSlot(QtCore.QObject)
async def ip_ping(HWSelect: QtCore.QObject):
    """
    Send command to HiprFisr to ping the host running the Sensor Node and await response.
    """
    # Send Message to Backend
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    await HWSelect.dashboard.backend.pingIP(str(tab_index))  


@qasync.asyncSlot(QtCore.QObject)
async def ip_node_refresh(HWSelect: QtCore.QObject):
    """
    Send command to HiprFisr to.
    """
    # Send Message to Backend
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    await HWSelect.dashboard.backend.nodeRefreshIP(str(tab_index))


@qasync.asyncSlot(QtCore.QObject)
async def ip_node_select(HWSelect: QtCore.QObject):
    """
    Send command to HiprFisr to.
    """
    # Get Node UUID
    tab_index = HWSelect.tabWidget_nodes.currentIndex()
    tableWidget_node_list_widget = getattr(HWSelect, f"tableWidget_node_list_{tab_index+1}")

    row = tableWidget_node_list_widget.currentRow()
    node_uuid = str(tableWidget_node_list_widget.item(row,0).text())

    # Send Message to Backend
    await HWSelect.dashboard.backend.nodeSelectIP(str(tab_index), node_uuid)


@qasync.asyncSlot(QtCore.QObject)
async def ip_node_reconnect(HWSelect: QtCore.QObject):
    """
    Send command to HiprFisr to.
    """
    # Send tab index
    tab_index = HWSelect.tabWidget_nodes.currentIndex()

    # Send Message to Backend
    await HWSelect.dashboard.backend.nodeReconnectIP(str(tab_index))