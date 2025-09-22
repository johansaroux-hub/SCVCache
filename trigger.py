import time

import pymqi
import xml.etree.ElementTree as ET
import fnmatch
import logging
import socket
import os

# Logging setup
logging.basicConfig(filename='fte_queue_monitor.log', level=logging.INFO, format='%(asctime)s - %(message)s')

# MQ connection details
queue_manager = 'QMMFT'
host = '10.30.6.154'
# host = '10.30.6.137'
port = '1416'
channel = 'SYSTEM.DEF.SVRCONN'
conn_info = f'{host}({port})'
queue_name = 'SCVCACHE.TRANSFER.MONITOR.Q'
command_queue_name = "SYSTEM.FTE.COMMAND.EBE_3PT_AGENT"
wait_interval = 60 * 5

import pymqi


def send_to_mq(xml_payload):
    print(xml_payload)

    queue = pymqi.Queue(qmgr, command_queue_name)
    queue.put(xml_payload.encode('utf-8'))
    queue.close()


def build_fte_transfer_request(file_path):
    xml_payload = f"""<?xml version="1.0" encoding="UTF-8"?>
<request version="1.00" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:noNamespaceSchemaLocation="FileTransfer.xsd">
  <managedTransfer>
    <originator>
      <hostName>esbReporter</hostName>
      <userID>s1020514</userID>
    </originator>
    <sourceAgent agent="EBE_3PT_AGENT" QMgr="QMMFT"/>
    <destinationAgent agent="ESB_AGENT" QMgr="QMMFT"/>
    <transferSet priority="1" recoveryTimeout="-1">
      <item mode="binary" checksumMethod="MD5">
        <source recursive="false" type="file" disposition="leave">
          <file>{file_path}</file>
        </source>
        <destination type="directory" exist="overwrite">
            <file>/esb_share/single_customer_view/</file>
        </destination>
      </item>
    </transferSet>
  </managedTransfer>
</request>
"""
    send_to_mq(xml_payload)
    return xml_payload


def extract_message_data(xml_payload):
    # Parse the XML payload
    root = ET.fromstring(xml_payload)

    # Find the destinationAgent element
    destination_agent = root.find('destinationAgent')

    # Check if the destinationAgent agent attribute is 'EBE_3PT_AGENT'
    if destination_agent is not None and destination_agent.attrib.get('agent') == 'EBE_3PT_AGENT':
        # Extract destinationAgent attributes
        destination_agent_info = destination_agent.attrib

        # Extract transferSet time attribute
        transfer_set = root.find('transferSet')
        transfer_time = transfer_set.attrib.get('time') if transfer_set is not None else None

        # Extract destination file path from current element
        current = transfer_set.find('current') if transfer_set is not None else None
        destination = current.find('destination') if current is not None else None
        file_element = destination.find('file') if destination is not None else None
        destination_file_path = file_element.text if file_element is not None else None

        # Check if destination_file_path contains one of the specified substrings
        valid_paths = [
            'D:/EDW_Source_Files/EDW/SAP/',
            'D:/EDW_Source_Files/EDW/ITS/',
            'D:/EDW_Source_Files/EDW/VAT/'
        ]

        if destination_file_path and any(substring in destination_file_path for substring in valid_paths):
            print('path is valid for scenario')

            # File masks
            file_masks = [
                'accbaldeltaits_D',
                'D_SCVACCBALFILE_VAT_SUM',
                'accbalsummarysap.D'
            ]

            if destination_file_path and any(substring in destination_file_path for substring in file_masks):
                print('filename is valid for scenario')

                # Return the extracted information as a dictionary
                return {
                    'destinationAgent': destination_agent_info,
                    'transferSetTime': transfer_time,
                    'destinationFile': destination_file_path
                }

    # Return None if destinationAgent does not match
    return None


while True:
    # Connect to MQ and open queue
    qmgr = pymqi.connect(queue_manager, channel, conn_info)
    queue = pymqi.Queue(qmgr, queue_name)

    # Set up message options similar to Java MQGetMessageOptions
    gmo = pymqi.GMO()
    gmo.Options = pymqi.CMQC.MQGMO_WAIT
    gmo.WaitInterval = wait_interval

    print('gmo.WaitInterval =', gmo.WaitInterval)

    try:
        while True:
            message = ""
            try:
                md = pymqi.MD()
                message = queue.get(None, md, gmo)

            except pymqi.MQMIError as e:
                if e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                    print('# No message available, continue waiting ... as  pymqi does not honour my gmo ')
                    time.sleep(5)
                    continue
                else:
                    # Log other MQ errors
                    logging.error(f"MQMIError occurred: {e}")
                    break

            result = extract_message_data(message)

            if result:
                print("✅ Valid EBE_3PT_AGENT message received:")
                print("Destination Agent:", result['destinationAgent'])
                print("Transfer Time:", result['transferSetTime'])
                print("Destination File:", result['destinationFile'])

                build_fte_transfer_request(result['destinationFile'])


            else:
                print("⏭️ Skipped message: Not for EBE_3PT_AGENT")

            # print('destinationAgent = ', destinationAgent)

            # trigger_fte_copy(destinationFile)


    except KeyboardInterrupt:
        logging.info("Monitor stopped by user.")
    finally:
        queue.close()
        qmgr.disconnect()
    print('no messages received in the longest time, taking a break, will chack again in 2 minutes, now sleeping')
    time.sleep(2 * 60 * 1000)
