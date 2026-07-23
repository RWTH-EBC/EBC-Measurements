"""
Module MqttTheThingsNetwork: Interface of MQTT server in TheThingsNetwork

What is the MQTT server of TheThingsNetwork (TTN):
    See https://www.thethingsindustries.com/docs/integrations/mqtt/
"""
from ebcmeasurements.Mqtt import MqttDataSourceOutput
import json
import logging.config
# Load logging configuration from file
logger = logging.getLogger(__name__)


class MqttTheThingsNetwork(MqttDataSourceOutput.MqttDataSourceOutput):
    class MqttDataOutput(MqttDataSourceOutput.MqttDataSourceOutput.MqttDataOutput):
        """MQTT (TTN) implementation of nested class SystemDataOutput"""
        def log_data(self, data: dict):
            if not data:
                logger.debug("No keys available in data, skipping logging ...")
                return

            data_cleaned = self.clean_keys_with_none_values(data)  # Clean none values
            if not data_cleaned:
                logger.info("No more keys after cleaning the data, skipping logging ...")
                return

            if not self.system.is_connected():
                logger.warning("Unable to publish the data due to disconnection")
                return

            # Prepare downlinks for all devices
            downlinks_map = {}  # Store map for all downlinks, with format {<device ID>: <downlinks (list[dict])>}
            for k, v in data.items():
                k_split = k.split(':')  # Split the key to device ID and variable name
                device_id, var_name = k_split[0], k_split[1]
                # Generate the downlink dict for the variable name
                downlink = {
                    'f_port': 1,
                    'decoded_payload': {var_name: str(v)},
                    'priority': 'NORMAL',
                    'confirmed': True
                }
                # Store downlink to list of downlinks
                if device_id not in downlinks_map:
                    downlinks_map[device_id] = [downlink]  # Add key and list to dict
                else:
                    downlinks_map[device_id].append(downlink)  # Append downlink to list

            # Publish payload to each device
            for k, v in downlinks_map.items():
                self.system.publish(
                    rf'v3/{self.system.username}/devices/{k}/down/push', json.dumps({'downlinks': v})
                )

    def __init__(
            self,
            broker: str = 'eu1.cloud.thethings.network',
            port: int = 1883,
            keepalive: int = 60,
            bind_address: str = '',
            bind_port: int = 0,
            username: str = None,
            password: str = None,
            device_ids: list[str] = None,
            device_uplink_payload_variable_names: dict[str, list[str]] = None,
            device_downlink_payload_variable_names: dict[str, list[str]] = None
    ):
        """
        Initialization of MqttTheThingsNetwork instance

        :param broker: MQTT broker of TheThingsNetwork
        :param port: See package paho.mqtt.client
        :param keepalive: See package paho.mqtt.client
        :param bind_address: See package paho.mqtt.client
        :param bind_port: See package paho.mqtt.client
        :param username: The username used for connecting, formatted as {application id}@{tenant id}
        :param password: The password used for connecting, must be generated in TTN under 'API Keys'
        :param device_ids: List of device IDs to connect to, the ID must match the one in "End devices - General
        information - End device ID"
        :param device_uplink_payload_variable_names: Dict containing all variable names in uplink decoded payload for
        each device, only these variables will be logged, formatted as {<Device ID>: [<var 1>, ...]}
        :param device_downlink_payload_variable_names: Dict containing all variable names in downlink payload for each
        device, only these variables will be allowed to be set to devices, formatted as {<Device ID>: [<var 1>, ...]}

        Default variable names are formatted as '<end device ID>:<variable>'.
        """
        logger.info("Initializing MqttTheThingsNetwork ...")

        # Check device ID
        if device_ids is None:
            raise ValueError("At least one device ID is required")
        else:
            self.device_ids = device_ids

        # Check if all device IDs are set with payload variable names
        self._up_pld_var_names = {}  # All variable names for uplink payload (read from TTN)
        self._down_pld_var_names = {}  # All variable names for downlink payload (set to TTN)
        for device_id in self.device_ids:
            # Set uplink payload variable names
            if device_uplink_payload_variable_names is None or device_id not in device_uplink_payload_variable_names:
                logger.warning(f"Uplink variable names not defined for device '{device_id}'")
                self._up_pld_var_names[device_id] = None
            else:
                self._up_pld_var_names[device_id] = device_uplink_payload_variable_names[device_id]
            # Set downlink payload variable names
            if device_downlink_payload_variable_names is None or device_id not in device_downlink_payload_variable_names:
                logger.warning(f"Downlink variable names not defined for device '{device_id}'")
                self._down_pld_var_names[device_id] = None
            else:
                self._down_pld_var_names[device_id] = device_downlink_payload_variable_names[device_id]

        # Init the MQTT of parent class
        super().__init__(
            broker=broker,
            port=port,
            keepalive=keepalive,
            bind_address=bind_address,
            bind_port=bind_port,
            username=username,
            password=password,
            subscribe_topics=[rf'v3/{username}/devices/{device_id}/up' for device_id in self.device_ids],
            publish_topics=[rf'v3/{username}/devices/{device_id}/down/push' for device_id in self.device_ids],
            data_source_all_variable_names=tuple(
                f'{key}:{v}' for key, values in self._up_pld_var_names.items() if values is not None for v in values
            ),
            data_output_all_variable_names=tuple(
                f'{key}:{v}' for key, values in self._down_pld_var_names.items() if values is not None for v in values
            )
        )

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        payload = json.loads(msg.payload.decode("utf-8"))
        logger.debug(f"Received message '{payload}' on topic '{topic}' with QoS {msg.qos}")
        unzipped_payload = self._unzip_payload(payload)  # Unzip payload
        device_id = unzipped_payload['device_id']
        if device_id is not None:
            data = {
                f'{device_id}:{var_name}': v
                for var_name, v in unzipped_payload['decoded_payload'].items()
                if f'{device_id}:{var_name}' in self.data_source.all_variable_names
            }
        else:
            return
        if self._data_source is not None:
            self._data_source.synchronize_data_buffer(data)  # Synchronize data buffer of data source
        if self._on_msg_data_logger is not None:
            self._on_msg_data_logger.run_data_logging(data)  # Trigger MQTT data logger

    @staticmethod
    def _unzip_payload(payload: dict) -> dict[str, str | dict | None]:
        """Unzip device ID and payload from TTN upstream payload"""
        return {
            'device_id': payload.get('end_device_ids', {}).get('device_id', None),
            'decoded_payload': payload.get('uplink_message', {}).get('decoded_payload', {})
        }
