from .Base import Auxiliary, DataLogger, DataOutput, DataSource
from .Icpdas import IcpdasDataSourceOutput
from .Mqtt import MqttDataSourceOutput, MqttTheThingsNetwork
from .Sensor_Electronic import SensoSysDataSource
import logging

__version__ = '1.3.1'

try:
    from .Beckhoff import AdsDataSourceOutput
except FileNotFoundError as e:
    # Without TwinCAT installed in system, 'FileNotFoundError' will be raised by Pyads due to missing 'TcAdsDll.dll'.
    # Ref1: https://github.com/stlehmann/pyads/issues/105
    # Ref2: https://stackoverflow.com/questions/76305160/windows-10-python-pyads-library-error-could-not-find-module-tcadsdll-dll
    logging.warning(
        f"Without TwinCAT installed on the system, 'AdsDataSourceOutput' submodule will not be available. "
        f"Original error: {e}")
except ImportError as e:
    logging.error(f"Failed to import 'AdsDataSourceOutput': {e}")

# Configure the root logger with a default leve and format
logging.basicConfig(
    level=logging.INFO,  # Set the default logging level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def set_logging_level(level: str):
    """
    Set the logging level for all loggers in this package.
    :param level: The desired logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    level = level.upper()
    try:
        numeric_level = getattr(logging, level)
        # Set the logging level for the root logger and all child loggers
        logging.getLogger().setLevel(numeric_level)
        for logger_name in [
            'ebcmeasurements.Base',
            'ebcmeasurements.Beckhoff',
            'ebcmeasurements.Icpdas',
            'ebcmeasurements.Mqtt',
            'ebcmeasurements.Sensor_Electronic',
        ]:
            logging.getLogger(logger_name).setLevel(numeric_level)
        print(f"Logging level set to {level} for all modules.")
    except AttributeError:
        print(f"Invalid logging level: {level}. Use DEBUG, INFO, WARNING, ERROR, or CRITICAL.")
