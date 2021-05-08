import abc
from weather_pipeline.utils.app_logger import _get_logger

class BaseDestination(metaclass=abc.ABCMeta):
    """BaseDestination
    Base class to define the interface of all the destination
    to which extracted data is to be loaded.
    
    Methods:
    -------
    1. load_data():
        Abstract method beared by all the destinations to
        define a point for loading the data to destination.
    
    2. load_data_as_chunk():
        Abstract method beared by all the destinations to
        define a point for loading the data to destination as chunks.
    """
    def __init__(self, _name:str=__name__):
        self.logger = _get_logger(name=_name)
    
    @abc.abstractmethod
    def load_data(self):
        raise NotImplementedError
    
    @abc.abstractmethod
    def load_data_as_chunk(self):
        raise NotImplementedError