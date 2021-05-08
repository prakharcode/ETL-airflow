import abc
from weather_pipeline.utils.app_logger import _get_logger

class BaseSource(metaclass=abc.ABCMeta):
    """BaseSource
    Base class to define the interface of all the sources
    that can be added to the pipeline.
    
    Methods:
    -------
    1. get():
        Abstract method beared by all the sources of extraction
        to define a single point of fetching content.
    """
    def __init__(self, _name:str=__name__):
        self.logger= _get_logger(name=_name)
    
    @abc.abstractmethod
    def get(self):
        raise NotImplementedError