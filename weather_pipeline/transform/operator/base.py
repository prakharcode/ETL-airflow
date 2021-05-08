import abc
from weather_pipeline.utils.app_logger import _get_logger


class BaseOperator(metaclass=abc.ABCMeta):
    """BaseOperator
    
    This is the abstract class that implements all the transformers
    needed to execute the pipeline.
    
    Methods:
    --------
        run_transform:
            An abstract class which gives a single intraction with 
            the engine required to carry out transformation.
    """
    def __init__(self, _name=__name__):
        self.logger = _get_logger(name=_name)
    
    @abc.abstractmethod
    def run_transform(self):
        raise NotImplementedError