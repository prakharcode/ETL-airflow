from weather_pipeline.utils.app_logger import _get_logger
from weather_pipeline.ingestion.load.destination.rdbms import rdbmsLoader

class Loader:
    """Loader
    
    Loader Driver Factory class that provides the drivers at runtime
    based on which type of driver is requested.
    
    Method:
    ------
    init():
        This returns the initialized driver based on user's request.
    """
    def __init__(self, destination_type, **kwargs):
        self.logger = _get_logger(name=__name__)
        if destination_type == "rdbms":
            self.driver = rdbmsLoader(**kwargs)
        else:
            raise NotImplementedError
    
    def init(self):
        return self.driver