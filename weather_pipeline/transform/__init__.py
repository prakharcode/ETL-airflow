from weather_pipeline.utils.app_logger import _get_logger
from weather_pipeline.transform.operator.rdbms import rdbmsOperator

class Transformer:
    def __init__(self, destination_type, **kwargs):
        self.logger = _get_logger(name=__name__)
        if destination_type == "rdbms":
            self.driver = rdbmsOperator(**kwargs)
        else:
            raise NotImplementedError
    
    def init(self):
        return self.driver