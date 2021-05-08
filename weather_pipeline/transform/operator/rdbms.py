from .base import BaseOperator
from sqlalchemy import create_engine

class rdbmsOperator(BaseOperator):
    """rdbmsOperator
    
    Deriver from BaseBaseOperator, implements transformation using rdbms to 
    staisfy customer needs.
    
    Methods:
    ----------
    run_transform():
        This method is responsible to carry out 
        transformation process over database using SQLAlchemy exexute.
    """
    def __init__(self, **kwargs):
        super().__init__(_name=__name__)
        self.connection_string = kwargs.get("connection_string")
        self.engine = create_engine(self.connection_string)
    
    def __validator(self):
        self.connection_string is not None, "The rdbms connection is not provided."
    
    def run_transform(self, sql:str):
        self.engine.execute(sql)