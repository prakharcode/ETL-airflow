from logging import exception
from typing import List, Optional, Tuple
import pandas as pd
from pandas.io.parsers import read_fwf
from .base import BaseDestination
from sqlalchemy import create_engine, exc


class rdbmsLoader(BaseDestination):
    """rdbmsLoader
    
    Deriver from BaseDestination, implements loading to rdbms as 
    destination.
    
    Methods:
    --------
    1. load_data(names_types= None, filters=None, 
                col_required= None, **kwargs)
            Method to load data to a database as a whole.
            Params:
                1. names_types: A list of name of columes with its type.
                2. filters: filters to apply over the read data
                3. col_required: list of only columns to be including while
                            sending data to rdbms.
                4. kwargs: passed to the underlying pandas engine.
    """
    def __init__(self, **kwargs) -> None:
        super().__init__(_name=__name__)
        self.connection = kwargs.get("connection_string")
        self.filename = kwargs.get("file")
        self.reader = kwargs.get("reader_type", "csv")
        self.table_name = kwargs.get("table_name")
        self.schema = kwargs.get("schema", None)
        self.engine = create_engine(self.connection)
        
        if self.reader:
            if self.reader == "csv":
                self.reader = pd.read_csv
            elif self.reader == "fixed_width":
                self.reader = pd.read_fwf
            else:
                raise NotImplementedError
        self.__validator()
        
        self.logger.info(f"Launched loader destination to rdbms for"\
                        f" table {self.table_name}"\
                        f" with file type {self.reader}")
    
    def __validator(self) -> None:
        self.connection is not None, "No rdbms credential given"
        self.filename is not None, "local filename not given to load data from"
        self.table_name is not None, "Table name not given to be created on rdbms"
    
    def load_data(self, 
                names_types: Optional[List[Tuple[str, str]]]= None, 
                filters: Optional[List[Tuple[str, object]]]= None, 
                col_required:Optional[List[str]] = None,
                **kwargs) -> None:
        
        self.logger.info("Loading data as a whole.")
        df = self.reader(self.filename, **kwargs)
        
        if filters:
            for key, value in filters:
                
                self.logger.info(f"Applying filter on {key}")
                df = df[df[key] == value]
                if col_required:
                    df = df[col_required]
        try:
            df.to_sql(self.table_name, 
                    con=self.engine, 
                    schema=self.schema, 
                    if_exists='replace',
                    index=False)
        except Exception as e:
            self.logger.error("Failed to load data to db, retry", exc_info=True)
    
    def load_data_as_chunk(self, 
                        chunksize:int, 
                        names_types:Optional[List[Tuple[str, str]]] = None, 
                        filters: Optional[List[Tuple[str, object]]] = None, 
                        col_required:Optional[List[str]] = None,
                        **kwargs) -> None:
        
        self.logger.info(f"Loading data as a chunk:: size {chunksize}")
        if names_types:
            names = [name[0] for name in names_types]
            kwargs["names"] = names
        chunks = self.reader(self.filename, chunksize=chunksize, **kwargs)
        for chunk in chunks:
            try:
                self.logger.info("Loading chunk...")
                if filters:
                    for key, value in filters:
                        self.logger.info(f"Filtering {key} for {value}")
                        chunk = chunk[chunk[key] == value]
                
                if col_required:
                    self.logger.info(f"Only loading {col_required}")
                    chunk = chunk[col_required]
                
                chunk.to_sql(self.table_name, 
                            con=self.engine, 
                            schema=self.schema, 
                            if_exists='append', 
                            index=False)
            except Exception as e:
                self.logger.error(f"Falied to upload chunk data to sql, skipping to next chunk",
                                exc_info=True)
    
    def load_and_merge_on(self, 
                        chunksize:int, 
                        names_types:Optional[List[Tuple[str, str]]] = None, 
                        filters: Optional[List[Tuple[str, object]]] = None, 
                        col_required:Optional[List[str]] = None, 
                        join_with:Optional[str]=None, 
                        join_key:Optional[str]=None,
                        chunk_suffix:Optional[str]=None,
                        **kwargs) -> None:
        """load_and_merge_on
        To use for merging multiple chunks of data over a 
        single table by using SQL join funtion.
        """
        if col_required:
            col_names = " ,".join([f'"{name[0]}"' for name in names_types \
                            if name[0] in col_required])
            type_string = [f'"{name}" {value}' for name, value in names_types \
                if name in col_required]
        else:
            col_names = " ,".join([f'"{name[0]}"' for name in names_types])
            type_string = [f'"{name}" {value}' for name, value in names_types]
        
        
        if names_types:
            names = [name[0] for name in names_types]
            kwargs["names"] = names
        
        type_string = ", ".join(type_string)
        
        chunks = self.reader(self.filename, chunksize=chunksize, **kwargs)
        self.engine.execute(f"""CREATE TABLE IF NOT EXISTS {self.table_name} ({type_string})""")
        
        for chunk_no, chunk in enumerate(chunks):
            try:
                self.logger.info("Loading chunk...")
                if filters:
                    for key, value in filters:
                        self.logger.info(f"Filtering {key} for {value}")
                        chunk = chunk[chunk[key] == value]
                
                if col_required:
                    self.logger.info(f"Only loading {col_required}")
                    chunk = chunk[col_required]
                
                chunk_table = f"{self.table_name}_{chunk_no}" if not chunk_suffix else \
                                f"{self.table_name}_{chunk_no}_{chunk_suffix}"
                
                chunk.to_sql(chunk_table, 
                            con=self.engine, 
                            schema=self.schema, 
                            if_exists='append', 
                            index=False)
                
                chunk_col_names = " ,".join([f'{chunk_table}."{name[0]}"' for name in names_types \
                                        if name[0] in col_required])
                self.logger.info("Inserting chunk into table.")
                self.engine.execute(f"""
                                    INSERT INTO {self.table_name} ({col_names})
                                    SELECT {chunk_col_names} FROM {chunk_table}
                                    JOIN {join_with} 
                                    ON {chunk_table}."{join_key}" = city_stations."{join_key}"
                                    """)
                
                self.engine.execute(f"""DROP TABLE IF EXISTS {chunk_table}""")
            except Exception as e:
                self.logger.error(f"Falied to upload chunk data to sql, skipping to next chunk",
                                exc_info=True)