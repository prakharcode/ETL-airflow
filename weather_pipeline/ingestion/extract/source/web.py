import requests
from tqdm import tqdm
from pathlib import Path
from .base import BaseSource


class webSource(BaseSource):
    """webSource
    
    Deriver from BaseSource, implements extraction from web as 
    source.
    
    Methods:
    -------
    get(chunksize)
    Responsible to fetch data from web as chunk.
        Params:
        1. chunksize (INT): Default 1024. chunksize to read and write. 
    """
    def __init__(self, **kwargs) -> None:
        super().__init__(_name = __name__)
        self.source = kwargs.get("source_url", None)
        self.temp_destination = kwargs.get("temp_location", None)
        
        if self.temp_destination is None:
            filename = self.source.rsplit('/')[-1]
            self.temp_destination = f"tmp/{filename}"
        
        self.temp_destination= Path(self.temp_destination)
        self.__validator()
    
    def __validator(self):
        assert self.source is not None, "No source given to start extraction from"
    
    @property
    def filepath(self):
        return self.temp_destination
    
    def get(self, chunksize:int=1024) -> None:
        try:
            self.logger.info(f"Beginning to download, file at {self.source}"
                f" :: {self.temp_destination}")
            
            response = requests.get(self.source, stream=True)
            self.temp_destination.parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.temp_destination, "wb") as fl:
                for chunk in tqdm(response.raw.stream(chunksize, decode_content=True)):
                    fl.write(chunk)
        
        except Exception as e:
            self.logger.error(f"Failed to download file: {self.source} => {e}")