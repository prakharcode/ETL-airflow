import datetime
from weather_pipeline.ingestion.load import Loader
from weather_pipeline.transform import Transformer
from weather_pipeline.ingestion.extract import Extractor
from weather_pipeline.utils.app_logger import _get_logger
from weather_pipeline.config import LOADER_DESTINATION, LOADER_CONNECTION_STRING

logger = _get_logger(name=__name__)

def ingest_load_cities() -> None:
    """ingest_load_cities
    
    This function is responsible to fetch the data for
    all german cities and load it to the final db.
    """
    logger.info("Extracting and loading cities..")
    url = "http://www.fa-technik.adfc.de/code/opengeodb/DE.tab"
    
    # define extractor
    extractor = Extractor(source_type="web", source_url=url, temp_location="tmp/cities.csv")
    # inti extractor
    web_extractor = extractor.init()
    web_extractor.get(chunksize=10000)
    file = web_extractor.filepath
    
    # define loader
    loader = Loader(destination_type=LOADER_DESTINATION,
                    reader_type="csv",
                    connection_string = LOADER_CONNECTION_STRING,
                    file = file,
                    table_name = "cities")
    # init loader
    db_loader = loader.init()
    
    # loading data as chunk
    db_loader.load_data_as_chunk(chunksize=10000,
                                filters=[("typ", "Stadt")],
                                col_required = ["name", "lat", "lon"],
                                error_bad_lines=False, 
                                encoding="utf-8", 
                                delimiter="\t")
    logger.info("Cities are loaded")

def ingest_load_weather_stations() -> None:
    """ingest_load_cities
    
    This function is responsible to fetch the data for
    all weather stations and load it to the final db.
    """
    logger.info("Extracting and loading weather stations")
    url = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
    
    # define extractor
    extractor = Extractor(source_type="web", source_url=url)
    # inti extractor
    web_extractor = extractor.init()
    web_extractor.get(chunksize=10000)
    file = web_extractor.filepath
    
    # define loader
    loader = Loader(destination_type=LOADER_DESTINATION,
                    reader_type="fixed_width",
                    connection_string = LOADER_CONNECTION_STRING,
                    file = file,
                    table_name = "stations")
    
    # init loader
    db_loader = loader.init()
    
    names_types = [("ID", "VARCHAR"), ("LATITUDE", "REAL"), ("LONGITUDE", "REAL"), 
                ("ELEVATION", "REAL"), ("STATE", "VARCHAR"), ("NAME", "VARCHAR"), 
                ("GSN FLAG", "VARCHAR"), ("HCN/CRN FLAG", "VARCHAR"), 
                ("WMO ID", "VARCHAR")]
    
    # loading data as chunk
    db_loader.load_data_as_chunk(chunksize=10000,
                                names_types= names_types,
                                col_required = ["ID", "LATITUDE", "LONGITUDE", "STATE"],
                                header=None,
                                error_bad_lines=False)
    logger.info("Weather Stations loaded.")

def filter_weather_stations() -> None:
    """filter_weather_stations
    
    This function is responsible to take the loaded city
    and weather station tables, apply a cross join to 
    exactly find how many weather station are under the 
    permissiable range of 5 km to any German city.
    """
    logger.info("Joining weather stations and cities")
    connection_string = LOADER_CONNECTION_STRING
    transform = Transformer(destination_type="rdbms", connection_string=connection_string)
    rdms_trasform = transform.init()
    
    rdms_trasform.run_transform(
        """CREATE TEMP TABLE IF NOT EXISTS city_join_stations AS (
            SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY
                    "ID"
        	        ) as row_num
                FROM 
                stations CROSS JOIN cities
                WHERE "LATITUDE" BETWEEN (lat - 0.05) and (lat + 0.05)
		        and "LONGITUDE" BETWEEN (lon - 0.1) and (lon + 0.1)
	            );
        """)
    
    rdms_trasform.run_transform("""
                                CREATE TABLE IF NOT EXISTS city_stations AS (SELECT * FROM city_join_stations WHERE row_num = 1 );
                                """)
    
    rdms_trasform.run_transform("""DROP table city_join_stations;""")
    rdms_trasform.run_transform("""DROP table cities;""")
    rdms_trasform.run_transform("""DROP table stations;""")
    logger.info("Join complete")

def ingest_load_weather_readings(year:int=2020) -> None:
    """ingest_load_weather_readings
    
    This function is responsible to fetch the weather reading for all the 
    weather stations and filter the TMAX element, and then chunk wise 
    ingest tables to db and only insert all the reading which are from
    the stations we get in filter_weather_stations() table.
    """
    logger.info(f"Extracting and loading weather stations data for {year}")
    url = f"https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/{year}.csv.gz"
    
    # define extractor
    extractor = Extractor(source_type="web", source_url=url)
    # init extractor
    web_extractor = extractor.init()
    web_extractor.get(chunksize=10000)
    file = web_extractor.filepath
    
    # define loader
    loader = Loader(destination_type=LOADER_DESTINATION,
                    reader_type="csv",
                    connection_string = LOADER_CONNECTION_STRING,
                    file = file,
                    table_name = "readings")
    
    # init loader
    db_loader = loader.init()
    
    names_types = [("ID", "VARCHAR"), ("DATE", "TIMESTAMP"), ("ELEMENT", "VARCHAR"), 
            ("DATA", "REAL"), ("M-FLAG", "CHAR"), ("Q-FLAG", "CHAR"), 
            ("S-FLAG", "CHAR"), ("OBS-TIME", "CHAR")]
    
    db_loader.load_and_merge_on(chunksize=400000,
                                names_types = names_types,
                                filters=[("ELEMENT", "TMAX")],
                                col_required = ["ID", "DATE", "DATA"],
                                join_with="city_stations",
                                join_key= "ID",
                                chunk_suffix=year,
                                header=None,
                                parse_dates= ["DATE"],
                                date_parser=(lambda x: datetime.datetime.strptime(x, "%Y%m%d")),
                                error_bad_lines=False,
                                encoding="utf-8")
    logger.info(f"Data for {year} loaded")

def create_germany_medians() -> None:
    """create_germany_medians
    
    This funciton is responsible to create an aggregated table from the 
    readings table, this has median temprature of germany against each date.
    """
    logger.info("Creating Germany median data from readings")
    connection_string = LOADER_CONNECTION_STRING
    
    # define transformer
    transform = Transformer(destination_type="rdbms", connection_string=connection_string)
    
    # init transformer
    rdms_trasform = transform.init()

    rdms_trasform.run_transform("""CREATE TABLE IF NOT EXISTS germany_medians AS 
                                (SELECT readings."DATE", PERCENTILE_CONT(0.5) 
                                WITHIN GROUP(ORDER BY readings."DATA") as median_data
                                from readings 
                                GROUP BY readings."DATE");
                                """)
    logger.info("Germany medians created")

def get_city_interval(city:str, date:str) -> None:
    """city_merged_median
    
    This funciton is responsible to create an aggregated over a date for all cities
    in the order of their percentile score of hotness.
    """
    logger.info(f"Creating {city} rank table for {date}.")
    connection_string = LOADER_CONNECTION_STRING
    table_name_date = date.replace("-","")
    transform = Transformer(destination_type="rdbms", connection_string=connection_string)
    rdms_trasform = transform.init()
    
    rdms_trasform.run_transform(
            f"""
                CREATE TEMP TABLE IF NOT EXISTS city_median_agg AS (
                    WITH cte AS 
                        (SELECT germany_medians."DATE", intermediate.name, 
                        CAST(intermediate.median_data >= (germany_medians.median_data + 30) AS INTEGER) AS median_greater_germany 
                        FROM germany_medians JOIN (
                                SELECT filter_readings."DATE", city_stations."name", PERCENTILE_CONT(0.5) 
                                WITHIN GROUP(ORDER BY filter_readings."DATA") as median_data
                                FROM city_stations JOIN
                                (SELECT * FROM readings WHERE readings."DATE" BETWEEN ('{date} 00:00:00'::TIMESTAMP - 									INTERVAL '1 year') 
                                and ('{date} 00:00:00')) as filter_readings
                                ON filter_readings."ID" = city_stations."ID"
                                GROUP BY filter_readings."DATE", city_stations."name") as intermediate 
                        ON germany_medians."DATE" = intermediate."DATE")
                    SELECT cte."name", SUM(cte.median_greater_germany) AS hotness 
                    FROM cte
                    GROUP BY cte."name"
                );
            """)
    
    rdms_trasform.run_transform(
            f"""
                CREATE TABLE city_rank_{table_name_date} as
                    WITH cte AS 
	                    (SELECT city_median_agg."name", city_median_agg."hotness", 
		                PERCENT_RANK() OVER (order by city_median_agg."hotness") as percentile 
		                FROM city_median_agg)
	                SELECT cte.name, cte.hotness, cte.percentile,
	                CASE
                        WHEN floor(cte.percentile * 10) = 10 THEN 9
                        ELSE  floor(cte.percentile * 10)
                        END as percentile_id
                    from cte;
            """)
    
    rdms_trasform.run_transform("""DROP TABLE IF EXISTS city_median_agg""")
    logger.info("Rank Table created")