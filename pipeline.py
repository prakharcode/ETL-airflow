from weather_pipeline.tasks import *
from weather_pipeline.utils.app_logger import _get_logger

logger = _get_logger("Weather_Pipeline")

if __name__ == "__main__":
    logger.info("Executing pipeline to create a table as given in the "\
                "challenge example")
    
    # extracting and loading cities
    ingest_load_cities()
    
    # extracting and loading weather stations
    ingest_load_weather_stations()
    
    # Joining weather stations and cities to create first table
    # city_stations - city and station pair to care about.
    filter_weather_stations()
    
    
    # extracting and loading weather stations data for
    # stations present in city_stations
    # creation of third table - readings
    ingest_load_weather_readings(year=2020)
    ingest_load_weather_readings(year=2021)
    
    # creating table which has median temprature
    # for Germany over each day.
    # table: Germany median
    create_germany_medians()
    
    # Agg table created from city_medians
    # final table 
    # Berlin_rank
    city = "Berlin"
    date = "2021-03-31"
    get_city_interval(city=city, date=date)
    
    logger.info("Pipeline execution complete check the database "\
                f"for {city}_rank table to run a query for percentile rank.")