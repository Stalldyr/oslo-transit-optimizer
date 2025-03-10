from entur_data import EnturAPI, EnturSQL
from frostapi import FrostAPI
import pandas as pd
from datetime import datetime, timedelta
import time

class DataFetcher:
    def __init__(self):
        self.enturJP = EnturAPI()
        self.enturSQL = EnturSQL()
        self.frost = FrostAPI()

        
    #╔════════════════════════════════════════════════════════════════════╗
    #║                      ENTUR JOURNEY PLANNER                         ║
    #╚════════════════════════════════════════════════════════════════════╝
    
    def collect_trip_data(self, route_id: str, time_interval=600, num_samples=6):
        """
        Collect multiple samples of trip data
        
        Args:
            route_id (str): ID of the bus route
            time_interval (int): Time between each sample in seconds
            num_samples (int): Number of samples to collect
            
        Returns:
            dataframe: Concated dataframe of all samples
        """

        line_info = self.enturJP.get_line_info(route_id)
        if line_info is None:
            print("Failed to get line info")
            return None
                
        processed_journey_ids = set()

        dfs = []
        
        for _ in range(num_samples):
            data = self.enturJP.get_realtime_journeys(route_id)
            transport_mode = data['line']['transportMode']
            if not data:
                continue 

            for journey in data['line']['serviceJourneys']:
                journey_id = journey['id']
                if journey_id in processed_journey_ids:
                    continue

                stops = journey['estimatedCalls']
                if any([stop['actualDepartureTime'] is None for stop in stops]):
                    continue
                
                processed_journey_ids.add(journey_id)
                journey_df = self.get_journey_dataframe(journey,journey_id,transport_mode)               
                dfs.append(journey_df)
        
            time.sleep(time_interval)
    
        return pd.concat(dfs) if dfs else None
    
    
    def get_journey_dataframe(self, journey, journey_id, transport_mode):
        """
        Convert journey data to DataFrame
        
        Args:
            journey (json): Data for a specific journey from the Entur API
            journey_id (str): ID of the journey
            transport_mode (str): Mode of transport (e.g., 'bus')
            
        Returns:
            dataframe: DataFrame containing journey data
        """
        
        rows = []
        
        for stop in journey['estimatedCalls']:
            stop['id'] = journey_id
            stop['stop'] = stop['quay']['name']
            rows.append(stop)

        #Drop unnecessary columns
        df = pd.DataFrame(rows).drop(['quay', 'realtime'], axis=1)
        if transport_mode == 'bus':
            df = df.drop(['aimedArrivalTime', 'expectedArrivalTime'], axis=1)

        # Convert time columns to datetime
        time_columns = [col for col in df.columns if 'Time' in col]
        for col in time_columns:
            df[col] = pd.to_datetime(df[col])

        # Add collection timestamp
        df['collectionTime'] = datetime.now()
        
        return df
    
    #╔════════════════════════════════════════════════════════════════════╗
    #║                            ENTUR SQL                               ║
    #╚════════════════════════════════════════════════════════════════════╝

    def get_data_SQL(self,line_id, start_date, end_date, target_times, window_minutes = 5):

        dfs = []
        
        for target_time in target_times:
            target_time_dt = datetime.strptime(target_time,"%H:%M:%S")
            start_time = (target_time_dt - timedelta(minutes=window_minutes)).strftime("%H:%M:%S")
            end_time = (target_time_dt + timedelta(minutes=window_minutes)).strftime("%H:%M:%S")

            df = self.enturSQL.get_data_by_lineid_and_timeframe(line_id, start_date,end_date, start_time, end_time)
            
            dfs.append(df)

        return pd.concat(dfs) if dfs else None

        
    #╔════════════════════════════════════════════════════════════════════╗
    #║                        FROST WEATHER DATA                          ║
    #╚════════════════════════════════════════════════════════════════════╝
    

    def collect_weather_data(self,start_time: datetime, end_time: datetime):
        """Fetch current weather data"""

        time_format = "%Y-%m-%dT%H:%M:%S"
        time_range = f"{start_time.strftime(time_format)}/{end_time.strftime(time_format)}"

        parameters = {
            'sources': 'SN18700',
            'elements': 'air_temperature,precipitation_amount , relative_humidity, wind_speed, surface_snow_thickness',
            'referencetime': time_range,
        }
        
        weather_data = self.frost.get_weather_data(parameters)
        
        if weather_data and 'data' in weather_data:
            return weather_data['data']
        return None


    def get_weather_dataframe(self, data):
        """
        Convert weather data to DataFrame
        UNFINISHED
        """
        df = pd.DataFrame()
        for entry in data:
            df[entry['elementId']] = entry['value']