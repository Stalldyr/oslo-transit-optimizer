from data_handler import DataHandler
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
        """
        Gets dataframe from SQL
        
        Args:
            line_id: 
            start_date: Format: YYYY-MM-DD

        """


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
    

    def collect_weather_data(self,start_date, end_date, chunk_size = 15, elements_list = None, source_list = None, save_to_csv = True):
        """Fetch current weather data"""

        if elements_list is None:
            elements_list = [
                'air_temperature', 
                'sum(precipitation_amount PT10M)', 
                'relative_humidity', 
                'wind_speed', 
                'surface_snow_thickness'
            ]

        if source_list is None:
            source_list = ['SN18700']


        sources = ','.join(source_list)


        for element in elements_list:
            chunk_dfs = []

            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')
            
            current_start = start
            while current_start <= end:
                current_end = min(current_start + timedelta(days=chunk_size-1), end)

                time_range = f"{current_start.strftime('%Y-%m-%d')}/{current_end.strftime('%Y-%m-%d')}"

                parameters = {
                    'sources': sources,
                    'elements': element,
                    'referencetime': time_range,
                }
                
                chunk_data = self.frost.get_weather_data(parameters)
                
                if chunk_data:
                    chunk_df = self.frost_data_to_df(chunk_data)
                    chunk_dfs.append(chunk_df)

                else:
                    print("Failed to fetch data for chunk:", time_range)

                current_start = current_end + timedelta(days=1)


            element_df = pd.concat(chunk_dfs) if chunk_dfs else None

            if element_df is not None and save_to_csv == True:
                DataHandler().save_raw_frost_data(element_df, f"{element}_{start_date}_{end_date}")
                print(f"Saved {element} data to CSV")
                
        return pd.concat(chunk_dfs) if chunk_dfs else None


    def frost_data_to_df(self, data):
        """
        Convert collected data to DataFrame
        
        Args:
            data (list): List of dictionaries containing weather data
            
        Returns:
            dataframe: DataFrame containing weather data
        """
        
        rows = []
        

        for item in data['data']:
            source_id = item['sourceId']
            reference_time = item['referenceTime']

            for obs in item['observations']:
                rows.append({
                    'sourceId': source_id,
                    'referenceTime': reference_time,
                    obs['elementId']: obs['value'],
                    'timeOffset': obs['timeOffset'],
                    'timeResolution': obs['timeResolution']
                })
                
        return pd.DataFrame.from_dict(rows)