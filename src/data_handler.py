from data_exploration import DataExplorer
import pandas as pd
from datetime import datetime
from slugify import slugify
import os

class DataHandler:
    def __init__(self, data_dir='data', dt_features = []):
        """Initialize DataHandler with a data directory"""
        self.data_dir = data_dir

        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(current_dir)

        self.processed_dir = os.path.join(project_root, 'data', 'processed')
        self.raw_dir = os.path.join(project_root, 'data', 'raw')
        self.gtfs_dir = os.path.join(self.raw_dir, 'Ruter-GTFS')
        self.entur_dir = os.path.join(self.raw_dir, 'Entur-data')

        self.dt_features = dt_features

    #╔════════════════════════════════════════════════════════════════════╗
    #║                         FILE HANDLING                              ║
    #╚════════════════════════════════════════════════════════════════════╝ 

    def get_file_name(self,route_id, start_date = "", end_date=""):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'{slugify(route_id)}_{slugify(start_date)}-{slugify(end_date)}_{timestamp}' 

        return filename

    def save_raw_data(self, df, filename):
        """Save raw data with timestamp"""
        filepath = os.path.join(self.raw_dir, self.entur_dir, filename + '.csv')
        df.to_csv(filepath, index=False)

        return filepath
    
    def save_processed_data(self, df, filename):
        filepath = os.path.join(self.processed_dir, self.entur_dir, filename + '.csv')
        df.to_csv(filepath,index=False)

        return filepath
    
    def load_raw_entur_data(self, filename, datetime_convert=True):
        filepath = os.path.join(self.raw_dir, self.entur_dir, filename)
        loaded_df = pd.read_csv(filepath)

        if datetime_convert:
            return self.convert_date_to_datetime(loaded_df,self.dt_features)
        else:
            return loaded_df
            
    def load_processed_entur_data(self, filename, datetime_convert=True):
        filepath = os.path.join(self.processed_dir, self.entur_dir, filename)
        loaded_df = pd.read_csv(filepath)

        if datetime_convert:
            return self.convert_date_to_datetime(loaded_df,self.dt_features)
        else:
            return loaded_df
    
    #╔════════════════════════════════════════════════════════════════════╗
    #║                         DATA CLEANING                              ║
    #╚════════════════════════════════════════════════════════════════════╝ 
 
    def convert_date_to_datetime(self,df,dt_features):
        """
            Converts date features into datetime format
        """

        try: 
            df[dt_features] = df[dt_features].apply(pd.to_datetime)
        except:
            print("Selected features are not in datetime format")

        return df

    def remove_missing_values(self,df, cutoff = 100):
        """
            Removes columns with a lot of missing values
        """
        explorer = DataExplorer(df)

        missing_values = explorer.get_missing_values()
        by_column = missing_values['by_column']

        drop_cols = []
        for col in by_column:
            if by_column[col]['percentage'] >= cutoff:
                drop_cols.append(col)

        return df.drop(drop_cols, axis = 1)
    
    def merge_duplicated_stop_times(self,df):
        """
        Merge arrival and departure features, as these have the same values except for endpoints: 
            First stop has no arrival time, and final stop has no departure time. 

        Returns:
            Dataframe where true and aimed departure and arrival time has been merged
        """

        df['stopTime'] = df['departureTime'].combine_first(df['arrivalTime'])
        df['aimedStopTime'] = df['aimedDepartureTime'].combine_first(df['aimedArrivalTime'])

        return df
            
    
    #╔════════════════════════════════════════════════════════════════════╗
    #║                         GTFS HANDLING                              ║
    #╚════════════════════════════════════════════════════════════════════╝

    '''
    GTFS data from Ruter retrived from https://developer.entur.org/stops-and-timetable-data at 14/02/2025 15:48  
    '''

    def get_servicejourneys(self, route_id):
        """
        Get service journeys for a specific route
        
        Args:
            route_id (str): The route ID to find journeys of
        
        Returns:
            Routes which 
        """
        
        trips = pd.read_csv(os.path.join(self.gtfs_dir, 'trips.txt'))

        routes = trips[trips['route_id'] == route_id]['trip_id']

        return routes.to_list()
    
    def get_trips_by_timeframes(self,route_id: str, time_frames: list[tuple] = [('00:00:00', '23:59:59')]):
        """
        Get unique trip IDs for a specific route within given timeframes
        
        Args:
            route_id (str): The route ID to filter by
            time_frames (list): List of (start_time, end_time) tuples in HH:MM:SS format. Default is whole day: [('00:00:00', '23:59:59')]
        
        Returns:
            list: Unique trip IDs that operate within any of the given timeframes
        """
        
        stop_times = pd.read_csv(os.path.join(self.gtfs_dir, 'stop_times.txt'))

        routes = self.get_servicejourneys(route_id)
        route_stop_times = stop_times[(stop_times['trip_id'].isin(routes))]

        unique_trip_ids = set()

        for start_time, end_time in time_frames:
            timeframe_trips = route_stop_times[(route_stop_times['arrival_time'] >= start_time) & (route_stop_times['arrival_time'] <= end_time)]['trip_id'].unique()
            unique_trip_ids.update(timeframe_trips)

        return list(unique_trip_ids)
    

    #╔════════════════════════════════════════════════════════════════════╗
    #║                      FEATURE ENGINEERING                           ║
    #╚════════════════════════════════════════════════════════════════════╝ 

    def calculate_delay(self,df):
        """
            Calculates the delay between the recorded stop time versus the aimed stop time 
        """

        df['delay'] = (df['stopTime'] - df['aimedStopTime'])


        df['delayMinutes'] = df['delay'].dt.total_seconds()/60

        return df

    def calculate_time_between_stops(self, df):
        """
        Calculates the travel time between this stop and the next one
        """

        # Sort by journey ID and time
        df = df.sort_values(['operatingDate','serviceJourneyId','sequenceNr'])

        # Calculate time difference between consecutive stops
        df['timeToNextStop'] = df.groupby('serviceJourneyId')['stopTime'].diff().shift(-1)

        
        # Convert timedelta to minutes
        df['timeToNextStopMinutes'] = df['timeToNextStop'].dt.total_seconds() / 60
        
        return df

    def get_average_time_between_stops(self, df):
        """Calculate average time between each pair of consecutive stops"""

        # Group by current and next stop
        df['nextSequenceNr'] = df.groupby('serviceJourneyId')['sequenceNr'].shift(-1)
        df['nextStopPointName'] = df.groupby('serviceJourneyId')['stopPointName'].shift(-1)

        #Corrects for the case where the current stop is an endstop
        df.loc[df['nextSequenceNr'] != df['sequenceNr'] + 1, 'nextStopPointName'] = None
        
        # Calculate averages for stop pairs
        avg_times = df.groupby(['stopPointName', 'nextStopPointName'])['timeToNextStopMinutes'].agg(travelTimeMean = 'mean', travelTimeStd = 'std', travelTimeCount = 'count').reset_index()

        return avg_times
        



