import pandas as pd
from datetime import datetime
from slugify import slugify
import os

class DataHandler:
    def __init__(self, data_dir='data'):
        """Initialize DataHandler with a data directory"""
        self.data_dir = data_dir

        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(current_dir)

        self.processed_dir = os.path.join(project_root, 'data', 'processed')
        self.raw_dir = os.path.join(project_root, 'data', 'raw')
        self.gtfs_dir = os.path.join(self.raw_dir, 'Ruter-GTFS')

    #╔════════════════════════════════════════════════════════════════════╗
    #║                         FILE HANDLING                              ║
    #╚════════════════════════════════════════════════════════════════════╝ 

    def save_raw_data(self, df, route_id):
        """Save raw data with timestamp"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'route_{slugify(route_id)}_{timestamp}.csv'
        filepath = os.path.join(self.raw_dir, filename)
        df.to_csv(filepath, index=False)
        return filepath

    def load_and_append(self, new_df, route_id, processed_filename='processed_trips.csv'):
        """Load existing processed data and append new data"""
        filepath = os.path.join(self.processed_dir, processed_filename)
        
        # If file exists, load and append
        if os.path.exists(filepath):
            existing_df = pd.read_csv(filepath)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            combined_df = new_df
            
        # Save combined data
        combined_df.to_csv(filepath, index=False)
        return combined_df
    
    
    #╔════════════════════════════════════════════════════════════════════╗
    #║                         GTFS HANDLING                              ║
    #╚════════════════════════════════════════════════════════════════════╝

    '''
    GTFS data from Ruter retrived from https://developer.entur.org/stops-and-timetable-data at 14/02/2025 15:48  
    '''

    def get_servicejourneys(self, route_id):
        """Get service journeys for a specific route"""
        
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

    def calculate_time_between_stops(self, df):
        """Calculate time differences between consecutive stops for each journey"""
        # Convert time columns to datetime if they're strings
        time_cols = ['expectedDepartureTime', 'expectedArrivalTime']
        for col in time_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])

        # Sort by journey ID and time
        df = df.sort_values(['id', 'expectedDepartureTime'])
        
        # Calculate time difference between consecutive stops
        df['time_to_next_stop'] = df.groupby('id')['expectedArrivalTime'].diff().shift(-1)
        
        # Convert timedelta to minutes
        df['minutes_to_next_stop'] = df['time_to_next_stop'].dt.total_seconds() / 60
        
        return df

    def get_average_time_between_stops(self, df):
        """Calculate average time between each pair of consecutive stops"""
        # Group by current and next stop
        df['next_stop'] = df.groupby('id')['stop'].shift(-1)
        
        # Calculate averages for stop pairs
        avg_times = df.groupby(['stop', 'next_stop'])['minutes_to_next_stop'].agg(['mean', 'std', 'count']).reset_index()
        
        return avg_times[avg_times['next_stop'].notna()] 
        