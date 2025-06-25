from numpy import std
from data_exploration import DataExplorer
import pandas as pd
from datetime import datetime, timedelta
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
        self.entur_dir = os.path.join(self.raw_dir, )

        self.dt_features = dt_features

        self.filenames = {
            "air_temperature": "air_temperature_2024-01-01_2024-12-31.csv",
            "relative_humidity": "relative_humidity_2024-01-01_2024-12-31.csv",
        }

    #╔════════════════════════════════════════════════════════════════════╗
    #║                         FILE HANDLING                              ║
    #╚════════════════════════════════════════════════════════════════════╝ 

    def get_file_name(self,route_id, start_date = "", end_date = ""):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = slugify(f'{route_id}_{start_date}_{end_date}_{timestamp}')

        return filename

    def save_raw_entur_data(self, df, filename):
        """Save raw data with timestamp"""
        filepath = os.path.join(self.raw_dir, 'Entur-data', filename + '.csv')
        df.to_csv(filepath, index=False)

        return filepath
    
    def save_processed_entur_data(self, df, filename):
        filepath = os.path.join(self.processed_dir, 'Entur-data', filename)
        df.to_csv(filepath,index=False)

        return filepath
    
    def load_raw_entur_data(self, filename, datetime_convert=True):
        filepath = os.path.join(self.raw_dir, 'Entur-data', filename)
        loaded_df = pd.read_csv(filepath)

        if datetime_convert:
            return self.convert_date_to_datetime(loaded_df)
        else:
            return loaded_df
            
    def load_processed_entur_data(self, filename, datetime_convert=True):
        filepath = os.path.join(self.processed_dir, 'Entur-data', filename)
        loaded_df = pd.read_csv(filepath)

        if datetime_convert:
            return self.convert_date_to_datetime(loaded_df)
        else:
            return loaded_df
    

    def save_raw_frost_data(self, df, filename):
        """Save raw data with timestamp"""
        filepath = os.path.join(self.raw_dir, 'Frost-data', filename + '.csv')
        df.to_csv(filepath, index=False)

        return filepath
    
    def save_processed_frost_data(self, df, filename):
        filepath = os.path.join(self.processed_dir, 'Frost-data', filename)
        df.to_csv(filepath,index=False)

        return filepath
    
    def load_raw_frost_data(self, filename, datetime_convert=True):
        filepath = os.path.join(self.raw_dir, 'Frost-data', filename)
        loaded_df = pd.read_csv(filepath)

        if datetime_convert:
            return self.convert_date_to_datetime(loaded_df)
        else:
            return loaded_df
            
    def load_processed_frost_data(self, filename, datetime_convert=True):
        filepath = os.path.join(self.processed_dir, 'Frost-data', filename)
        loaded_df = pd.read_csv(filepath)

        if datetime_convert:
            return self.convert_date_to_datetime(loaded_df)
        else:
            return loaded_df

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
    #║                         DATA CLEANING                              ║
    #╚════════════════════════════════════════════════════════════════════╝ 
 
    def convert_date_to_datetime(self,df,n_samples = 5):
        """
        Converts date features into datetime format
        """

        for col in df.select_dtypes(["object"]).columns:
            samples = []
            count = 0
            for val in df[col]:
                if pd.notna(val):
                    samples.append(val)
                    count += 1
                    if count >= n_samples:
                        break


            try:
                #Checks if the samples is in datatime format
                for val in samples:
                    pd.to_datetime(val)
                
                #Converts the whole column if the samples passes
                df[col] = pd.to_datetime(df[col])

            except:
                pass

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
        
    #╔════════════════════════════════════════════════════════════════════╗
    #║                         DATA FILTERING                             ║
    #╚════════════════════════════════════════════════════════════════════╝     

    def filter_by_time(self, df, start_time, end_time, time_col = 'aimedStopTime'):
        """
        Filters the dataframe by a specific time range
        """
        
        return df[(df[time_col].dt.time >= pd.to_datetime(start_time).time()) & (df[time_col].dt.time <= pd.to_datetime(end_time).time())]


    #╔════════════════════════════════════════════════════════════════════╗
    #║                      FEATURE ENGINEERING                           ║
    #╚════════════════════════════════════════════════════════════════════╝ 

    def merge_duplicated_stop_times(self,df):
        """
        Merge arrival and departure features, as these have the same values except for endpoints for most bus routes. 
        First stop has no arrival time, and final stop has no departure time. 

        Returns:
            Dataframe where true and aimed departure and arrival time has been merged
        """

        df['stopTime'] = df['departureTime'].combine_first(df['arrivalTime'])
        df['aimedStopTime'] = df['aimedDepartureTime'].combine_first(df['aimedArrivalTime'])

        return df
    
    def append_next_stop(self,df):
        
        df['nextSequenceNr'] = df.groupby('serviceJourneyId')['sequenceNr'].shift(-1)

        df['nextStopPointName'] = df.groupby('serviceJourneyId')['stopPointName'].shift(-1)

        #Corrects for the case where the current stop is an endstop
        df.loc[df['nextSequenceNr'] != df['sequenceNr'] + 1, 'nextStopPointName'] = None

        return df
    
    def calculate_time_at_stop(self,df):
        df['stopDuration'] = df['arrivalTime'] - df['departureTime']
        df['aimedStopDuration'] = df['aimedArrivalTime'] - df['aimedDepartureTime']

        return df


    def calculate_delay(self,df):
        """
        Calculates the delay between the recorded stop time versus the aimed stop time 
        """

        if 'stopTime' in df.columns:
            df['delay'] = (df['stopTime'] - df['aimedStopTime'])
            df['delayMinutes'] = df['delay'].dt.total_seconds()/60
        else:
            df['arrivalDelay'] = (df['arrivalTime'] - df['aimedArrivalTime'])
            df['departureDelay'] = (df['departureTime'] - df['aimedDepartureTime'])
            df['arrivalDelayMinutes'] = df['arrivalDelay'].dt.total_seconds()/60
            df['departureDelayMinutes'] = df['departureDelay'].dt.total_seconds()/60

        return df

    def calculate_time_between_stops(self, df):
        """
        Calculates the travel time between this stop and the next one
        """

        # Sort by journey ID and time
        df = df.sort_values(['operatingDate','serviceJourneyId','sequenceNr'])

        # Calculate time difference between consecutive stops
        if 'stopTime' in df.columns:
            df['timeToNextStop'] = df.groupby('serviceJourneyId')['stopTime'].diff().shift(-1)
            df['aimedTimeToNextStop'] = df.groupby('serviceJourneyId')['aimedStopTime'].diff().shift(-1)
        else:
            df['timeToNextStop'] = df.groupby('serviceJourneyId')['arrivalTime'].shift(-1) - df.groupby('serviceJourneyId')['departureTime']
            df['aimedTimeToNextStop'] = df.groupby('serviceJourneyId')['aimedArrivalTime'].shift(-1) - df.groupby('serviceJourneyId')['aimedDepartureTime']

        if 'nextSequenceNr' not in df.columns:
            df['nextSequenceNr'] = df.groupby('serviceJourneyId')['sequenceNr'].shift(-1)
            df.loc[df['nextSequenceNr'] != df['sequenceNr'] + 1, 'timeToNextStop'] = None
        
        # Convert timedelta to minutes
        df['timeToNextStopMinutes'] = df['timeToNextStop'].dt.total_seconds() / 60
        df['aimedTimeToNextStopMinutes'] = df['aimedTimeToNextStop'].dt.total_seconds() / 60
        
        return df
    
    def calculate_delay_change(self, df):
        # Make sure dataframe is sorted correctly
        df = df.sort_values(['operatingDate', 'serviceJourneyId', 'sequenceNr'])
            
        # Calculate delay change between consecutive stops
        df['delayChange'] = df.groupby('serviceJourneyId')['delayMinutes'].diff()

        return df
    
    #╔════════════════════════════════════════════════════════════════════╗
    #║                          DATA ANALYSIS                             ║
    #╚════════════════════════════════════════════════════════════════════╝ 

    def get_stop_pair_stats(self, df):
        """
        Calculate average time between each pair of consecutive stops
        Suggested columns: 
            -timeToNextStopMinutes
            -delayMinutes
        """

        stop_pairs = df.groupby(['stopPointName', 'nextStopPointName']).agg(
            travelTimeAvg=('timeToNextStopMinutes', 'mean'),
            travelTimeStd=('timeToNextStopMinutes', 'std'),
            scheduledTimeAvg=('aimedTimeToNextStopMinutes', 'mean'),
            scheduledTimeStd=('aimedTimeToNextStopMinutes', 'std'),
            delayAvg = ('delayMinutes', 'mean'),
            delayStd = ('delayMinutes', 'std'),
            delayChangeAvg=('delayChange', 'mean'),
            delayChangeMax=('delayChange', 'max'),
            count=('timeToNextStopMinutes', 'count')
        ).reset_index()

        return stop_pairs
    
    def get_average_delay_by_time(self,df, target_time, limit = 10, hours = 1):
        """
        Get average delay between stops for a specific time window
        """

        target_time_dt = datetime.strptime(target_time, '%H:%M:%S')
        start = target_time_dt - timedelta(hours=hours)
        end = target_time_dt + timedelta(hours=hours)

        time_df = self.filter_by_time(df, start, end)
        stop_pairs = self.get_stop_pair_stats(time_df)

        stop_pairs = stop_pairs[stop_pairs['count'] > limit].sort_values('delayChangeAvg')

        return stop_pairs[['stopPointName', 'nextStopPointName', 'delayChangeAvg','delayChangeMax']]


    def get_weather_delay_correlation(self, transit_df, weather_df = None, weather = "surface_snow_thickness"):

        '''
        Calculates the correlation between delay and weather data
        Possible inputs: 
            'surface_snow_thickness', 
            'air_temperature', 
            'sum(precipitation_amount PT10M)', 
            'wind_speed', 
            'relative_humidity'
        '''

        if weather_df is None:
            weather_df = self.load_raw_frost_data(f"{weather}_2024-01-01_2024-12-31.csv")

        joined_df = self.join_transit_and_weather_data(transit_df, weather_df)

        correlation = joined_df[['delayMinutes', weather]].corr()

        return correlation
        
    #╔════════════════════════════════════════════════════════════════════╗
    #║                              PLOTS                                 ║
    #╚════════════════════════════════════════════════════════════════════╝ 

    def stop_plots(df):
        pass

    #╔════════════════════════════════════════════════════════════════════╗
    #║                            JOINING                                 ║
    #╚════════════════════════════════════════════════════════════════════╝ 
    
    def join_transit_and_weather_data(self, transit_df, weather_df, time_col='stopTime', weather_time_col='referenceTime'):
        """
        Join transit and weather data on the date column
        
        """
        
        transit_copy = transit_df.copy()
        weather_copy = weather_df.copy()

        transit_copy = transit_copy.dropna(subset=[time_col])
        weather_copy = weather_copy.dropna(subset=[weather_time_col])
               
        joined_df = pd.merge_asof(
            transit_copy.sort_values(time_col),
            weather_copy.sort_values(weather_time_col),
            left_on=time_col,
            right_on=weather_time_col,
            direction='nearest'
        )

        return joined_df

