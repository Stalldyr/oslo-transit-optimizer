from entur_data import EnturAPI, EnturSQL
from frostapi import FrostAPI
from data_fetcher import DataFetcher
from data_handler import DataHandler


def main(route_id, start_date, end_date, target_times):
    """Main function to collect and process transit data"""

    #Fetches rawdata from the Entur database through an SQL-query
    raw_data = fetcher.get_data_SQL(route_id, start_date, end_date, target_times)
    
    if raw_data is not None:
        #Saves the raw data to csv-file
        filename = handler.get_file_name(route_id, start_date, end_date)
        handler.save_raw_data(raw_data,filename)

        #Performs various data cleansing methods
        cleaned_data = data_cleaning(raw_data)

        #Performs various data feature engineering
        processed_data = feature_engineering(cleaned_data)
        avg_times = handler.get_average_time_between_stops(processed_data)

        #Saves the processed data to csv-file
        handler.save_processed_data(processed_data,filename+"_processed")
        handler.save_processed_data(avg_times)

    weather_data = fetcher.collect_weather_data(start_date, end_date, save_to_csv=True)


def data_cleaning(df):
    #Removes empty features
    df = handler.remove_missing_values(df)

    #Converts features with dates into datetime-format for simpler calculation
    df = handler.convert_date_to_datetime(df, ['aimedArrivalTime','arrivalTime','aimedDepartureTime','departureTime'])

    #Merges duplicated arrival and departure times
    df = handler.merge_duplicated_stop_times(df)

    return df

def feature_engineering(df):
    #Creates new features corresponding to the next stop
    df = handler.append_next_stop(df)

    #Calculate delay between true stop time and aimed stop time
    df = handler.calculate_delay(df)

    #
    df = handler.calculate_delay_change(df)

    #Calculates average time between stops
    df = handler.calculate_time_between_stops(df)

    return df


def test_connection():
    """
    Test connection to Entur and Frost APIs
    """

    enturJP = EnturAPI()
    if enturJP.test_connection():
        print("Successfully connected to Entur API!")
    else:
        print("Failed to connect to Entur API")
        return False

    enturSQL = EnturSQL()
    if enturSQL.test_connection():
        print("Successfully connected to Entur API!")
    else:
        print("Failed to connect to Entur API")
        return False

    frost = FrostAPI()
    if frost.test_connection():
        print("Successfully connected to Frost API!")
    else:
        print("Failed to connect to Frost API")
        return False

    return True

if __name__ == "__main__":
    bus_route = "RUT:Line:34" #The transit route to analyse
    start_date = "2024-01-01" #The date which to start analysing from
    end_date = "2024-12-31" #The date which to end analysing from
    target_times = ['08:00:00','13:00:00', '17:00:00', '22:00:00'] # The times of each day to analyse

    fetcher = DataFetcher()
    handler = DataHandler()


    connection = True #test_connection()

    if connection:
        #main(bus_route, start_date, end_date, target_times)
        
        df = handler.load_processed_entur_data("rut-line-34_2024-01-01-2024-12-31_20250306_160847_processed.csv")
        df = feature_engineering(df)
        handler.save_processed_data(df,"rut-line-34_2024-01-01-2024-12-31_20250306_160847_processed.csv")
    else:
        print("Connection with online services not established.")