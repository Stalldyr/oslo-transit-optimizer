from entur_data import EnturAPI, EnturSQL
from frostapi import FrostAPI
from data_fetcher import DataFetcher
from data_handler import DataHandler


def collect_and_process_data(route_id="RUT:Line:34"):
    """Main function to collect and process transit data"""
    fetcher = DataFetcher()
    handler = DataHandler()
    
    raw_data = fetcher.collect_trip_data(route_id)
    if raw_data is not None:
        handler.save_raw_data(raw_data, route_id)


def test_connection():
    """
    Test connection to Entur and Frost APIs
    """

    entur = EnturAPI()
    if entur.test_connection():
        print("Successfully connected to Entur API!")
    else:
        print("Failed to connect to Entur API")

    frost = FrostAPI()
    if frost.test_connection():
        print("Successfully connected to Frost API!")
    else:
        print("Failed to connect to Frost API")


if __name__ == "__main__":
    test_connection()

    collect_and_process_data()