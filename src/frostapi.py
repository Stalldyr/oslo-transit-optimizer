import requests
import os
from dotenv import load_dotenv

load_dotenv()

class FrostAPI:
    """
    Client for interacting with the Frost weather API.

    Full API reference documentation and concepts, including possible operations, parameters, components and specifications, can be found at:
        - https://frost.met.no/api.html
        - https://frost.met.no/concepts2.html

    Commonly used weather elements for transit analysis:
        Precipitation:
            - precipitation_amount: Current precipitation in mm. Updated once a minute.
            - sum(precipitation_amount P1H): Hourly precipitation in mm
            - sum(precipitation_amount PT10M): 10-minute precipitation in mm
            - sum(duration_of_precipitation PT1M): Precipitation iper minute (precipation intensity)
            - sum(duration_of_precipitation PT1H): Duration of precipitation last hour in seconds
            - sum(duration_of_precipitation_as_snow P1H): Number of minutes with snow the last hour
            - sum(duration_of_precipitation_as_rain P1H): Number of minutes with rain the last hour
            - sum(duration_of_precipitation_as_hail P1H): Number of minutes with hail the last hour
            - sum(duration_of_precipitation_as_drizzle P1H): Number of minutes with drizzle the last hour

        Temperature:
            - air_temperature: Current air temperature in Celsius 
            - mean(air_temperature P1H): Mean air temperature last hour in Celsius
            - min(air_temperature P1H): Minimum air temperature last hour in Celsius
            - max(air_temperature P1H): Maximum air temperature last hour in Celsius

        Humidity:
            - relative_humidity: Relative humidity as percentage
            - mean(relative_humidity PT1H): Mean relative humidity in percentage the last hour
        
        Wind:
            - mean(wind_speed P1H): Mean wind speed last hour in m/s
            - max(wind_speed P1H): Maximum wind speed last hour in m/s
        
        Road Conditions:
            - surface_temperature: Road surface temperature in Celsius
            - mean(surface_snow_thickness PT1H): Snow depth in cm last hour
        
        Time Interval Notation:
            - PT10M: Last 10 minutes
            - P1H: Last 1 hour
            - P1D: Last 24 hours

    Full reference table for weather and climate elements be found at:
        - https://frost.met.no/elementtable

    """
    
    #API request paths
    BASE_URL = 'https://frost.met.no/'
    OBSERVATIONS_PATH = 'observations/v0.jsonld'
    SOURCES_PATH = 'sources/v0.jsonld'

    def __init__(self):
        self.client_id = os.getenv("FROST_CLIENT_ID")
        self.client_secret = os.getenv("FROST_CLIENT_SECRET")

    #╔════════════════════════════════════════════════════════════════════╗
    #║                          API REQUEST                               ║
    #╚════════════════════════════════════════════════════════════════════╝ 

    def execute_query(self, parameters: dict, url: str) -> dict:
        """
        Execute a HTTP request against the Frost API
        
        Args:
            parameters (dict): parameters
            
        Returns:
            dict: Response from the API
        """
        
        try:
            response = requests.get(self.BASE_URL + url, params=parameters,  auth=(self.client_id,''))
            response.raise_for_status()

            result = response.json()

            if "error" in result:
                print(f"{result['error']['code']} error: {result['error']['message']}. {result['error']['reason']}")
                return None
            else:
                return result
            
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {str(e)}")
            return None
        
    #╔════════════════════════════════════════════════════════════════════╗
    #║                          API QUERIES                               ║
    #╚════════════════════════════════════════════════════════════════════╝    
        
    def get_weather_data(self, parameters: dict):
        """
        Get weather observations data
        
        Args:
            parameters: Dictionary containing parameters:
                - sources (required): Weather station ID (e.g., 'SN18700' for Oslo-Blindern)
                - elements (required): Comma-separated list of weather elements to fetch
                - referencetime (required): Time period for data (e.g., '2024-02-24/2024-02-25'). 'latest' returns the most recent data. 
                - maxage: Maximum age of data, appliable only when referencetime is set to 'latest' (e.g. 'PT1H' for 1 hour.)
                - limit: Maximum number of observations to return
                - timeoffsets: Time offset relative to midnight for each observation
                - timeresolutions: The period between each data value, i.e. data output frequency

        Returns:
            Weather observations data or None if request failed
        """

        url = self.OBSERVATIONS_PATH

        response = self.execute_query(parameters, url)

        return response


    def get_weather_stations(self, parameters: dict):
        """
        Get weather stations based on location criteria
        
        Args:
            parameters: Dictionary containing location filters such as:
                - municipality: Name of municipality (e.g., 'Oslo')
                - county: Name of county (e.g., 'Viken')
                - country: Country name (e.g., 'Norway')
                - near: Coordinate pair (e.g., '59.9139,10.7522')
                - within: WKT polygon of area to search within
                
        Returns:
            Weather stations data or None if request failed
        """

        url = self.SOURCES_PATH

        response = self.execute_query(parameters,url)

        return response
    
    def test_connection(self) -> bool:
        """
        Test the connection to the Frost API
        
        Returns:
            True if connection is successful, False otherwise
        """

        test_params = {
            'sources': 'SN18700',
            'elements': 'air_temperature',
            'referencetime': 'latest'
        }

        result = self.get_weather_data(test_params)
        return result is not None