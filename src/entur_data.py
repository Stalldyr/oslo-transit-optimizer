import requests
from datetime import datetime, timedelta
from google.cloud import bigquery
import pandas_gbq as pdgbq
import functools

class EnturAPI:
    def __init__(self, client_name="oslo-transit-optimizer"):
        """
        Initialize the Entur API client
        
        Args:
            client_name (str): Name of your application for identification
        """

        self.endpoint = "https://api.entur.io/journey-planner/v3/graphql"
        self.endpointrt = "https://api.entur.io/realtime/v1/rest/sx"
        self.headers = {
            "ET-Client-Name": client_name,
            "Content-Type": "application/json",
        }

    #╔════════════════════════════════════════════════════════════════════╗
    #║                         API REQUESTS                               ║
    #╚════════════════════════════════════════════════════════════════════╝    
    
    def execute_query(self, query: str, variables: dict = None) -> dict:
        """
        Execute a GraphQL query against the Entur API
        
        Args:
            query (str): GraphQL query string
            variables (dict): Variables for the query (optional)
            
        Returns:
            dict: Response from the API
        """

        payload = {
            "query": query,
            "variables": variables or {}
        }
        
        try:
            response = requests.post(self.endpoint, json=payload, headers=self.headers)
            response.raise_for_status()

            try: 
                result = response.json()

                if "errors" in result:
                    print(f"GraphQL Error: {result['errors'][0]['message']}")
                    return None
                    
                return result
            
            except ValueError as e:
                print(f"JSON Parse Error: {e}")
                return None
            

        except Exception as e:
            print(str(e))
            return None
                

    def test_connection(self) -> bool:
        """
        Test the connection to the Entur API with a simple query
        
        Returns:
            bool: True if connection is successful
        """
        test_query = """
        {
            stopPlace(id: "NSR:StopPlace:4000") {
                name
                id
            }
        }
        """

        response = self.execute_query(test_query)
        return response is not None
    

    #╔════════════════════════════════════════════════════════════════════╗
    #║                          API QUERIES                               ║
    #╚════════════════════════════════════════════════════════════════════╝

    def get_stop_info(self, stop_id: str = "NSR:StopPlace:4000") -> dict:
        """
        Get information about a specific bus stop
        
        Args:
            stop_id (str): The ID of the stop to query
            
        Returns:
            dict: Information about the stop
        """

        query = """
        query GetStpInfo($id: String!) {
            stopPlace(id: $id) {
                id
                latitude
                longitude
                name
                description
                quays {
                    id
                    name
                    publicCode
                    lines {
                        id
                        name
                        transportMode
                    }
                }
            }
        }
        """
        variables = {"id": stop_id}
        
        response = self.execute_query(query, variables)
    
        if response["data"]["stopPlace"] is not None:
            return response["data"]["stopPlace"]
        else:
            print("Error: StopID does not exist")

        
    def get_line_info(self, line_id: str) -> dict:
        """
        Get information about a specific bus line
        
        Args:
            line_id (str): The ID of the line to query
            
        Returns:
            dict: Information about the line including its stops
        """
        
        query = """
        query GetLineInfo($lineId: ID!) {
            line(id: $lineId) {
                id
                name
                transportMode
                quays {
                    id
                    name
                }
            }
        }
        """
        variables = {"lineId": line_id}
        
        response = self.execute_query(query, variables)

        if response["data"]["line"] is not None:
            return response["data"]["line"]
        else:
            print("Error: LineID does not exist")

    def get_journey_info(self, journey_id: str) -> dict:
        #UNFINISHED
        """
        Get information about a specific bus journey
        
        Args:
            journey_id (str): The ID of the journey to query
            
        Returns:
            dict: Information about the journey including its stops
        """
        
        query = """
        query GetLineInfo($journeyId: String!) {
            serviceJourney(id: $journeyId) {
                id
                directionType
                transportMode
                line {
                    id
                    name
                    }
                activeDates
            }
        }
        """
        variables = {"journeyId": journey_id}
        
        response = self.execute_query(query, variables)

        if response["data"]["serviceJourney"] is not None:
            return response["data"]["serviceJourney"]
        else:
            print("Error: JourneyId does not exist")

    
    def get_realtime_journeys(self, line_id: str) -> dict:
        """
        Get only journeys that have real-time data
        
        Args:
            line_id (str): The ID of the bus line (e.g., "RUT:Line:31")
            
        Returns:
            dict: Information about journeys with real-time data
        """
        query = """
        query ($lineId: ID!) {
            line(id: $lineId) {
                id
                name
                transportMode
                serviceJourneys {
                    id
                    estimatedCalls {
                        actualArrivalTime
                        actualDepartureTime
                        aimedArrivalTime
                        aimedDepartureTime
                        expectedArrivalTime
                        expectedDepartureTime
                        realtime
                        quay {
                            name
                        }
                    }
                }
            }
        }
        """
        
        variables = {"lineId": line_id}
        response = self.execute_query(query, variables)

        # Filter to only keep journeys that have any real-time data
        if response["data"]["line"] is not None:
            realtime_journeys = []
            for journey in response["data"]["line"]["serviceJourneys"]:
                has_realtime = any(call["realtime"] is True for call in journey["estimatedCalls"])
                if has_realtime:
                    journey["estimatedCalls"] = [call for call in journey["estimatedCalls"] if call["realtime"] is True]
                    realtime_journeys.append(journey)
            
            response["data"]["line"]["serviceJourneys"] = realtime_journeys
            return response["data"]
        else:
            print("Error: LineID does not exist")
    

class EnturSQL:
    """
    requires gcloud CLI to be installed and authenticated

    https://data.entur.no/domain/public-transport-data/product/realtime_siri_et/urn:li:container:1d391ef93913233c516cbadfb190dc65
    """
    def __init__(self, project_id=None):
            self.client = bigquery.Client(project=project_id)
            self.exceptions = ["recordedAtTime", "datedServiceJourneyId", "operatorRef", "vehicleMode", "dataSource", "dataSourceName"]
            self.table_id = "`ent-data-sharing-ext-prd.realtime_siri_et.realtime_siri_et_last_recorded`"

    def build_query(self, conditions, limit = None, select = None):
        limit_clause = f"LIMIT {limit}" if limit else ""
        select_clause = f"SELECT {select}" if select else f"SELECT * EXCEPT ({self._list_to_string(self.exceptions)})"
        
        query = f'''
            {select_clause}
            FROM {self.table_id}
            WHERE {conditions}
            {limit_clause}
            '''
        return query

    
    #╔════════════════════════════════════════════════════════════════════╗
    #║                          SQL REQUESTS                              ║
    #╚════════════════════════════════════════════════════════════════════╝
        
    def query_to_dataframe(self, query):
        """
        Execute a SQL query against the BigQuery API and returns a DataFrame
        """
        return pdgbq.read_gbq(query,dialect='standard', project_id=self.client.project)

    #╔════════════════════════════════════════════════════════════════════╗
    #║                          SQL QUERIES                               ║
    #╚════════════════════════════════════════════════════════════════════╝

    def test_connection(self):
        
        conditions = 'operatingDate = "2024-08-10" AND lineRef = "RUT:Line:34"'
        query = self.build_query(conditions, limit=10)
        
        return self.query_to_dataframe(query)
    
    def get_data_by_lineid(self, line_id, start_date, end_date, limit=None, execute = True):
        
        conditions = f'''operatingDate BETWEEN "{start_date}"
                    AND "{end_date}" AND lineRef = "{line_id}"
                    '''
        query = self.build_query(conditions, limit=limit)

        return self.query_to_dataframe(query) if execute else query
    
    def get_data_by_journeyids(self, journey_ids:list, start_date, end_date, limit=None, execute = True):
        
        conditions = f'''
                    operatingDate BETWEEN "{start_date}" AND "{end_date}" 
                    AND serviceJourneyId IN ({self._list_to_quoted_string(journey_ids)})
                    '''
        query = self.build_query(conditions, limit=limit)
        
        return self.query_to_dataframe(query) if execute else query
    
    def get_data_by_lineid_and_timeframe(self, line_id, start_date, end_date, start_time, end_time, days_of_week = None, limit=None, journey_ids=None, execute = True):
        
        day_condition = ""
        if days_of_week:
            day_condition = f"AND dayOfTheWeek IN ({self._list_to_string(days_of_week)})"

        if not journey_ids: 
            journey_ids = self.get_journey_id_by_lineid_and_timeframe(line_id, start_date, end_date, start_time, end_time, limit=limit, execute=False)
        
        conditions = f'''
                    lineRef = "{line_id}"
                    AND serviceJourneyId IN ({journey_ids})
                    AND operatingDate BETWEEN "{start_date}" AND "{end_date}"
                    {day_condition}
                    '''
        
        query = self.build_query(conditions, limit=limit)
        
        return self.query_to_dataframe(query) if execute else query

    
    def get_journey_id_by_lineid_and_timeframe(self, line_id, start_date, end_date, start_time, end_time, limit=None, execute = True):
        """
        Get the ID of journeys that starts its route within a specified timeframe
        """


        select = "serviceJourneyId"

        conditions = f'''
                    operatingDate BETWEEN "{start_date}" AND "{end_date}"
                    AND lineRef = "{line_id}"
                    AND sequenceNr = 1
                    AND TIME(aimedDepartureTime) BETWEEN "{start_time}" AND "{end_time}"
                    '''
        query = self.build_query(conditions, limit=limit, select=select)

        return self.query_to_dataframe(query) if execute else query
    
    #╔════════════════════════════════════════════════════════════════════╗
    #║                             HELPER                                 ║
    #╚════════════════════════════════════════════════════════════════════╝

    def _list_to_string(self, lst):
        return ", ".join([f"{item}" for item in lst])
    
    def _list_to_quoted_string(self, lst):
        return ", ".join([f"'{item}'" for item in lst])
