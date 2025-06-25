import pulp
import pandas as pd
#import numpy as np

class RouteOptimizer:
    def __init__(self, data_path, stop_pairs):
        self.data_path = data_path
        self.model = None
        #self.df = df
        self.stop_pairs = stop_pairs

    def build_optimization_model(self, max_schedule_change = 30):

        model = pulp.LpProblem("Route_Optimization", pulp.LpMinimize)
        
        stops = self.stop_pairs['stopPointName'].unique()

        departure_adjustments = {}
        for stop in stops:
            departure_adjustments[stop] = pulp.LpVariable(f"adj_{stop}", -max_schedule_change, max_schedule_change)

        travel_time_components = []
        mean_stop_count = self.stop_pairs['stopPointName'].value_counts().mean()
        for _, row in self.stop_pairs.iterrows():
            current_travel_time = row["travelTimeAvg"]

            weight = min(1, row['count'] / mean_stop_count)

            travel_time_components.append(current_travel_time * weight)


        model += pulp.lpSum(travel_time_components)


        for i, row in self.stop_pairs.iterrows():
            if pd.isna(row['nextStopPointName']):
                continue
                
            # Minimum travel time between stops (use 80% of current scheduled time as minimum)
            min_travel_time = 0.8 * row['scheduledTimeAvg']
            
            model += departure_adjustments[row['nextStopPointName']] - departure_adjustments[row['stopPointName']] >= min_travel_time

        total_current_time = self.stop_pairs['scheduledTimeAvg'].sum()
        model += pulp.lpSum([departure_adjustments[stop] for stop in stops]) <= 0.1 * total_current_time
        
        self.model = model
        return model

    def solve_model(self):
        if self.model is None:
            raise ValueError("Model has not been built yet. Call build_optimization_model() first.")
        
        self.model.solve()
        
        if self.model.status == pulp.LpStatusOptimal:
            print("Optimal solution found:")
            for v in self.model.variables():
                print(f"{v.name}: {v.varValue}")
            return True
        else:
            print("No optimal solution found.")
            return False

