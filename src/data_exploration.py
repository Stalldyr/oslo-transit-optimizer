import pandas as pd
import json
import os
from datetime import datetime

class DataExplorer():
    def __init__(self,df):
        self.df = df

        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(current_dir)
        self.log_dir = os.path.join(project_root,'logs')

    #╔════════════════════════════════════════════════════════════════════╗
    #║                         DATA EXPLORATION                           ║
    #╚════════════════════════════════════════════════════════════════════╝
    def full_exploration(self, output = 'dict'):
        '''Performs exploratory data analysis'''

        results = {
            'basic_info': self.get_data_shape(),
            'dtypes': self.get_dtypes(),
            'unique_values': self.get_unique_values(),
            'binary_ratios': self.get_binary_feature_ratios(),
            'numerical_stats': self.get_numerical_statistics(),
            'missing_values': self.get_missing_values(),
            'sample_data': self.get_sample_data(5)
        }

        return self._handle_output(results, output)
    
    def custom_exploration(self, output = "dict", *args):
        pass

    def get_data_shape(self, output = 'dict'):
        '''Provides an overview of the data set'''

        result = {
            'rows': self.df.shape[0],
            'columns': self.df.shape[1]
        }

        return self._handle_output(result, output)
    
    def get_dtypes(self, output = 'dict'):
        '''Prints the datatype of each feature'''

        dtypes_dict = {col: str(dtype) for col, dtype in self.df.dtypes.items()}

        result = {
            'column_types': dtypes_dict
        }

        return self._handle_output(result, output)

    def get_missing_values(self, output = 'dict'):
        "Checks how much of each column which contains empty values"

        missing = self.df.isnull().sum()
        missing_percent = missing/self.df.shape[0]*100
        result = {
            'total_missing_values': int(missing.sum()),
            'percent_values_missing': float(missing.sum()/self.df.size),
            'by_column': {}
        }

        for col in self.df.columns:
            if missing[col] > 0:
                result['by_column'][col] = {
                    'count': int(missing[col]),
                    'percentage': float(missing_percent[col])
                }

        return self._handle_output(result, output)


    def get_unique_values(self, max_values = 10, output = 'dict'):
        '''Prints the unique values of each feature'''

        result = {}

        for col in self.df.select_dtypes(['object', 'category', 'bool']):
            unique_vals = self.df[col].unique()
            count = len(unique_vals)

            if count > max_values:
                unique_vals = unique_vals[:max_values]
                unique_vals.append(f"... {count - max_values} more values")
            
            result[col] = {
                'n_uniques': count,
                'unique_values': unique_vals
            }

        return self._handle_output(result, output)
    
    def get_binary_feature_ratios(self, output = 'dict'):
        '''Calculates the ratio of boolean values'''

        result = {}

        for col in self.df.select_dtypes("bool"):
            result[col] = {
                'ratio_true': float(self.df[col].mean()),
                'counts': {
                    'True': int(self.df[col].sum()),
                    'False': int((~self.df[col]).sum())
                }
            }


        for col in self.df.select_dtypes(include=['object', 'category']):
            if self.df[col].nunique() == 2:
                counts = self.df[col].value_counts()
                ratios = self.df[col].value_counts(normalize=True)
                
                result[col] = {
                    'counts': {str(key): int(val) for key, val in counts.items()},
                    'ratios': {str(key): float(val) for key, val in ratios.items()}
                }

        return self._handle_output(result, output)
        

    def get_numerical_statistics(self, output = 'dict'):
        '''Calculates the metrics for numerical features'''

        result = {}

        for col in self.df.select_dtypes("number"):
            stats = self.df[col].describe()

            result[col] = {}

            for stat, value in stats.items():
                result[col][stat] = value

        return self._handle_output(result, output)
    
    def get_duplicates(self, output = 'dict'):
        duplicated = self.df.duplicated().sum()

        result = {
            'duplicated_rows': int(duplicated),
            'duplicated_percantege': float(duplicated/self.df.shape[0])
        }

        return self._handle_output(result, output)
    
    def get_time_analysis(self, output = 'dict'):
        #UNFINISHED

        result ={}

        for col in self.df.select_dtypes('datetime'):
            print(col)


        return self._handle_output(result, output)

    def get_sample_data(self, n = 5, output = 'dict'):
        '''Get a sample of data from the dataframe.'''

        sample = self.df.head(n)

        result = {
            'columns': list(sample.columns),
            'rows': sample.to_dict(orient='records')
        }

        return self._handle_output(result, output)
    
    #╔════════════════════════════════════════════════════════════════════╗
    #║                       TRANSIT SPECIFIC                             ║
    #╚════════════════════════════════════════════════════════════════════╝ 

    def get_directon_stats(self,output):
        result = {}

        for direction in self.df['directionRef'].unique():
            dir_df = self.df[self.df['directionRef'] == direction]
            subset_df = dir_df[['delayMinutes','timeToNextStopMinutes']]

            result[direction] = self._calculate_numerical_statistics(subset_df)

        return self._handle_output(result, output)
    


    
    #╔════════════════════════════════════════════════════════════════════╗
    #║                         HELPER FUNCTIONS                           ║
    #╚════════════════════════════════════════════════════════════════════╝

    def _handle_output(self, result, output):

        if output == "dict":
            return result
        
        elif output == "print":
            print(json.dumps(result, indent=2, default=str, ensure_ascii=False))

        elif output == "txt":
            time = datetime.now().strftime('%d/%m/%Y %H:%M:%S')

            log_path = os.path.join(self.log_dir, "analytics_log.txt")

            with open(log_path,'a', encoding='utf-8') as file:
                file.write(time + "\n")
                json.dump(result, file, indent=2, default=str, ensure_ascii=False)

                        
    def _calculate_numerical_statistics(self, dataframe):
        """Extract the core functionality from get_numerical_statistics to operate on any dataframe"""
        result = {}

        for col in dataframe.select_dtypes("number"):
            stats = dataframe[col].describe()
            result[col] = {}
            for stat, value in stats.items():
                result[col][stat] = value
                
        return result



