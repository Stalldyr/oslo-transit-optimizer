class DataExplorer():
    def __init__(self,df):
        self.df = df
        self.txt_output = []

    #╔════════════════════════════════════════════════════════════════════╗
    #║                EXPLORATION, CLEANING, & ENGINEERING                ║
    #╚════════════════════════════════════════════════════════════════════╝
    def full_exploration(self):
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

        return results

    def get_data_shape(self):
        '''Provides an overview of the data set'''

        shape_info = {
            'rows': self.df.shape[0],
            'columns': self.df.shape[1]
        }

        return shape_info
    
    def get_dtypes(self):
        '''Prints the datatype of each feature'''

        dtypes_dict = {col: str(dtype) for col, dtype in self.df.dtypes.items()}

        dtype_info = {
            'column_types': dtypes_dict
        }

        return dtype_info

    def get_missing_values(self):
        "Checks how much of each column which contains empty values"

        missing = self.df.isnull().sum()
        missing_percent = missing/self.df.shape[0]*100
        nan_dict = {
            'total_missing_values': int(missing.sum()),
            'percent_values_missing': float(missing.sum()/self.df.size),
            'by_column': {}
        }

        for col in self.df.columns:
            if missing[col] > 0:
                nan_dict['by_column'][col] = {
                    'count': int(missing[col]),
                    'percentage': float(missing_percent[col])
                }

        return nan_dict


    def get_unique_values(self, max_values = 10):
        '''Prints the unique values of each feature'''

        unique_dict = {}

        for col in self.df.select_dtypes(['object', 'category', 'bool']):
            unique_vals = self.df[col].unique()
            count = len(unique_vals)

            if count > max_values:
                unique_vals = unique_vals[:max_values]
                #unique_vals.append(f"... {count - max_values} more values")
            
            unique_dict[col] = {
                'n_uniques': count,
                'unique_values': unique_vals
            }

        return unique_dict
    
    def get_binary_feature_ratios(self):
        '''Calculates the ratio of boolean values'''

        bool_dict = {}

        for col in self.df.select_dtypes("bool"):
            bool_dict[col] = float(self.df[col].mean()*100)


        for col in self.df.select_dtypes(include=['object', 'category']):
            if self.df[col].nunique() == 2:
                pass

        return bool_dict
        

    def get_numerical_statistics(self):
        '''Calculates the metrics for numerical features'''

        num_stats_dict = {}

        for col in self.df.select_dtypes("number"):
            stats = self.df[col].describe()

            num_stats_dict[col] = {}

            for stat, value in stats.items():
                num_stats_dict[col][stat] = value

        return num_stats_dict
    
    def get_duplicates(self):
        duplicated = self.df.duplicated().sum()

        duplicated_dict = {
            'duplicated_rows': int(duplicated),
            'duplicated_percantege': float(duplicated/self.df.shape[0])
        }

        return duplicated_dict
    
    def get_time_analysis(self):
        time_dict ={}

        for col in self.df.select_dtypes('datetime'):
            print(col)


        return time_dict

    def get_sample_data(self, n = 5):
        '''Get a sample of data from the dataframe.'''

        sample = self.df.head(n)

        sample_dict = {
            'columns': list(sample.columns),
            'rows': sample.to_dict(orient='records')
        }

        return sample_dict


