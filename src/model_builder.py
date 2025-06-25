from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix
from sklearn.preprocessing import MinMaxScaler, StandardScaler, OneHotEncoder, OrdinalEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, cross_val_score, KFold
from sklearn.svm import SVC
import pandas as pd
import numpy as np


class ModelBuilder:
    def __init__(self, X, y, column_types, test_size=0.2, random_state=42, num_folds=5):
        self.X = X
        self.y = y

        self.column_types = column_types
        self.test_size = test_size
        self.random_state = random_state
        self.num_folds = num_folds

        self.model_type = None

        self.metrics = {}
        self.cv_results = {}
        self.conf_mat = None
        self.feature_importance = None

        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(self.X,self.y, test_size=self.test_size, random_state=self.random_state)


    def run_model(self, model_type):
        '''Runs the model and returns the results'''
        pipeline = self._setup_model(model_type)
        fitted_pipeline = self._train_model(pipeline)
        self._evaluate_model(fitted_pipeline)
        return fitted_pipeline
    
    def build_feature_transformer(self, scaler = StandardScaler()):
        '''Creates a preprocessor for the data'''
        transformers = []
        
        numerical_columns = [col for col in self.X_train.columns if col in self.column_types['numerical']]
        cateogrical_columns = [col for col in self.X_train.columns if col in self.column_types['categorical']]
        ordinal_columns = [col for col in self.X_train.columns if col in self.column_types["ordinal"].keys()]

        if numerical_columns:                            
            transformers.append(('num', scaler, numerical_columns))

        if cateogrical_columns:
            transformers.append(('cat', OneHotEncoder(handle_unknown='ignore'), cateogrical_columns))    
        
        if ordinal_columns:
            transformers.append(('ord', OrdinalEncoder(categories=[self.column_types["ordinal"][order] for order in ordinal_columns]), ordinal_columns))

        self.preprocessor = ColumnTransformer(transformers=transformers)

        return self
    
    def get_scores(self):
        scores = {
            'metrics': self.metrics,
            'cv_results': self.cv_results,
            'confusion_matrix': self.conf_mat,
            'feature_importance': self.feature_importance
        }
        return scores
    


    def _create_pipeline(self, model):
        '''Creates a pipeline for the data'''
        return Pipeline([
            ('preprocessor', self.preprocessor),
            ('classifier', model)
        ])

    def _setup_model(self, model_type):
        '''Sets up model based on input'''
        
        #Build preprocessor if not already built
        if not hasattr(self, 'preprocessor'):
            self.build_feature_transformer()

        self.model_type = model_type
        model = self._get_model(model_type)

        return self._create_pipeline(model)
    
    def _get_model(self, model):
        '''Returns the model based on the input'''
        if model == 'logistic regression':
            return LogisticRegression(random_state=self.random_state)
        elif model == 'random forest':
            return RandomForestClassifier(random_state=self.random_state)
        elif model == 'svm':
            return SVC(random_state=self.random_state)
        elif model == 'xgboost':
            return XGBClassifier(
                n_estimators=200, 
                max_depth=5, 
                learning_rate=0.1,  
                random_state=self.random_state
            )
        else:
            raise ValueError('Model not found')
    
    def _train_model(self, pipeline):
        '''Trains the model and returns the predictions'''
        self._cross_validation(pipeline)
        return pipeline.fit(self.X_train, self.y_train)

    def _evaluate_model(self, fitted_pipeline):
        '''Predicts the output and evaluates the model'''
        y_pred = fitted_pipeline.predict(self.X_test)
        self._model_evaluation(y_pred)
        self._get_feature_importance(fitted_pipeline)

    
    def _cross_validation(self, pipeline):
        '''Performs cross-validation on the model'''
        kf = KFold(n_splits=self.num_folds, shuffle=True, random_state=self.random_state)
        self.cv_results = cross_val_score(pipeline, self.X, self.y, cv=kf)

    def _model_evaluation(self, y_pred):
        '''Calculates scores for the model'''
        metrics = {
            'accuracy': accuracy_score(self.y_test, y_pred),
            'precision': precision_score(self.y_test, y_pred),
            'recall': recall_score(self.y_test, y_pred),
            'f1': f1_score(self.y_test, y_pred)
        }
        
        conf_mat = confusion_matrix(self.y_test, y_pred)

        self.metrics = metrics
        self.conf_mat = conf_mat

    def _get_feature_importance(self, fitted_pipeline):
        '''Calculates the feature importance for the model'''

        model = fitted_pipeline.named_steps['classifier']
        preprocessor = fitted_pipeline.named_steps['preprocessor']
        
        # Get feature names after preprocessing
        feature_names = []
    
        for transformer_name, transformer, columns in preprocessor.transformers_:
            if transformer_name == 'num':
                # Numerical columns stay the same
                feature_names.extend(columns)
            elif transformer_name == 'cat':
                # Get the encoded feature names for categorical columns
                feature_names.extend(transformer.get_feature_names_out(columns))
            elif transformer_name == 'ord':
                # Ordinal columns stay the same
                feature_names.extend(columns)


        if hasattr(model, 'feature_importances_'):
            importance = model.feature_importances_
        elif hasattr(model, 'coef_'): 
            importance = np.abs(model.coef_[0])
        else:
            return None
            
        self.feature_importance = pd.DataFrame({
            'feature': feature_names,
            'importance': importance
        }).sort_values('importance', ascending=False)


    





        
