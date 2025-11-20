import yaml
import pickle
import pandas as pd

class Predictor:
    """
    Loads model, prepares data, and generates predictions.
    """
    def __init__(self):
        with open("/Workspace/Users/hilgdna.belagavi@gmail.com/sample_repo/config/config.yaml", "r") as f:
            config = yaml.safe_load(f)
        
        model_path = config['artefacts']['model_path']
        features_path = config['artefacts']['features_path']

        with open(model_path, "rb") as f:
            self.model = pickle.load(f)
        
        with open(features_path, "rb") as f:
            self.feature = pickle.load(f)

    def predict(self, pdf: pd.DataFrame) -> pd.DataFrame:
        """
        Predict from already preprocessed Pandas DataFrame.
        """
        X = pdf[self.feature]
        y_pred = self.model.predict(X)

        return y_pred
