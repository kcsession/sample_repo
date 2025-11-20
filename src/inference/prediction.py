import yaml
import pickle
import pandas as pd
from typing import Optional

class Predictor:
    """
    Loads model, prepares data, and generates predictions.
    """
    def __init__(self):
        try:
            # Load configuration
            with open("/Workspace/Users/hilgdna.belagavi@gmail.com/sample_repo/config/config.yaml", "r") as f:
                config = yaml.safe_load(f)

            model_path = config['artefacts']['model_path']
            features_path = config['artefacts']['features_path']

            # Load model
            with open(model_path, "rb") as f:
                self.model = pickle.load(f)

            # Load features
            with open(features_path, "rb") as f:
                self.feature = pickle.load(f)

        except FileNotFoundError as fnf:
            print(f"FileNotFoundError: Config or model file not found. Details: {str(fnf)}")
            self.model, self.feature = None, None

        except KeyError as ke:
            print(f"KeyError: Missing key in config.yaml. Details: {str(ke)}")
            self.model, self.feature = None, None

        except Exception as e:
            print(f"Unexpected error in Predictor init: {type(e).__name__} - {str(e)}")
            self.model, self.feature = None, None

    def predict(self, pdf: pd.DataFrame) -> Optional[pd.Series]:
        """
        Predict from already preprocessed Pandas DataFrame.
        """
        try:
            if self.model is None or self.feature is None:
                raise RuntimeError("Model or features not loaded properly.")

            X = pdf[self.feature]
            y_pred = self.model.predict(X)
            return y_pred

        except KeyError as ke:
            print(f"KeyError: Missing required feature columns in input DataFrame. Details: {str(ke)}")
            return None

        except AttributeError as ae:
            print(f"AttributeError: Model object is invalid. Details: {str(ae)}")
            return None

        except Exception as e:
            print(f"Unexpected error during prediction: {type(e).__name__} - {str(e)}")
            return None
