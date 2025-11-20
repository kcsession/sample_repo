import joblib
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error
from src.utils.logging import get_logger

logger = get_logger(__name__)

def evaluate():
    # Load model and test data
    model = joblib.load("artefacts/models/random_forest.pkl")
    X_test, y_test = joblib.load("artefacts/models/test_data.pkl")

    preds = model.predict(X_test)

    mae = mean_absolute_error(y_test, preds)
    rmse = np.sqrt(mean_squared_error(y_test, preds))

    logger.info(f"Evaluation complete. MAE: {mae:.4f}, RMSE: {rmse:.4f}")
    print("MAE:", mae)
    print("RMSE:", rmse)

if __name__ == "__main__":
    evaluate()
