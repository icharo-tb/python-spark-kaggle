import os
from dotenv import load_dotenv

load_dotenv()

KAGGLE_API_KEY = os.getenv('KAGGLE_DATASET_API_KEY')