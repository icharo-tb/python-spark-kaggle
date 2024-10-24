from utils.etl_functions import extract_kaggle_dataset

if __name__ == "__main__":
    assets_path = r'/home/daniel-kairos/workspace/python-spark/assets'
    extract_kaggle_dataset("ankulsharma150/netflix-data-analysis", assets_path, False)