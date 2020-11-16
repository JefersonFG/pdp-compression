import pandas as pd

import src.parquet as parquet
import src.util as util

DATASET_PATH = 'data/US_Accidents_June20.csv'

if __name__ == '__main__':
    csv_data = pd.read_csv(DATASET_PATH)

    print('Original file size: {} bytes'.format(util.get_readable_file_size(DATASET_PATH)))
    parquet.evaluate_compression(csv_data)
