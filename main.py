import pandas as pd

import src.parquet as parquet
import src.avro as avro
import src.util as util
import src.dataset as dataset

DATASET_PATH = 'data/dataset.csv'

if __name__ == '__main__':
    csv_data = dataset.make_random_dataset(50000, 50, 50, 50, 50, 50)

    csv_data.to_csv(DATASET_PATH)

    print('Original file size: {} bytes'.format(util.get_readable_file_size(DATASET_PATH)))
    parquet.evaluate_compression(csv_data)
    avro.evaluate_compression(csv_data)
