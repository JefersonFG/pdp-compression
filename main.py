import src.parquet as parquet
import src.avro as avro
import src.orc as orc
import src.util as util
import src.dataset as dataset

DATASET_PATH = 'data/dataset.csv'

if __name__ == '__main__':
    rows = 50
    float_cols = 2
    int_cols = 2
    string_cols = 2
    char_cols = 2

    generated_dataset = dataset.make_random_dataset(rows, float_cols, int_cols, string_cols, char_cols)
    generated_dataset.to_csv(DATASET_PATH)
    print('Original file size: {} bytes'.format(util.get_readable_file_size(DATASET_PATH)))

    parquet.evaluate_compression(generated_dataset)
    avro.evaluate_compression(generated_dataset)
    orc_schema = orc.generate_schema(list(generated_dataset), float_cols, int_cols, string_cols, char_cols)
    orc.evaluate_compression(generated_dataset, orc_schema)
