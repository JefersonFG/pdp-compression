import pyarrow as pa
import pyarrow.parquet as pq
from timeit import default_timer as timer
import src.util as util

OUTPUT_FILE_PATH = 'output/accidents.parquet'


def evaluate_compression(df):
    table = pa.Table.from_pandas(df)

    start = timer()
    pq.write_table(table, OUTPUT_FILE_PATH)
    end = timer()
    print('Time to write parquet: {} seconds'.format(end - start))
    print('Resulting size: {}'.format(util.get_readable_file_size(OUTPUT_FILE_PATH)))
