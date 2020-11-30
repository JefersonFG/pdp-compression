import pyarrow as pa
import pyarrow.parquet as pq
from timeit import default_timer as timer
import src.util as util

OUTPUT_FILE_PATH = 'output/output.parquet'


def evaluate_compression(df):
    print('Evaluating parquet compression')
    table = pa.Table.from_pandas(df)

    # none, snappy, gzip, brotli, lz4, zstd
    write_with_compression(table, 'none')
    write_with_compression(table, 'snappy')
    write_with_compression(table, 'gzip')
    write_with_compression(table, 'brotli')
    write_with_compression(table, 'lz4')
    write_with_compression(table, 'zstd')


def write_with_compression(table, compression):
    start = timer()
    pq.write_table(table, OUTPUT_FILE_PATH, compression=compression)
    end = timer()
    print('Time to write parquet with {} compression: {} seconds'.format(compression, end - start))
    print('Resulting size: {}'.format(util.get_readable_file_size(OUTPUT_FILE_PATH)))
