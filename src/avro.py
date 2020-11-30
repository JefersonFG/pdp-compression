import pandavro as pdx
from timeit import default_timer as timer
import src.util as util

OUTPUT_FILE_PATH = 'output/output.avro'


def evaluate_compression(df):
    print('Evaluating avro compression')

    # null, deflate, snappy, zstandard, bzip2, lz4, xz
    write_with_compression(df, 'null')
    write_with_compression(df, 'deflate')
    write_with_compression(df, 'snappy')
    write_with_compression(df, 'zstandard')
    write_with_compression(df, 'bzip2')
    write_with_compression(df, 'lz4')
    write_with_compression(df, 'xz')


def write_with_compression(df, compression):
    start = timer()
    pdx.to_avro(OUTPUT_FILE_PATH, df, codec=compression)
    end = timer()
    print('Time to write avro with {} compression: {} seconds'.format(compression, end - start))
    print('Resulting size: {}'.format(util.get_readable_file_size(OUTPUT_FILE_PATH)))
