import pandavro as pdx
from timeit import default_timer as timer
import src.util as util

OUTPUT_FILE_PATH = 'output/accidents.avro'


def evaluate_compression(df):
    print('Evaluating avro compression')

    start = timer()
    pdx.to_avro(OUTPUT_FILE_PATH, df)
    end = timer()
    print('Time to write avro: {} seconds'.format(end - start))
    print('Resulting size: {}'.format(util.get_readable_file_size(OUTPUT_FILE_PATH)))
