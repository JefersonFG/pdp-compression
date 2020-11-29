import pyorc
from timeit import default_timer as timer
import src.util as util

OUTPUT_FILE_PATH = 'output/accidents.orc'


def generate_schema(col_list, float_cols, int_cols, string_cols, char_cols):
    schema = "struct<"
    i = 0

    for _ in range(float_cols):
        schema += col_list[i] + ':float,'
        i += 1

    for _ in range(int_cols):
        schema += col_list[i] + ':float,'
        i += 1

    for _ in range(string_cols):
        schema += col_list[i] + ':string,'
        i += 1

    for _ in range(char_cols):
        schema += col_list[i] + ':char,'
        i += 1

    # Remove last comma
    schema = schema[:-1]
    schema += ">"
    return schema


def evaluate_compression(df, schema):
    print("Evaluating orc compression")

    with open(OUTPUT_FILE_PATH, "wb") as f:
        # NONE, ZLIB
        with pyorc.Writer(f, schema, compression=pyorc.CompressionKind.NONE,
                          compression_strategy=pyorc.CompressionStrategy.COMPRESSION) as writer:
            start = timer()
            for i in range(len(df)):
                writer.write(tuple([x for x in df.iloc[i, :]]))
    end = timer()
    print('Time to write orc: {} seconds'.format(end - start))
    print('Resulting size: {}'.format(util.get_readable_file_size(OUTPUT_FILE_PATH)))
