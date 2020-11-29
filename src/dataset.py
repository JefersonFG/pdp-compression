# Imported from https://github.com/niczky12/medium/blob/master/tech/bigquery/benchmarks/file_loads.py

import pandas as pd
from hashlib import sha1
from sklearn import datasets


def make_int(x: int) -> int:
    return int(x * 100_000)


def make_string(x: int) -> str:
    return sha1(str(x).encode()).hexdigest()


def make_char(x: int) -> str:
    return chr(97 + (make_int(x) % 26))


def col_name_formatter(x: int) -> str:
    return f"col_{x:04d}"


def make_random_dataset(
    rows: int,
    float_cols: int = 1,
    int_cols: int = 1,
    string_cols: int = 1,
    char_cols: int = 1,
) -> pd.DataFrame:

    functions = [
        lambda x: x,
        make_int,
        make_string,
        make_char,
    ]

    quantities = [
        float_cols,
        int_cols,
        string_cols,
        char_cols,
    ]

    total_columns = sum(quantities)

    X, _ = datasets.make_regression(
        n_samples=rows, n_features=total_columns
    )

    df = pd.DataFrame(
        X,
        columns=[
            col_name_formatter(i)
            for i in range(total_columns)
        ],
    )

    start_index = 0

    for fn, quantity in zip(functions, quantities):
        if quantity == 0:
            continue

        end_index = start_index + quantity
        df.iloc[:, start_index:end_index] = df.iloc[
            :, start_index:end_index
        ].applymap(fn)
        start_index = end_index

    return df
