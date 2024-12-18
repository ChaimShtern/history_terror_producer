import os
import pandas as pd
import json
from toolz import partition_all


def read_csv_file_to_json(file_path):
    try:
        return pd.read_csv(file_path, encoding="utf-8").to_dict(orient="records")
    except UnicodeDecodeError:
        return pd.read_csv(file_path, encoding="latin1").to_dict(orient="records")


def split_json_to_batch(json_file, produce_funk):
    for batch in partition_all(200, json_file):
        produce_funk(json.dumps(batch))


global_terrorism_1000_rows_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'global_terrorism_1000_rows.csv')
