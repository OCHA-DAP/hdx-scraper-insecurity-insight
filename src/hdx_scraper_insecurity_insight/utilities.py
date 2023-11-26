#!/usr/bin/env python
# encoding: utf-8

"""
Miscellaneous utilities, some borrowed from elsewhere
Ian Hopkinson 2023-11.20
"""

import csv
import json
import os

from typing import Any

from urllib3 import request
from urllib3.util import Retry


def fetch_json(dataset_name: str, use_sample: bool = False):
    if use_sample:
        json_response = fetch_json_from_samples(dataset_name)
    else:
        json_response = fetch_json_from_api(dataset_name)
    return json_response


def fetch_json_from_api(dataset_name: str) -> list[dict]:
    attributes = read_attributes(dataset_name)

    response = request(
        "GET", attributes["api_url"], timeout=60, retries=Retry(90, backoff_factor=1.0)
    )

    json_response = response.json()
    return json_response


def fetch_json_from_samples(dataset_name: str) -> list[dict]:
    attributes = read_attributes(dataset_name)
    with open(
        os.path.join(os.path.dirname(__file__), "api-samples", attributes["api_response_filename"]),
        "r",
        encoding="UTF-8",
    ) as api_response_filehandle:
        json_response = json.load(api_response_filehandle)
    return json_response


def read_attributes(dataset_name: str) -> dict:
    with open(
        os.path.join(os.path.dirname(__file__), "metadata", "attributes.csv"), "r", encoding="UTF-8"
    ) as attributes_filehandle:
        attribute_rows = csv.DictReader(attributes_filehandle)

        attributes = {}
        for row in attribute_rows:
            if row["dataset_name"] != dataset_name:
                continue
            if row["attribute"] == "resource":
                if "resource" not in attributes:
                    attributes["resource"] = [row["value"]]
                else:
                    attributes["resource"].append(row["value"])
            else:
                attributes[row["attribute"]] = row["value"]

    return attributes


def read_schema(dataset_name: str) -> dict:
    with open(
        os.path.join(os.path.dirname(__file__), "metadata", "schema.csv"), "r", encoding="UTF-8"
    ) as schema_filehandle:
        schema_rows = csv.DictReader(schema_filehandle)

        hdx_row = {}
        row_template = {}
        for row in schema_rows:
            if row["dataset_name"] != dataset_name:
                continue
            hdx_row[row["field_name"]] = row["terms"]
            row_template[row["field_name"]] = row["upstream"]

    return hdx_row, row_template


def write_dictionary(
    output_filepath: str, output_rows: list[dict[str, Any]], append: bool = True
) -> str:
    keys = list(output_rows[0].keys())
    newfile = not os.path.isfile(output_filepath)

    if not append and not newfile:
        os.remove(output_filepath)
        newfile = True

    with open(output_filepath, "a", encoding="utf-8", errors="ignore") as output_file:
        dict_writer = csv.DictWriter(
            output_file,
            keys,
            lineterminator="\n",
        )
        if newfile:
            dict_writer.writeheader()
        dict_writer.writerows(output_rows)

    status = _make_write_dictionary_status(append, output_filepath, newfile)

    return status


def _make_write_dictionary_status(append: bool, filepath: str, newfile: bool) -> str:
    status = ""
    if not append and not newfile:
        status = f"Append is False, and {filepath} exists therefore file is being deleted"
    elif not newfile and append:
        status = f"Append is True, and {filepath} exists therefore data is being appended"
    else:
        status = f"New file {filepath} is being created"
    return status
