#!/usr/bin/env python
# encoding: utf-8

"""
This code is for establishing field mappings and HXL codes for the existing data to the new API
Ian Hopkinson 2023-11-18
"""

import datetime
import logging
import os
import sys

import pandas as pd

from hdx.utilities.easy_logging import setup_logging
from hdx.api.configuration import Configuration, ConfigurationError

from hdx_scraper_insecurity_insight.utilities import (
    read_attributes,
    write_schema,
    fetch_json_from_samples,
    # fetch_json_from_api,
    list_entities,
    print_banner_to_log,
    read_field_mappings,
    read_countries,
)

setup_logging()
LOGGER = logging.getLogger(__name__)
try:
    Configuration.create(
        user_agent_config_yaml=os.path.join(os.path.expanduser("~"), ".useragents.yaml"),
        user_agent_lookup="hdx-scraper-insecurity-insight",
    )
except ConfigurationError:
    LOGGER.info("Configuration already exists when trying to create in `create_datasets.py`")

FIELD_MAPPINGS = read_field_mappings()

EXPECTED_COUNTRY_LIST = list(read_countries().keys())

# Datamesh style schema file
SCHEMA_TEMPLATE = {
    "dataset_name": None,
    "upstream": None,  # API field name
    "field_name": None,  # Excel field name
    "field_number": None,
    "field_type": None,
    "terms": None,  # Use this for HXL tags
    "tags": None,
    "descriptions": None,
}


def marshall_datasets(dataset_name_pattern: str):
    print("*********************************************", flush=True)
    print("* Insecurity Insight - Generate schema.csv  *", flush=True)
    print(f"* Invoked at: {datetime.datetime.now().isoformat(): <23} *", flush=True)
    print("*********************************************", flush=True)
    print(f"Processing dataset: {dataset_name_pattern}\n", flush=True)
    status_list = []
    if dataset_name_pattern.lower() != "all":
        status = generate_schema(dataset_name_pattern)
        status_list.append(status)
    else:
        dataset_names = list_entities(type_="resource")

        LOGGER.info(f"Attributes file contains {len(dataset_names)} resource names")

        for dataset_name in dataset_names:
            LOGGER.info(f"Processing {dataset_name}")
            status = generate_schema(dataset_name, api_fields_basis=True)
            status_list.append(status)

    return status_list


def generate_schema(dataset_name: str, api_fields_basis: bool = False) -> str:
    # api_fields_basis sets whether we use the API as the soruce for column names or the
    # sample spreadsheets
    status = {
        "dataset_name": dataset_name,
        "n_api_fields": "",
        "n_spreadsheet_fields": "",
        "n_hxl_tags": "",
    }
    if "overview" in dataset_name:
        return status
    attributes = read_attributes(dataset_name)
    # Get relevant cached API response
    api_response = fetch_json_from_samples(dataset_name)
    api_fields = list(api_response[0].keys())

    try:
        resource_df = pd.read_excel(
            os.path.join(
                os.path.dirname(__file__),
                "spreadsheet-samples",
                attributes["legacy_resource_filename"],
            )
        )
        column_names = resource_df.columns.tolist()
        # Get HXL tags
        hxl_tags = resource_df.loc[0, :].values.flatten().tolist()
        hxl_tags = ["" if isinstance(x, float) else x for x in hxl_tags]
    except (FileNotFoundError, IsADirectoryError):
        print(f"No example spreadsheet provided for {dataset_name}", flush=True)
        resource_df = None
        column_names = api_fields
        hxl_tags = [""] * len(api_fields)

    if api_fields_basis:
        hxl_tag_dict = {column_names[i]: hxl_tags[i] for i in range(len(column_names))}
        column_names = api_fields
        hxl_tags = [hxl_tag_dict.get(x, "") for x in api_fields]
        resource_df = None
    #
    # Display Original fields, HXL and matching API field
    columns = zip(column_names, hxl_tags)

    # dataset_name,timestamp,upstream,field_name,field_number,field_type,terms,tags,description

    output_rows = []

    # timestamp = datetime.datetime.now().isoformat()

    print(f"\nEntries for the '{dataset_name}' endpoint", flush=True)
    print(f"{ '':<2}   {'Spreadsheet column':<50},{'HXL tag':<50}, {'api_field':<50}", flush=True)
    for i, column in enumerate(columns):
        if column[0].endswith(".1"):
            print(f"Column {column} is a duplicate", flush=True)
        # if i not in [50, 58]:
        #     continue
        api_field = find_corresponding_api_field(dataset_name, api_fields, column)

        print(f"{i:<2}.  {column[0]:<50.50},{column[1]:<50.50}, {api_field:<50.50}", flush=True)
        output_row = SCHEMA_TEMPLATE.copy()
        output_row["dataset_name"] = dataset_name
        # output_row["timestamp"] = timestamp
        output_row["upstream"] = api_field
        output_row["field_name"] = column[0]
        output_row["field_number"] = i
        output_row["field_type"] = ""
        output_row["terms"] = column[1]  # Use this for HXL tags
        output_row["tags"] = ""
        output_row["descriptions"] = ""

        output_rows.append(output_row)

    file_status = write_schema(dataset_name, output_rows)
    print(file_status, flush=True)
    n_hxl_tags = len([x for x in hxl_tags if x != ""])
    status = {
        "dataset_name": dataset_name,
        "n_api_fields": len(api_fields),
        "n_spreadsheet_fields": len(column_names),
        "n_hxl_tags": n_hxl_tags,
    }
    return status


# Collect the set of country ISO codes - this is repeated in the create_datasets code
def print_country_codes_analysis(api_response: list[dict]) -> (set, set):
    country_codes = {x["Country ISO"] for x in api_response}

    api_countries_not_in_hdx = country_codes.difference(set(EXPECTED_COUNTRY_LIST))
    print("\nCountries in API data but not currently on HDX", flush=True)
    print(api_countries_not_in_hdx, flush=True)

    hdx_countries_not_in_api = set(EXPECTED_COUNTRY_LIST).difference(country_codes)
    print("\nCountries on HDX but not in API data", flush=True)
    print(hdx_countries_not_in_api, flush=True)

    return api_countries_not_in_hdx, hdx_countries_not_in_api


def find_corresponding_api_field(dataset_name: str, api_fields: list, column: str) -> str:
    if dataset_name in FIELD_MAPPINGS:
        normalised_column = FIELD_MAPPINGS[dataset_name].get(column[0], column[0])
    else:
        normalised_column = column[0]
    api_field = ""
    if normalised_column in api_fields:
        api_field = normalised_column
    return api_field


def compare_api_to_samples(api_cache: dict[str], dataset_names: list = None) -> (bool, list[str]):
    print_banner_to_log(LOGGER, "Compare API")

    if dataset_names is None:
        dataset_names = list_entities(type_="resource")

    LOGGER.info(f"Found {len(dataset_names)} endpoints")

    api_changed = False
    changed_list = []
    for dataset_name in dataset_names:
        attributes = read_attributes(dataset_name)

        samples_response = fetch_json_from_samples(dataset_name)
        api_response = api_cache[dataset_name]

        sample_keys = samples_response[0].keys()
        if len(api_response) != 0:
            api_keys = api_response[0].keys()
        else:
            api_keys = []

        if sample_keys == api_keys:
            LOGGER.info(f"{dataset_name} matches")
        else:
            changed_list.append(dataset_name)
            LOGGER.info(f"**MISMATCH between API and sample for {dataset_name}")
            LOGGER.info(f"API endpoint: {attributes['api_url']}")
            LOGGER.info(f"API sample: {attributes['api_response_filename']}")
            LOGGER.info(f"Number of API records: {len(api_response)}")
            LOGGER.info(f"Number of sample records: {len(samples_response)}")
            LOGGER.info(f"Number of API endpoint record keys: {len(api_keys)}")
            LOGGER.info(f"Number of sample record keys: {len(sample_keys)}")
            LOGGER.info(f"API keys: {api_keys}")
            LOGGER.info(f"Sample keys: {sample_keys}")

            show_response_overlap(api_keys, sample_keys)
            api_changed = True

    return api_changed, changed_list


def show_response_overlap(api_keys: list[dict], sample_keys: list[dict]):
    LOGGER.info("Keys in API data but not in sample")
    LOGGER.info(set(api_keys).difference(set(sample_keys)))

    LOGGER.info("Keys in sample but not in API data")
    LOGGER.info(set(sample_keys).difference(api_keys))


if __name__ == "__main__":
    DATASET_NAME = "all"
    if len(sys.argv) == 2:
        DATASET_NAME = sys.argv[1]
    STATUS_LIST = marshall_datasets(DATASET_NAME)
    print(
        f"{'dataset_name':<50},{'n_api_fields':<20},{'n_spreadsheet_fields':<30},{'n_hxl_tags':<20}"
    )
    for STATUS in STATUS_LIST:
        print(
            f"{STATUS['dataset_name']:<50},"
            f"{STATUS['n_api_fields']:<20},"
            f"{STATUS['n_spreadsheet_fields']:<30},"
            f"{STATUS['n_hxl_tags']:<20}"
        )
