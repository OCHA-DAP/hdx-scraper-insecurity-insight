#!/usr/bin/env python
# encoding: utf-8

"""
This code is for establishing field mappings and HXL codes for the existing data to the new API
Ian Hopkinson 2023-11-18
"""

import csv
import datetime
import os

import pandas as pd

from hdx_scraper_insecurity_insight.utilities import (
    read_attributes,
    write_schema,
    fetch_json_from_samples,
)

FIELD_MAPPINGS = {}

with open(
    os.path.join(os.path.dirname(__file__), "metadata", "field_mappings.csv"),
    "r",
    encoding="UTF-8",
) as FIELD_MAPPINGS_FILEHANDLE:
    FIELD_MAPPING_ROWS = csv.DictReader(FIELD_MAPPINGS_FILEHANDLE)
    for ROW in FIELD_MAPPING_ROWS:
        if ROW["dataset_name"] not in FIELD_MAPPINGS:
            FIELD_MAPPINGS[ROW["dataset_name"]] = {}
        FIELD_MAPPINGS[ROW["dataset_name"]][ROW["field_name"]] = ROW["upstream"]
# FIELD_MAPPINGS = {
#     "Aid Workers Killed": "aidworkers_killed",
#     "Aid Workers Injured": "aidworkers_injured",
#     "Aid Workers Kidnapped": "aidworkers_kidnapped",
#     "Aid Workers Arrested": "aidworkers_arrested",
#     "SiND Event ID": "event_id",
#     "Type of Education Facility": "Type of education facility",
#     "Military Occupation of Education facility": "Military Occupation of Schools",
#     "Arson Attack on Education Facility": "",
#     "Forced Entry into Education Facility": "Forced Entry into Schools",
#     "Damage/Destruction To Education Facility": "Damage/Destruction To School Event",
# }

EXPECTED_COUNTRY_LIST = [
    "OPT",
    "MMR",
    "SDN",
    "SYR",
    "AFG",
    "BFA",
    "CMR",
    "UKR",
    "SSD",
    "COD",
    "ETH",
    "NGA",
    "YEM",
    "MLI",
    "NER",
    "IRQ",
    "LBY",
    "MOZ",
    "HTI",
    "SOM",
    "CAF",
    "MEX",
    "PAK",
    "IRN",
    "LBN",
]

# Datamesh style schema file
SCHEMA_TEMPLATE = {
    "dataset_name": None,
    "timestamp": None,
    "upstream": None,  # API field name
    "field_name": None,  # Excel field name
    "field_number": None,
    "field_type": None,
    "terms": None,  # Use this for HXL tags
    "tags": None,
    "descriptions": None,
}


def generate_schema(dataset_name: str) -> str:
    print("*********************************************", flush=True)
    print("* Insecurity Insight - Generate schema.csv  *", flush=True)
    print(f"* Invoked at: {datetime.datetime.now().isoformat(): <23} *", flush=True)
    print("*********************************************", flush=True)
    print(f"Processing dataset: {dataset_name}\n", flush=True)
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
    except FileNotFoundError:
        print(f"No example spreadsheet provided for {dataset_name}", flush=True)
        resource_df = None
        column_names = api_fields
        hxl_tags = [""] * len(api_fields)

    # Display Original fields, HXL and matching API field
    columns = zip(column_names, hxl_tags)

    # dataset_name,timestamp,upstream,field_name,field_number,field_type,terms,tags,description

    output_rows = []

    timestamp = datetime.datetime.now().isoformat()

    print(f"{ '':<2}   {'Spreadsheet column':<50},{'HXL tag':<50}, {'api_field':<30}", flush=True)
    for i, column in enumerate(columns):
        api_field = find_corresponding_api_field(dataset_name, api_fields, column)

        print(f"{i:<2}.  {column[0]:<50},{column[1]:<50}, {api_field:<30}", flush=True)
        output_row = SCHEMA_TEMPLATE.copy()
        output_row["dataset_name"] = dataset_name
        output_row["timestamp"] = timestamp
        output_row["upstream"] = api_field
        output_row["field_name"] = column[0]
        output_row["field_number"] = i
        output_row["field_type"] = ""
        output_row["terms"] = column[1]  # Use this for NXL tags
        output_row["tags"] = ""
        output_row["descriptions"] = ""

        output_rows.append(output_row)

    status = write_schema(dataset_name, output_rows)
    return status


# Collect the set of country ISO codes - this is repeated in the create_datasets code
def print_country_codes_analysis(api_response: list[dict]):
    country_codes = {x["Country ISO"] for x in api_response}
    print("\nCountries in API data but not currently on HDX", flush=True)
    print(country_codes.difference(set(EXPECTED_COUNTRY_LIST)))

    print("\nCountries on HDX but not in API data", flush=True)
    print(set(EXPECTED_COUNTRY_LIST).difference(country_codes))


def find_corresponding_api_field(dataset_name: str, api_fields: list, column: str) -> str:
    # KIKA requires this normalisation
    # normalised_column = FIELD_MAPPINGS.get(
    #     column[0], column[0].lower().replace(" ", "_")
    # )
    if dataset_name in FIELD_MAPPINGS:
        normalised_column = FIELD_MAPPINGS[dataset_name].get(column[0], column[0])
    else:
        normalised_column = column[0]
    api_field = ""
    if normalised_column in api_fields:
        api_field = normalised_column
    return api_field


if __name__ == "__main__":
    # DATASET_NAME = "insecurity-insight-crsv-incidents"
    DATASET_NAME = "insecurity-insight-education-overview"
    STATUS = generate_schema(DATASET_NAME)
    print(STATUS, flush=True)
