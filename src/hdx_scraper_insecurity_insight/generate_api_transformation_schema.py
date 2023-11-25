#!/usr/bin/env python
# encoding: utf-8

"""
This code is for establishing field mappings and HXL codes for the existing data to the new API
Ian Hopkinson 2023-11-18
"""


import datetime
import os
import pandas as pd

from hdx_scraper_insecurity_insight.utilities import (
    write_dictionary,
    read_schema,
    read_attributes,
    fetch_json_from_samples,
)

FIELD_MAPPINGS = {
    "Aid Workers Killed": "aidworkers_killed",
    "Aid Workers Injured": "aidworkers_injured",
    "Aid Workers Kidnapped": "aidworkers_kidnapped",
    "Aid Workers Arrested": "aidworkers_arrested",
    "SiND Event ID": "event_id",
}

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


# Datamesh style schema file
# dataset_name,timestamp,upstream,field_name,field_number,field_type,terms,tags,description
def generate_schema(dataset_name: str) -> str:
    print("*********************************************", flush=True)
    print("* Insecurity Insight - Generate schema.csv  *", flush=True)
    print(f"* Invoked at: {datetime.datetime.now().isoformat(): <23} *", flush=True)
    print("*********************************************", flush=True)
    attributes = read_attributes(dataset_name)

    # Get relevant cached API response
    api_response = fetch_json_from_samples(dataset_name)
    api_fields = list(api_response[0].keys())

    if len(attributes["legacy_resource_filename"]) != 0:
        resource_df = pd.read_excel(
            os.path.join(
                os.path.dirname(__file__), "spreadsheet-samples", attributes["resource_filename"]
            )
        )
        print(resource_df, flush=True)
        # Get column headers
        column_names = resource_df.columns.tolist()
        # Get HXL tags
        hxl_tags = resource_df.loc[0, :].values.flatten().tolist()
        hxl_tags = ["" if isinstance(x, float) else x for x in hxl_tags]
    else:
        print(f"No example spreadsheet provided for {dataset_name}", flush=True)
        column_names = api_fields
        hxl_tags = [""] * len(api_fields)

    # Collect the set of country ISO codes
    country_codes = {x["Country ISO"] for x in api_response}
    print("\nCountries in API data but not currently on HDX", flush=True)
    print(country_codes.difference(set(EXPECTED_COUNTRY_LIST)))

    print("\nCountries on HDX but not in API data", flush=True)
    print(set(EXPECTED_COUNTRY_LIST).difference(country_codes))

    # print(country_codes, flush=True)

    # Display Original fields, HXL and matching API field
    columns = zip(column_names, hxl_tags)

    # dataset_name,timestamp,upstream,field_name,field_number,field_type,terms,tags,description

    output_rows = []
    schema_output_filepath = os.path.join(os.path.dirname(__file__), "metadata", "schema.csv")

    timestamp = datetime.datetime.now().isoformat()

    for i, column in enumerate(columns):
        # KIKA requires this normalisation
        # normalised_column = FIELD_MAPPINGS.get(
        #     column[0], column[0].lower().replace(" ", "_")
        # )
        in_api, api_field = is_column_in_api_field(api_fields, column)

        print(f"{i:<2}. {column[0]:<50},{column[1]:<30},{in_api}", flush=True)
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

    hdx_row, _ = read_schema(dataset_name)
    if not hdx_row:
        status = write_dictionary(schema_output_filepath, output_rows, append=True)
    else:
        status = f"Schema for {dataset_name} already in {schema_output_filepath}, no update made"
    return status


def is_column_in_api_field(api_fields, column):
    normalised_column = column[0]
    in_api = ""
    api_field = ""
    if normalised_column in api_fields:
        in_api = "in_api"
        api_field = normalised_column
    return in_api, api_field


if __name__ == "__main__":
    DATASET_NAME = "insecurity-insight-crsv-overview"
    STATUS = generate_schema(DATASET_NAME)
    print(STATUS, flush=True)
