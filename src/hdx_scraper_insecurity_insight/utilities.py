#!/usr/bin/env python
# encoding: utf-8

"""
Miscellaneous utilities, some borrowed from elsewhere
Ian Hopkinson 2023-11.20
"""

import csv
import datetime
import json
import logging
import os
import sys
import time

from typing import Any

from urllib3 import request
from urllib3.util import Retry

SCHEMA_FILEPATH = os.path.join(os.path.dirname(__file__), "metadata", "schema.csv")
ATTRIBUTES_FILEPATH = os.path.join(os.path.dirname(__file__), "metadata", "attributes.csv")
INSECURITY_INSIGHTS_FILEPATH_PAGES = os.path.join(
    os.path.dirname(__file__), "metadata", "New-HDX-APIs-1-HDX-Home-Page.csv"
)
INSECURITY_INSIGHTS_FILEPATH_TOPICS = os.path.join(
    os.path.dirname(__file__), "metadata", "New-HDX-APIs-2-Topics.csv"
)
INSECURITY_INSIGHTS_FILEPATH_COUNTRIES = os.path.join(
    os.path.dirname(__file__), "metadata", "New-HDX-APIs-3-Country.csv"
)


def fetch_json(dataset_name: str, use_sample: bool = False):
    if use_sample:
        json_response = fetch_json_from_samples(dataset_name)
    else:
        json_response = fetch_json_from_api(dataset_name)

    if json_response is None:
        return None

    censored_location_response = censor_location("PSE", json_response)
    censored_response = censor_event_description(censored_location_response)

    return censored_response


def fetch_json_from_api(dataset_name: str) -> list[dict] | None:
    attributes = read_attributes(dataset_name)

    response = request(
        "GET", attributes["api_url"], timeout=60, retries=Retry(90, backoff_factor=1.0)
    )

    if response.status == 404:
        logging.info(f"Endpoint returned a 404 status for {dataset_name}")
        return None

    if response.status == 503:
        logging.info(
            f"Endpoint returned a 503 status for {dataset_name}, waiting 300 seconds to retry"
        )
        time.sleep(300)
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


def filter_json_rows(country_filter: str, year_filter: str, api_response: list[dict]) -> list[dict]:
    filtered_rows = []

    date_field, iso_country_field = pick_date_and_iso_country_fields(api_response[0])

    for api_row in api_response:
        if (
            country_filter is not None
            and len(country_filter) != 0
            and api_row[iso_country_field] != country_filter
        ):
            continue
        if (
            year_filter is not None
            and len(year_filter) != 0
            and api_row[date_field][0:4] != year_filter
        ):
            continue
        filtered_rows.append(api_row)

    return filtered_rows


def censor_location(countries: list[str], api_response: list[dict]) -> list[dict]:
    censored_rows = []

    if "Latitude" not in api_response[0].keys():
        logging.info("API response does not contain latitude/longitude fields")
        return api_response
    else:
        logging.info(f"API response contains latitude/longitude fields, censoring for {countries}")
    _, iso_country_field = pick_date_and_iso_country_fields(api_response[0])

    # Geo fields are Latitude, Longitude and Geo Precision
    n_censored = 0
    n_records = 0
    for api_row in api_response:
        n_records += 1
        if api_row[iso_country_field] in countries:
            n_censored += 1
            api_row["Latitude"] = None
            api_row["Longitude"] = None
            api_row["Geo Precision"] = "censored"
        censored_rows.append(api_row)

    logging.info(f"{n_censored} of {n_records} censored for {countries}")
    return censored_rows


def censor_event_description(api_response: list[dict]) -> list[dict]:
    censored_rows = []

    if "Event Description" not in api_response[0].keys():
        logging.info("API response does not contain Event Description fields")
        return api_response
    else:
        logging.info("API response contains Event Description, censoring for all countries")

    # Geo fields are Latitude, Longitude and Geo Precision
    n_censored = 0
    n_records = 0
    for api_row in api_response:
        n_records += 1
        n_censored += 1
        api_row["Event Description"] = ""
        # api_row.pop("Event Description", None)
        censored_rows.append(api_row)

    logging.info(f"{n_censored} of {n_records} Event Description blanked")
    return censored_rows


def read_attributes(dataset_name: str) -> dict:
    with open(ATTRIBUTES_FILEPATH, "r", encoding="UTF-8") as attributes_filehandle:
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


def read_insecurity_insight_attributes_pages(dataset_name: str) -> dict:
    ii_attributes = {}
    with open(INSECURITY_INSIGHTS_FILEPATH_PAGES, "r", encoding="UTF-8") as attributes_filehandle:
        attribute_rows = csv.DictReader(attributes_filehandle)
        for row in attribute_rows:
            if row["ih_name"] != dataset_name:
                continue
            ii_attributes = row
            break

    if ii_attributes:
        legacy_name = ii_attributes["HDX link"].split("/")[-1]
        ii_attributes["legacy_name"] = legacy_name
    return ii_attributes


def read_insecurity_insight_resource_attributes(dataset_name: str) -> list[dict]:
    resource_attributes = []

    with open(INSECURITY_INSIGHTS_FILEPATH_TOPICS, "r", encoding="UTF-8") as attributes_filehandle:
        attribute_rows = csv.DictReader(attributes_filehandle)
        for row in attribute_rows:
            if row["parent_name"] != dataset_name:
                continue
            resource_attributes.append(row)

    if len(resource_attributes) == 0:
        with open(
            INSECURITY_INSIGHTS_FILEPATH_COUNTRIES, "r", encoding="UTF-8"
        ) as attributes_filehandle:
            attribute_rows = csv.DictReader(attributes_filehandle)
            for row in attribute_rows:
                if row["parent_name"] != dataset_name:
                    continue
                resource_attributes.append(row)

    return resource_attributes


def list_entities(type_: str = "dataset") -> list[str]:
    entity_list = []
    with open(ATTRIBUTES_FILEPATH, "r", encoding="UTF-8") as attributes_filehandle:
        attribute_rows = csv.DictReader(attributes_filehandle)
        for row in attribute_rows:
            if row["attribute"] == "entity_type" and row["value"] == type_:
                entity_list.append(row["dataset_name"])

    return entity_list


def read_schema(dataset_name: str) -> tuple[dict, dict]:
    hdx_row = {}
    row_template = {}
    if "overview" not in dataset_name:
        schema_filepath = SCHEMA_FILEPATH
    else:
        schema_filepath = SCHEMA_FILEPATH.replace("schema.csv", "schema-overview.csv")
    if os.path.exists(schema_filepath):
        with open(schema_filepath, "r", encoding="UTF-8") as schema_filehandle:
            schema_rows = csv.DictReader(schema_filehandle)

            for row in schema_rows:
                if row["dataset_name"] != dataset_name:
                    continue
                hdx_row[row["field_name"]] = row["terms"]
                # This is where we would switch to using the API as the template
                row_template[row["field_name"]] = row["upstream"]

    return hdx_row, row_template


def write_schema(dataset_name: str, output_rows: list[dict]) -> str:
    hdx_row, _ = read_schema(dataset_name)
    if not hdx_row:
        status = write_dictionary(SCHEMA_FILEPATH, output_rows, append=True)
    else:
        status = f"Schema for {dataset_name} already in {SCHEMA_FILEPATH}, no update made"
    return status


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


def parse_commandline_arguments() -> tuple[str, str]:
    dataset_name = "insecurity-insight-aidworkerKIKA-overview"
    country_code = ""
    if len(sys.argv) == 2:
        dataset_name = sys.argv[1]
        country_code = ""
    elif len(sys.argv) == 3:
        dataset_name = sys.argv[1]
        country_code = sys.argv[2]

    return dataset_name, country_code


def pick_date_and_iso_country_fields(row_dictionary: dict) -> tuple[str, str]:
    iso_country_field = "Country ISO"
    if iso_country_field not in row_dictionary.keys():
        iso_country_field = "country_iso"

    date_field = "Date"
    for date_field_option in ["Date", "Year", "date"]:
        if date_field_option in row_dictionary:
            date_field = date_field_option
            break

    return date_field, iso_country_field


def print_banner_to_log(logger: logging.Logger, name: str):
    title = f"Insecurity Insight - {name}"
    timestamp = f"Invoked at: {datetime.datetime.now().isoformat()}"
    width = max(len(title), len(timestamp))
    logger.info((width + 4) * "*")
    logger.info(f"* {title:<{width}} *")
    logger.info(f"* {timestamp:<{width}} *")
    logger.info((width + 4) * "*")


def read_field_mappings() -> dict:
    field_mappings = {}
    with open(
        os.path.join(os.path.dirname(__file__), "metadata", "field_mappings.csv"),
        "r",
        encoding="UTF-8",
    ) as field_mappings_filehandle:
        field_mapping_rows = csv.DictReader(field_mappings_filehandle)
        for row in field_mapping_rows:
            if row["dataset_name"] not in field_mappings:
                field_mappings[row["dataset_name"]] = {}
            field_mappings[row["dataset_name"]][row["field_name"]] = row["upstream"]

    return field_mappings


def read_countries() -> dict:
    countries = {}
    with open(
        os.path.join(os.path.dirname(__file__), "metadata", "countries.csv"),
        "r",
        encoding="UTF-8",
    ) as countries_filehandle:
        countries_rows = csv.DictReader(countries_filehandle)
        for row in countries_rows:
            countries[row["country_iso3"]] = row["legacy_dataset_name"]

    return countries
