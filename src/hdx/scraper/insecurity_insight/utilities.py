#!/usr/bin/python
"""insecurity insight utilities"""

import csv
import logging
import os
from os.path import dirname, join

from hdx.utilities.dictandlist import read_list_from_csv
from pandas import DataFrame
from pandas.io.formats import excel

logger = logging.getLogger(__name__)

ATTRIBUTES_FILEPATH = os.path.join(
    os.path.dirname(__file__), "metadata", "attributes.csv"
)
INSECURITY_INSIGHTS_FILEPATH_PAGES = os.path.join(
    os.path.dirname(__file__), "metadata", "New-HDX-APIs-1-HDX-Home-Page.csv"
)
INSECURITY_INSIGHTS_FILEPATH_TOPICS = os.path.join(
    os.path.dirname(__file__), "metadata", "New-HDX-APIs-2-Topics.csv"
)
INSECURITY_INSIGHTS_FILEPATH_COUNTRIES = os.path.join(
    os.path.dirname(__file__), "metadata", "New-HDX-APIs-3-Country.csv"
)


def filter_json_rows(
    country_filter: str, year_filter: str, api_response: list[dict]
) -> list[dict]:
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
        logging.info(
            f"API response contains latitude/longitude fields, censoring for {countries}"
        )
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
        logging.info(
            "API response contains Event Description, censoring for all countries"
        )

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
    with open(
        INSECURITY_INSIGHTS_FILEPATH_PAGES, "r", encoding="UTF-8"
    ) as attributes_filehandle:
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

    with open(
        INSECURITY_INSIGHTS_FILEPATH_TOPICS, "r", encoding="UTF-8"
    ) as attributes_filehandle:
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


def read_schema(dataset_name: str) -> list[dict]:
    dataset_name = dataset_name.replace("-current-year", "")
    row_template = []
    schema_filepath = join(dirname(__file__), "config", "schema.csv")
    schema_rows = read_list_from_csv(schema_filepath, headers=1, dict_form=True)
    for row in schema_rows:
        if row["dataset_name"] != dataset_name:
            continue
        row["field_number"] = int(row["field_number"])
        row_template.append(row)
    row_template = sorted(row_template, key=lambda x: x["field_number"])
    return row_template


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


def create_spreadsheet(
    topic: str,
    topic_type: str,
    proper_name: str,
    api_response: list[dict],
    output_dir: str,
    year_filter: str = "",
    country_filter: str = None,
) -> None | str:
    filtered_rows = filter_json_rows(country_filter, year_filter, api_response)
    if len(filtered_rows) == 0:
        logger.info(
            f"API reponse for `{topic}-{topic_type}` with country_filter {country_filter} contained no data"
        )
        return None

    # get columns in correct order and with correct type
    field_templates = read_schema(f"{topic}-{topic_type}")
    field_order = [field_template["field_name"] for field_template in field_templates]
    field_types = {
        field_template["field_name"]: field_template["field_type"]
        for field_template in field_templates
    }

    output_dataframe = DataFrame.from_dict(filtered_rows)
    output_dataframe = output_dataframe[field_order]
    output_dataframe.replace("", None, inplace=True)
    output_dataframe = output_dataframe.astype(field_types, errors="ignore")
    for key, value in field_types.items():
        if value == "datetime64[ns, UTC]":
            output_dataframe[key] = output_dataframe[key].dt.date

    # Generate filename
    date_field, _ = pick_date_and_iso_country_fields(api_response[0])
    start_year = min([x[date_field] for x in api_response])[0:4]
    end_year = max([x[date_field] for x in api_response])[0:4]
    country_iso = ""
    if (country_filter is not None) and (len(country_filter) != 0):
        country_iso = f"-{country_filter}"

    if topic_type == "incidents":
        filename = (
            f"{start_year}-{end_year}{country_iso} {proper_name} Incident Data.xlsx"
        )
    elif topic_type == "incidents-current-year":
        filename = f"{start_year} {proper_name} Incident Data.xlsx"
    elif topic_type == "overview":
        filename = (
            f"{start_year}-{end_year}{country_iso} {proper_name} Overview Data.xlsx"
        )

    if start_year == end_year:
        filename = filename.replace(f"-{end_year}", "")

    # We can make the output an Excel table:
    # https://stackoverflow.com/questions/58326392/how-to-create-excel-table-with-pandas-to-excel
    excel.ExcelFormatter.header_style = None

    output_filepath = os.path.join(output_dir, filename)
    output_dataframe.to_excel(
        output_filepath,
        index=False,
    )

    return output_filepath
