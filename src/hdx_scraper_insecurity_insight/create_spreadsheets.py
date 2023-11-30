#!/usr/bin/env python
# encoding: utf-8

"""
This code generates an Excel file from the API response
"""

import datetime
import os

import pandas
from pandas.io.formats import excel

from hdx_scraper_insecurity_insight.utilities import (
    read_schema,
    read_attributes,
    fetch_json_from_samples,
)


def create_spreadsheet(
    dataset_name: str, country_filter: str = None, year_filter: str = None
) -> str:
    output_rows = []
    print("*********************************************", flush=True)
    print("* Insecurity Insight - Create spreadsheet   *", flush=True)
    print(f"* Invoked at: {datetime.datetime.now().isoformat(): <23}    *", flush=True)
    print("*********************************************", flush=True)
    print(f"Dataset name: {dataset_name}", flush=True)
    print(f"Country filter: {country_filter}", flush=True)
    print(f"Year filter: {year_filter}", flush=True)

    attributes = read_attributes(dataset_name)

    api_response = fetch_json_from_samples(dataset_name)

    # Fetch API to Spreadsheet lookup
    hdx_row, row_template = read_schema(dataset_name)

    output_rows.append(hdx_row)

    filtered_rows = filter_json_rows(country_filter, year_filter, api_response)

    output_rows.extend(transform_input_rows(row_template, filtered_rows))

    output_dataframe = pandas.DataFrame.from_dict(output_rows)

    print(output_dataframe, flush=True)

    # Generate filename
    filename = generate_spreadsheet_filename(country_filter, attributes, filtered_rows)

    # We can make the output an Excel table:
    # https://stackoverflow.com/questions/58326392/how-to-create-excel-table-with-pandas-to-excel
    excel.ExcelFormatter.header_style = None
    output_filepath = os.path.join(os.path.dirname(__file__), "output-spreadsheets", filename)
    output_dataframe.to_excel(
        output_filepath,
        index=False,
    )

    status = f"\nWrote spreadsheet with filepath {output_filepath}"
    return status


def transform_input_rows(row_template: dict, filtered_rows: list[dict]) -> list[dict]:
    transformed_rows = []
    for api_row in filtered_rows:
        transformed_row = row_template.copy()
        for key, value in row_template.items():
            transformed_row[key] = api_row.get(value, "")
        transformed_rows.append(transformed_row)

    return transformed_rows


def filter_json_rows(country_filter: str, year_filter: str, api_response: list[dict]) -> list[dict]:
    filtered_rows = []
    date_field = "Date"
    if date_field not in api_response[0].keys():
        date_field = "Year"
    for api_row in api_response:
        if (
            country_filter is not None
            and len(country_filter) != 0
            and api_row["Country ISO"] != country_filter
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


def generate_spreadsheet_filename(
    country_filter: str, attributes: dict, json_response: list[dict]
) -> str:
    start_year, end_year = date_range_from_json(json_response)
    country_iso = ""
    if (country_filter is not None) and (len(country_filter) != 0):
        country_iso = f"-{country_filter}"

    filename = attributes["filename_template"].format(
        start_year=start_year, end_year=end_year, country_iso=country_iso
    )

    if start_year == end_year:
        filename = filename.replace(f"-{end_year}", "")
    return filename


def date_range_from_json(json_response: list[dict]) -> (str, str):
    date_field = "Date"
    if date_field not in json_response[0]:
        date_field = "Year"

    # The tail command ensures the HDX tag line is skipped
    start_year = min([x[date_field] for x in json_response])[0:4]
    end_year = max([x[date_field] for x in json_response])[0:4]
    return start_year, end_year


if __name__ == "__main__":
    DATASET_NAME = "insecurity-insight-aidworkerKIKA-overview"
    STATUS = create_spreadsheet(DATASET_NAME)
    print(STATUS, flush=True)
