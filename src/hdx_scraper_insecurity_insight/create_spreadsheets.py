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

    for api_row in api_response:
        if country_filter is not None and api_row["Country ISO"] != country_filter:
            continue
        if year_filter is not None and api_row["Date"][0:4] != year_filter:
            continue
        output_row = row_template.copy()
        for key, value in row_template.items():
            output_row[key] = api_row[value]
        output_rows.append(output_row)

    output_dataframe = pandas.DataFrame.from_dict(output_rows)

    print(output_dataframe, flush=True)

    # Generate filename
    filename = generate_spreadsheet_filename(
        country_filter, attributes, row_template, output_dataframe
    )

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


def generate_spreadsheet_filename(country_filter, attributes, row_template, output_dataframe):
    country_iso = ""
    if country_filter is not None:
        country_iso = f"-{country_filter}"

    date_field = "Date"
    if date_field not in row_template:
        date_field = "Year"

    # The tail command ensures the HDX tag line is skipped
    start_year = output_dataframe.tail(-1)[date_field].min()[0:4]
    end_year = output_dataframe[date_field].max()[0:4]

    filename = attributes["filename_template"].format(
        start_year=start_year, end_year=end_year, country_iso=country_iso
    )

    if start_year == end_year:
        filename = filename.replace(f"-{end_year}", "")
    return filename


if __name__ == "__main__":
    # DATASET_NAME = "insecurity-insight-crsv-overview"
    DATASET_NAME = "insecurity-insight-education-incidents"
    STATUS = create_spreadsheet(DATASET_NAME)
    print(STATUS, flush=True)
