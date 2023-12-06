#!/usr/bin/env python
# encoding: utf-8

"""
This code generates an Excel file from the API response
"""

import datetime
import logging
import os
import time

import pandas
from pandas.io.formats import excel

from hdx.utilities.easy_logging import setup_logging

from hdx_scraper_insecurity_insight.utilities import (
    read_schema,
    read_attributes,
    fetch_json_from_samples,
    filter_json_rows,
    list_entities,
    parse_commandline_arguments,
    pick_date_and_iso_country_fields,
)

setup_logging()
LOGGER = logging.getLogger(__name__)

OUTPUT_DIRECTORY = os.path.join(os.path.dirname(__file__), "output-spreadsheets")


def marshall_spreadsheets(dataset_name_pattern: str, country_pattern: str) -> list[str]:
    LOGGER.info("*********************************************")
    LOGGER.info("* Insecurity Insight - Create spreadsheet   *")
    LOGGER.info(f"* Invoked at: {datetime.datetime.now().isoformat(): <23}    *")
    LOGGER.info("*********************************************")
    LOGGER.info(f"Dataset name: {dataset_name_pattern}")
    LOGGER.info(f"Country filter: {country_pattern}")
    LOGGER.info(f"Output directory: {OUTPUT_DIRECTORY}")
    status_list = []

    t0 = time.time()
    if dataset_name_pattern.lower() != "all":
        status = create_spreadsheet(dataset_name_pattern)
        status_list.append(status)
    else:
        dataset_names = list_entities(type_="resource")

        LOGGER.info(f"Attributes file contains {len(dataset_names)} resource names")

        for dataset_name in dataset_names:
            status = create_spreadsheet(dataset_name, country_pattern)
            status_list.append(status)

    LOGGER.info("\n")
    LOGGER.info("Processing complete")
    LOGGER.info(f"Processing took {time.time() - t0:0.2f} seconds")
    return status_list


def create_spreadsheet(
    dataset_name: str,
    country_filter: str = None,
    year_filter: str = None,
    api_response: list[dict] = None,
) -> str:
    LOGGER.info("\n")
    LOGGER.info(f"Processing {dataset_name}")
    output_rows = []

    attributes = read_attributes(dataset_name)

    if api_response is None:
        LOGGER.info("Using api_response sample, not live API")
        api_response = fetch_json_from_samples(dataset_name)
    else:
        LOGGER.info("Using live API")

    # Fetch API to Spreadsheet lookup
    hdx_row, row_template = read_schema(dataset_name)

    output_rows.append(hdx_row)

    filtered_rows = filter_json_rows(country_filter, year_filter, api_response)

    if len(filtered_rows) == 0:
        status = (
            f"API reponse for `{dataset_name}` with country_filter {country_filter} "
            "contained no data"
        )
        LOGGER.info(status)
        return status

    output_rows.extend(transform_input_rows(row_template, filtered_rows))

    output_dataframe = pandas.DataFrame.from_dict(output_rows)

    # print(output_dataframe, flush=True)

    # Generate filename
    filename = generate_spreadsheet_filename(country_filter, attributes, filtered_rows)

    # We can make the output an Excel table:
    # https://stackoverflow.com/questions/58326392/how-to-create-excel-table-with-pandas-to-excel
    excel.ExcelFormatter.header_style = None

    output_filepath = os.path.join(OUTPUT_DIRECTORY, filename)
    output_dataframe.to_excel(
        output_filepath,
        index=False,
    )

    status = f"Output filename `{filename}`"
    LOGGER.info(status)
    return status


def transform_input_rows(row_template: dict, filtered_rows: list[dict]) -> list[dict]:
    transformed_rows = []
    for api_row in filtered_rows:
        transformed_row = row_template.copy()
        for key, value in row_template.items():
            transformed_row[key] = api_row.get(value, "")
        transformed_rows.append(transformed_row)

    return transformed_rows


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
    date_field, _ = pick_date_and_iso_country_fields(json_response[0])

    # The tail command ensures the HDX tag line is skipped
    start_year = min([x[date_field] for x in json_response])[0:4]
    end_year = max([x[date_field] for x in json_response])[0:4]
    return start_year, end_year


if __name__ == "__main__":
    DATASET_NAME, COUNTRY_CODE = parse_commandline_arguments()
    STATUS_LIST = marshall_spreadsheets(DATASET_NAME, COUNTRY_CODE)
