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
    output_directory: str = None,
) -> str:
    LOGGER.info(f"Processing {dataset_name}")
    if output_directory is None:
        output_directory = OUTPUT_DIRECTORY
    output_rows = []

    attributes = read_attributes(dataset_name)

    if not year_filter:
        year_filter = attributes.get("year_filter", "")
        if year_filter == "current year":
            year_filter = datetime.datetime.now().isoformat()[0:4]
        # print(year_filter, flush=True)

    if api_response is None:
        # LOGGER.info("Using api_response sample, not live API")
        api_response = fetch_json_from_samples(dataset_name)
    else:
        pass
        # LOGGER.info("Using supplied API response")

    # Fetch API to Spreadsheet lookup
    if dataset_name.endswith("current-year"):
        modified_dataset_name = dataset_name.replace("-current-year", "")
    else:
        modified_dataset_name = dataset_name
    hdx_row, row_template = read_schema(modified_dataset_name)

    # output_rows.append(hdx_row)

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

    # Type conversions
    type_dict = make_type_dict(hdx_row)
    output_dataframe = output_dataframe.astype(type_dict, errors="ignore")
    for key, value in type_dict.items():
        if value == "datetime64[ns, UTC]":
            output_dataframe[key] = output_dataframe[key].dt.date

    # Add hdx_row
    # hdx_row_df = pandas.DataFrame(hdx_row, index=[0])
    # output_dataframe = pandas.concat([hdx_row_df, output_dataframe])
    # print(output_dataframe.dtypes, flush=True)

    # print(output_dataframe, flush=True)

    # Generate filename
    filename = generate_spreadsheet_filename(country_filter, attributes, filtered_rows)

    # We can make the output an Excel table:
    # https://stackoverflow.com/questions/58326392/how-to-create-excel-table-with-pandas-to-excel
    excel.ExcelFormatter.header_style = None

    output_filepath = os.path.join(output_directory, filename)
    output_dataframe.to_excel(
        output_filepath,
        index=False,
    )

    status = f"Output filename `{filename}`"
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


def date_range_from_json(json_response: list[dict]) -> tuple[str, str]:
    date_field, _ = pick_date_and_iso_country_fields(json_response[0])

    # The tail command ensures the HDX tag line is skipped
    start_year = min([x[date_field] for x in json_response])[0:4]
    end_year = max([x[date_field] for x in json_response])[0:4]
    return start_year, end_year


def make_type_dict(row_template: dict) -> dict:
    type_dict = {}

    for key, value in row_template.items():
        if "#date" in value:
            type_dict[key] = "datetime64[ns, UTC]"
        elif "#geo" in value and "+precision" not in value:
            type_dict[key] = "float64"
        elif "#affected" in value or "+num" in value:
            type_dict[key] = "Int64"
        else:
            type_dict[key] = str

        # Known Kidnapping or Arrest Outcome has a HXL tag "#affected" but is a string field
        if "outcome" in key.lower() or key == "Affected":
            type_dict[key] = str

    return type_dict


if __name__ == "__main__":
    DATASET_NAME, COUNTRY_CODE = parse_commandline_arguments()
    STATUS_LIST = marshall_spreadsheets(DATASET_NAME, COUNTRY_CODE)
