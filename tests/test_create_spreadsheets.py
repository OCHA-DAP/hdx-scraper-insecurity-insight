#!/usr/bin/env python
# encoding: utf-8

import os
import pandas

from hdx_scraper_insecurity_insight.create_spreadsheets import (
    generate_spreadsheet_filename,
    date_range_from_json,
    transform_input_rows,
    create_spreadsheet,
    make_type_dict,
)

from hdx_scraper_insecurity_insight.utilities import (
    fetch_json_from_samples,
    read_attributes,
    read_schema,
    filter_json_rows,
)

DATASET_NAME = "insecurity-insight-crsv-incidents"
SAMPLE_RESPONSE = fetch_json_from_samples(DATASET_NAME)


def test_create_spreadsheet():
    expected_filename = "2020-2025 Conflict Related Sexual Violence Incident Data.xlsx"
    temp_directory = os.path.join(os.path.dirname(__file__), "temp")

    expected_file_path = os.path.join(temp_directory, expected_filename)
    if os.path.exists(expected_file_path):
        os.remove(expected_file_path)
    status = create_spreadsheet(
        DATASET_NAME, output_directory=temp_directory, api_response=SAMPLE_RESPONSE
    )

    assert expected_filename in status
    assert os.path.exists(expected_file_path)

    sheets_df = pandas.read_excel(expected_file_path)

    assert len(sheets_df) > 1
    # if os.path.exists(expected_file_path):
    #     os.remove(expected_file_path)


def test_create_spreadsheet_healthcare():
    healthcare_dataset = "insecurity-insight-healthcare-incidents"
    expected_filename = "2020-2025 Attacks on Health Care Incident Data.xlsx"
    temp_directory = os.path.join(os.path.dirname(__file__), "temp")

    expected_file_path = os.path.join(temp_directory, expected_filename)
    if os.path.exists(expected_file_path):
        os.remove(expected_file_path)
    status = create_spreadsheet(
        healthcare_dataset, output_directory=temp_directory, api_response=SAMPLE_RESPONSE
    )

    assert expected_filename in status
    assert os.path.exists(expected_file_path)

    sheets_df = pandas.read_excel(expected_file_path)

    assert len(sheets_df) > 1
    # if os.path.exists(expected_file_path):
    #     os.remove(expected_file_path)


def test_create_current_year_spreadsheet():
    dataset_name = "insecurity-insight-crsv-incidents-current-year"
    expected_filename = "2025 Conflict Related Sexual Violence Incident Data.xlsx"
    temp_directory = os.path.join(os.path.dirname(__file__), "temp")

    expected_file_path = os.path.join(temp_directory, expected_filename)
    if os.path.exists(expected_file_path):
        os.remove(expected_file_path)
    status = create_spreadsheet(
        dataset_name, output_directory=temp_directory, api_response=SAMPLE_RESPONSE
    )

    sheets_df = pandas.read_excel(expected_file_path)

    assert len(sheets_df) == 3

    assert expected_filename in status
    assert os.path.exists(expected_file_path)
    if os.path.exists(expected_file_path):
        os.remove(expected_file_path)


def test_date_range_from_json():
    start_year, end_year = date_range_from_json(SAMPLE_RESPONSE)

    assert start_year == "2020"
    assert end_year == "2025"


def test_generate_spreadsheet_filename_two_year():
    attributes = read_attributes(DATASET_NAME)
    filename = generate_spreadsheet_filename("", attributes, SAMPLE_RESPONSE)

    assert filename == "2020-2025 Conflict Related Sexual Violence Incident Data.xlsx"


def test_generate_spreadsheet_filename_one_year():
    filtered_response = filter_json_rows("", "2021", SAMPLE_RESPONSE)
    attributes = read_attributes(DATASET_NAME)
    filename = generate_spreadsheet_filename("", attributes, filtered_response)

    assert filename == "2021 Conflict Related Sexual Violence Incident Data.xlsx"


def test_generate_spreadsheet_filename_country():
    filtered_response = filter_json_rows("NGA", "2021", SAMPLE_RESPONSE)
    attributes = read_attributes(DATASET_NAME)
    filename = generate_spreadsheet_filename("NGA", attributes, filtered_response)

    assert filename == "2021-NGA Conflict Related Sexual Violence Incident Data.xlsx"


def test_transform_input_rows():
    filtered_rows = filter_json_rows("", "2021", SAMPLE_RESPONSE)
    _, row_template = read_schema(DATASET_NAME)
    output_rows = transform_input_rows(row_template, filtered_rows)

    assert len(output_rows) == len(filtered_rows)

    assert "Admin 1" in output_rows[0].keys()
    assert "Admin 1" in filtered_rows[0].keys()


def test_make_type_dict():
    hdx_row, row_template = read_schema(DATASET_NAME)
    type_dict = make_type_dict(hdx_row)
    assert type_dict["Date"] == "datetime64[ns, UTC]"
    assert type_dict["Longitude"] == "float64"
    assert type_dict["Number of Reported Victims"] == "Int64"
