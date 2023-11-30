#!/usr/bin/env python
# encoding: utf-8

from hdx_scraper_insecurity_insight.create_spreadsheets import (
    generate_spreadsheet_filename,
    date_range_from_json,
    filter_json_rows,
    transform_input_rows,
)

from hdx_scraper_insecurity_insight.utilities import (
    fetch_json_from_samples,
    read_attributes,
    read_schema,
)

DATASET_NAME = "insecurity-insight-crsv-incidents"
SAMPLE_RESPONSE = fetch_json_from_samples(DATASET_NAME)


def test_date_range_from_json():
    start_year, end_year = date_range_from_json(SAMPLE_RESPONSE)

    assert start_year == "2020"
    assert end_year == "2023"


def test_generate_spreadsheet_filename_two_year():
    attributes = read_attributes(DATASET_NAME)
    filename = generate_spreadsheet_filename("", attributes, SAMPLE_RESPONSE)

    assert filename == "2020-2023-conflict-related-sexual-violence-incident-data.xlsx"


def test_generate_spreadsheet_filename_one_year():
    filtered_response = filter_json_rows("", "2021", SAMPLE_RESPONSE)
    attributes = read_attributes(DATASET_NAME)
    filename = generate_spreadsheet_filename("", attributes, filtered_response)

    assert filename == "2021-conflict-related-sexual-violence-incident-data.xlsx"


def test_generate_spreadsheet_filename_country():
    filtered_response = filter_json_rows("NGA", "2021", SAMPLE_RESPONSE)
    attributes = read_attributes(DATASET_NAME)
    filename = generate_spreadsheet_filename("NGA", attributes, filtered_response)

    assert filename == "2021-NGA-conflict-related-sexual-violence-incident-data.xlsx"


def test_transform_input_rows():
    filtered_rows = filter_json_rows("", "2021", SAMPLE_RESPONSE)
    _, row_template = read_schema(DATASET_NAME)
    output_rows = transform_input_rows(row_template, filtered_rows)

    assert len(output_rows) == len(filtered_rows)

    assert "Admin 1" in output_rows[0].keys()
    assert "Admin 1" in filtered_rows[0].keys()


def test_filter_json_rows():
    filtered_response = filter_json_rows("NGA", "2021", SAMPLE_RESPONSE)

    assert len(filtered_response) == 15
