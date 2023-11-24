#!/usr/bin/env python
# encoding: utf-8


from hdx_scraper_insecurity_insight.utilities import (
    read_schema,
    read_attributes,
    fetch_json_from_samples,
    fetch_json_from_api,
)


def test_read_schema():
    dataset_name = "insecurity-insight-crsv"
    hdx_row, row_template = read_schema(dataset_name)

    assert hdx_row.keys() == row_template.keys()
    assert len(hdx_row.keys()) == 22


def test_read_attributes():
    dataset_name = "insecurity-insight-crsv"
    attributes = read_attributes(dataset_name)

    assert list(attributes.keys()) == [
        "legacy_resource_filename",
        "description",
        "api_url",
        "api_response_filename",
        "filename_template",
        "file_format",
    ]


def test_fetching_json():
    dataset_name = "insecurity-insight-crsv"

    samples_response = fetch_json_from_samples(dataset_name)
    api_response = fetch_json_from_api(dataset_name)

    assert samples_response[0].keys() == api_response[0].keys()
