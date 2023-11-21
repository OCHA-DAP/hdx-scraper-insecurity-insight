#!/usr/bin/env python
# encoding: utf-8


from hdx_scraper_insecurity_insight.utilities import read_schema, read_attributes


def test_read_schema():
    dataset_name = "insecurity-insight-crsv"
    hdx_row, row_template = read_schema(dataset_name)

    assert hdx_row.keys() == row_template.keys()
    assert len(hdx_row.keys()) == 22


def test_read_attributes():
    dataset_name = "insecurity-insight-crsv"
    attributes = read_attributes(dataset_name)

    assert list(attributes.keys()) == [
        "resource_filename",
        "api_url",
        "api_response_filename",
        "filename_template",
    ]
