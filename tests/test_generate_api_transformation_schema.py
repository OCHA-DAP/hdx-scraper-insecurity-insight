#!/usr/bin/env python
# encoding: utf-8

from hdx_scraper_insecurity_insight.generate_api_transformation_schema import generate_schema


def test_generate_schema():
    dataset_name = "insecurity-insight-crsv-incidents"
    summary = generate_schema(dataset_name)

    assert summary == {
        "dataset_name": "insecurity-insight-crsv-incidents",
        "n_api_fields": 22,
        "n_spreadsheet_fields": 22,
        "n_hxl_tags": 18,
    }
