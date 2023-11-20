#!/usr/bin/env python
# encoding: utf-8


from hdx_scraper_insecurity_insight.utilities import read_schema


def test_read_schema():
    dataset_name = "insecurity-insight-crsv"
    hdx_row, row_template = read_schema(dataset_name)

    assert hdx_row.keys() == row_template.keys()
    assert len(hdx_row.keys()) == 22
