#!/usr/bin/env python
# encoding: utf-8

import os
from hdx_scraper_insecurity_insight.utilities import read_attributes
from hdx_scraper_insecurity_insight.create_datasets import (
    find_resource_filepath,
    get_date_and_country_ranges_from_resources,
)


def test_find_resource_filename():
    resource_name = "insecurity-insight-crsv"
    attributes = read_attributes(resource_name)
    filepath = find_resource_filepath(resource_name, attributes)
    filename = os.path.basename(filepath)

    assert filename == "2020-2023-conflict-related-sexual-violence-incident-data.xlsx"


def test_get_date_and_country_ranges_from_resources():
    resource_names = ["insecurity-insight-crsv", "insecurity-insight-crsv-overview"]
    dataset_date, countries_group = get_date_and_country_ranges_from_resources(
        resource_names, use_sample=True
    )

    assert dataset_date == "[2016 TO 2023-10-22]"
    assert len(countries_group) == 77
