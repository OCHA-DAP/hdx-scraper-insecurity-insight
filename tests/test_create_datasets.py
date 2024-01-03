#!/usr/bin/env python
# encoding: utf-8

import os
from hdx_scraper_insecurity_insight.utilities import read_attributes, fetch_json_from_samples
from hdx_scraper_insecurity_insight.create_datasets import (
    find_resource_filepath,
    get_date_and_country_ranges_from_resources,
    get_date_range_from_api_response,
    get_countries_group_from_api_response,
    get_legacy_dataset_name,
)


def test_find_resource_filename():
    resource_name = "insecurity-insight-crsv-incidents"
    attributes = read_attributes(resource_name)
    filepath = find_resource_filepath(resource_name, attributes)
    filename = os.path.basename(filepath)

    assert filename == "2020-2023-conflict-related-sexual-violence-incident-data.xlsx"


def test_get_date_and_country_ranges_from_resources():
    resource_names = ["insecurity-insight-crsv-incidents", "insecurity-insight-crsv-overview"]
    dataset_date, countries_group = get_date_and_country_ranges_from_resources(
        resource_names, use_sample=True
    )

    assert dataset_date == "[2020 TO 2023-12-09]"
    assert len(countries_group) == 76


def test_get_date_range_from_api_response():
    dataset_name = "insecurity-insight-crsv-incidents"
    api_response = fetch_json_from_samples(dataset_name)

    start_date, end_date = get_date_range_from_api_response(api_response)

    assert start_date == "2020-01-01"
    assert end_date == "2023-12-09"


def test_get_countries_group_from_api_response():
    dataset_name = "insecurity-insight-crsv-incidents"
    api_response = fetch_json_from_samples(dataset_name)

    countries = get_countries_group_from_api_response(api_response)

    assert len(countries) == 76
    for country in countries:
        assert list(country.keys()) == ["name"]
        assert country["name"] == country["name"].lower()
        assert len(country["name"]) == 3


def test_get_legacy_dataset_name_non_country():
    dataset_name = "insecurity-insight-crsv-dataset"
    legacy_dataset_name = get_legacy_dataset_name(dataset_name)

    assert legacy_dataset_name == "conflict-related-sexual-violence"


def test_get_legacy_dataset_name_country():
    dataset_name = "insecurity-insight-country-dataset"
    country_filter = "COD"
    legacy_dataset_name = get_legacy_dataset_name(dataset_name, country_filter=country_filter)

    assert legacy_dataset_name == "attacks-on-ebola-response"


def test_get_legacy_dataset_name_country_returns_none():
    dataset_name = "obviously-fake-name"
    legacy_dataset_name = get_legacy_dataset_name(dataset_name)

    assert legacy_dataset_name is None
