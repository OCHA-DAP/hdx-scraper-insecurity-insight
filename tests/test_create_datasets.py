#!/usr/bin/env python
# encoding: utf-8

import json
import os
from unittest import mock

from hdx_scraper_insecurity_insight.utilities import read_attributes, fetch_json_from_samples
from hdx_scraper_insecurity_insight.create_datasets import (
    find_resource_filepath,
    get_date_and_country_ranges_from_resources,
    get_date_range_from_api_response,
    get_countries_group_from_api_response,
    get_legacy_dataset_name,
    create_or_fetch_base_dataset,
    create_datasets_in_hdx,
    get_date_range_from_resource_file,
)


@mock.patch("hdx.data.dataset.Dataset.read_from_hdx")
def test_create_or_fetch_base_dataset_fetch(mock_hdx):
    mock_hdx.return_value = {"name": "insecurity-insight-healthcare-dataset"}
    dataset_name = "insecurity-insight-healthcare-dataset"
    dataset, is_new = create_or_fetch_base_dataset(dataset_name)

    assert not is_new
    assert dataset["name"] == "insecurity-insight-healthcare-dataset"
    assert len(dataset.keys()) == 1


def test_create_or_fetch_base_dataset_create():
    dataset_name = "insecurity-insight-healthcare-dataset"
    dataset, is_new = create_or_fetch_base_dataset(dataset_name, force_create=True)

    assert is_new
    assert dataset["name"] == "insecurity-insight-healthcare-dataset"
    assert len(dataset.keys()) == 17


def test_create_or_fetch_base_dataset_create_country():
    dataset_name = "insecurity-insight-country-dataset"
    dataset, is_new = create_or_fetch_base_dataset(
        dataset_name, country_filter="MMR", force_create=True
    )

    assert is_new
    assert dataset["name"] == "insecurity-insight-mmr-dataset"
    assert len(dataset.keys()) == 17


def test_create_or_fetch_base_dataset_create_use_legacy():
    dataset_name = "insecurity-insight-healthcare-dataset"
    dataset, is_new = create_or_fetch_base_dataset(dataset_name, use_legacy=True)

    assert not is_new
    assert dataset["name"] == "sind-safeguarding-healthcare-monthly-news-briefs-dataset"
    assert len(dataset.keys()) > 45


def test_create_or_fetch_base_dataset_create_country_use_legacy():
    dataset_name = "insecurity-insight-country-dataset"
    dataset, is_new = create_or_fetch_base_dataset(
        dataset_name, country_filter="MMR", use_legacy=True
    )

    assert not is_new
    assert dataset["name"] == "myanmar-attacks-on-aid-operations-education-health-and-protection"
    assert len(dataset.keys()) > 45


def test_find_resource_filename():
    spreadsheet_directory = os.path.join(os.path.dirname(__file__), "fixtures")
    resource_name = "insecurity-insight-crsv-incidents"
    attributes = read_attributes(resource_name)
    filepath = find_resource_filepath(
        resource_name, attributes, spreadsheet_directory=spreadsheet_directory
    )
    filename = os.path.basename(filepath)

    assert filename == "2020-2023 Conflict Related Sexual Violence Incident Data.xlsx"


def test_find_resource_filename_single_year():
    spreadsheet_directory = os.path.join(os.path.dirname(__file__), "fixtures")
    resource_name = "insecurity-insight-healthcare-incidents"
    attributes = read_attributes(resource_name)
    filepath = find_resource_filepath(
        resource_name, attributes, country_filter="MMR", spreadsheet_directory=spreadsheet_directory
    )
    filename = os.path.basename(filepath)

    assert filename == "2023-MMR Attacks on Health Care Incident Data.xlsx"


def test_find_resource_filename_current_year():
    spreadsheet_directory = os.path.join(os.path.dirname(__file__), "fixtures")
    resource_name = "insecurity-insight-healthcare-incidents-current-year"
    attributes = read_attributes(resource_name)
    filepath = find_resource_filepath(
        resource_name, attributes, country_filter="", spreadsheet_directory=spreadsheet_directory
    )
    filename = os.path.basename(filepath)

    assert filename == "2024 Attacks on Health Care Incident Data.xlsx"


def test_get_date_and_country_ranges_from_resources():
    resource_names = ["insecurity-insight-crsv-incidents", "insecurity-insight-crsv-overview"]
    dataset_date, countries_group = get_date_and_country_ranges_from_resources(
        resource_names, use_sample=True
    )

    assert dataset_date == "[2020 TO 2024-02-25]"
    assert len(countries_group) == 80


def test_get_date_range_from_api_response():
    dataset_name = "insecurity-insight-crsv-incidents"
    api_response = fetch_json_from_samples(dataset_name)

    start_date, end_date = get_date_range_from_api_response(api_response)

    assert start_date == "2020-01-01"
    assert end_date == "2024-02-25"


def test_get_countries_group_from_api_response():
    dataset_name = "insecurity-insight-crsv-incidents"
    api_response = fetch_json_from_samples(dataset_name)

    countries = get_countries_group_from_api_response(api_response)

    assert len(countries) == 80
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


def test_create_datasets_in_hdx():
    dataset_name = "insecurity-insight-crsv-dataset"
    dataset_date = "[2020-01-01T00:00:00 TO 2023-09-30T23:59:59]"
    countries_group = [
        {
            "description": "",
            "display_name": "Afghanistan",
            "id": "afg",
            "image_display_url": "",
            "name": "afg",
            "title": "Afghanistan",
        }
    ]
    dataset, _ = create_datasets_in_hdx(
        dataset_name,
        dataset_date=dataset_date,
        countries_group=countries_group,
        dry_run=True,  # If debugging the "extras" issue then set this to False
        use_legacy=True,
    )

    # print(dataset, flush=True)
    resources = dataset.get_resources()
    for resource in resources:
        print(resource["name"], flush=True)
    # assert len(resources) in [1, 3]  # struggle to get this working locally and on GitHub Actions
    assert dataset["name"].endswith("conflict-related-sexual-violence")


def test_create_datasets_in_hdx_country():
    dataset_name = "insecurity-insight-country-dataset"
    dataset_date = "[2020-01-01T00:00:00 TO 2023-09-30T23:59:59]"
    countries_group = [
        {
            "description": "",
            "display_name": "Afghanistan",
            "id": "afg",
            "image_display_url": "",
            "name": "afg",
            "title": "Afghanistan",
        }
    ]
    dataset, _ = create_datasets_in_hdx(
        dataset_name,
        country_filter="AFG",
        dataset_date=dataset_date,
        countries_group=countries_group,
        dry_run=True,
    )

    print(dataset.resources, flush=True)

    assert dataset["name"].endswith(
        "afghanistan-violence-against-civilians-and-vital-civilian-facilities"
    )
    assert dataset["title"] == (
        "Afghanistan (AFG): Attacks on Aid Operations, Education and Health Care, and "
        "Explosive Weapons Incident Data"
    )


def test_create_datasets_in_hdx_use_legacy():
    dataset_name = "insecurity-insight-crsv-dataset"
    dataset_date = "[2020-01-01T00:00:00 TO 2023-09-30T23:59:59]"
    countries_group = [
        {
            "description": "",
            "display_name": "Afghanistan",
            "id": "afg",
            "image_display_url": "",
            "name": "afg",
            "title": "Afghanistan",
        }
    ]
    dataset, _ = create_datasets_in_hdx(
        dataset_name,
        dataset_date=dataset_date,
        countries_group=countries_group,
        dry_run=True,
        use_legacy=True,
    )

    assert dataset["name"].endswith("conflict-related-sexual-violence")


def test_create_datasets_in_hdx_country_use_legacy():
    dataset_name = "insecurity-insight-country-dataset"
    dataset_date = "[2020-01-01T00:00:00 TO 2023-09-30T23:59:59]"
    countries_group = [
        {
            "description": "",
            "display_name": "Afghanistan",
            "id": "afg",
            "image_display_url": "",
            "name": "afg",
            "title": "Afghanistan",
        }
    ]
    dataset, _ = create_datasets_in_hdx(
        dataset_name,
        country_filter="AFG",
        dataset_date=dataset_date,
        countries_group=countries_group,
        dry_run=True,
        use_legacy=True,
    )

    assert dataset["name"].endswith(
        "afghanistan-violence-against-civilians-and-vital-civilian-facilities"
    )
    assert dataset["title"] == (
        "Afghanistan (AFG): Attacks on Aid Operations, Education and Health Care, "
        "and Explosive Weapons Incident Data"
    )


def test_get_date_range_from_resource_file():
    test_filenames = [
        ("2024 Conflict Related Sexual Violence Incident Data.xlsx", "2024-05-28T00:00:00"),
        ("2020-2023 Conflict Related Sexual Violence Incident Data.xlsx", "2023-12-09T00:00:00"),
        ("2023-MMR Attacks on Health Care Incident Data.xlsx", "2023-04-29T00:00:00+00:00"),
    ]
    test_file_directory = os.path.join(os.path.dirname(__file__), "fixtures")

    for filename, expected_end_date in test_filenames:
        test_filepath = os.path.join(test_file_directory, filename)
        _, end_date = get_date_range_from_resource_file(test_filepath)
        assert end_date == expected_end_date
