#!/usr/bin/env python
# encoding: utf-8
import pytest

from hdx_scraper_insecurity_insight.run import (
    parse_dates_from_string,
    fetch_and_cache_datasets,
    fetch_and_cache_api_responses,
    decide_which_resources_have_fresh_data,
    # refresh_spreadsheets_with_fresh_data,
    # update_datasets_whose_resources_have_changed,
)


@pytest.mark.skip(reason="Testing API is really slow, and not a unit test")
def test_fetch_and_cache_api_response():
    api_cache = fetch_and_cache_api_responses()

    assert len(api_cache) == 11


def test_fetch_and_cache_datasets():
    dataset_cache = fetch_and_cache_datasets()

    assert len(dataset_cache) == 32


def test_check_api_has_not_changed():
    # compare_api_to_samples is tested, effectively in
    # test_generate_api_transformation_schema.test_compare_api_to_samples_changed
    assert True


def test_decide_which_resources_have_fresh_data():
    dataset_cache = {
        "insecurity-insight-healthcare-dataset": {
            "dataset_date": "[2020-01-01T00:00:00 TO 2023-10-17T23:59:59]"
        }
    }
    api_cache = {
        "insecurity-insight-healthcare-incidents": [
            {
                "Date": "2024-03-06T00:00:00.000Z",
                "Country ISO": "UKR",
            },
            {
                "Date": "2024-03-03T00:00:00.000Z",
                "Country ISO": "UKR",
            },
        ]
    }
    items_to_update = decide_which_resources_have_fresh_data(
        dataset_cache,
        api_cache,
        refresh=[],
        dataset_list=["insecurity-insight-healthcare-dataset"],
        resource_list=["insecurity-insight-healthcare-incidents"],
        topic_list=["healthcare"],
    )

    assert items_to_update == [("healthcare", "2024-03-03", "2024-03-06")]


def test_decide_which_resources_have_fresh_data_backfill():
    dataset_cache = {
        "insecurity-insight-healthcare-dataset": {
            "dataset_date": "[2020-01-01T00:00:00 TO 2023-10-17T23:59:59]"
        }
    }
    api_cache = {
        "insecurity-insight-healthcare-incidents": [
            {
                "Date": "2023-10-17T00:00:00.000Z",
                "Country ISO": "UKR",
            },
            {
                "Date": "2019-01-01T00:00:00.000Z",
                "Country ISO": "UKR",
            },
        ]
    }
    items_to_update = decide_which_resources_have_fresh_data(
        dataset_cache,
        api_cache,
        refresh=[],
        dataset_list=["insecurity-insight-healthcare-dataset"],
        resource_list=["insecurity-insight-healthcare-incidents"],
        topic_list=["healthcare"],
    )
    print(items_to_update, flush=True)
    assert items_to_update == [("healthcare", "2019-01-01", "2023-10-17")]


def test_parse_dates_from_string():
    dataset_date = "[2020-01-01T00:00:00 TO 2023-10-17T23:59:59]"

    start_date, end_date = parse_dates_from_string(dataset_date)

    assert start_date == "2020-01-01"
    assert end_date == "2023-10-17"


def test_refresh_spreadsheets_with_fresh_data():
    pass


def test_update_datasets_whose_resources_have_changed():
    pass
