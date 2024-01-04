#!/usr/bin/env python
# encoding: utf-8
import pytest

from hdx_scraper_insecurity_insight.run import (
    update_date_from_string,
    fetch_and_cache_datasets,
    fetch_and_cache_api_responses,
    # decide_which_resources_have_fresh_data,
    # refresh_spreadsheets_with_fresh_data,
    # update_datasets_whose_resources_have_changed,
)


@pytest.mark.skip(reason="Testing API is really slow, and not a unit test")
def test_fetch_and_cache_api_response():
    api_cache = fetch_and_cache_api_responses()

    assert len(api_cache) == 11


def test_fetch_and_cache_datasets():
    dataset_cache = fetch_and_cache_datasets()

    assert len(dataset_cache) == 31


def test_check_api_has_not_changed():
    # compare_api_to_samples is tested, effectively in
    # test_generate_api_transformation_schema.test_compare_api_to_samples_changed
    assert True


def test_decide_which_resources_have_fresh_data():
    pass


def test_update_date_from_string():
    dataset_date = "[2020-01-01T00:00:00 TO 2023-10-17T23:59:59]"

    update_date = update_date_from_string(dataset_date)

    assert update_date == "2023-10-17"


def test_refresh_spreadsheets_with_fresh_data():
    pass


def test_update_datasets_whose_resources_have_changed():
    pass
