#!/usr/bin/env python
# encoding: utf-8

from hdx_scraper_insecurity_insight.generate_api_transformation_schema import (
    generate_schema,
    compare_api_to_samples,
    print_country_codes_analysis,
)
from hdx_scraper_insecurity_insight.utilities import fetch_json_from_samples


def test_generate_schema():
    dataset_name = "insecurity-insight-crsv-incidents"
    summary = generate_schema(dataset_name)

    assert summary == {
        "dataset_name": "insecurity-insight-crsv-incidents",
        "n_api_fields": 22,
        "n_spreadsheet_fields": 22,
        "n_hxl_tags": 18,
    }


def test_generate_schema_where_no_example_spreadsheet():
    dataset_name = "insecurity-insight-crsv-overview"
    summary = generate_schema(dataset_name)

    assert summary == {
        "dataset_name": "insecurity-insight-crsv-overview",
        "n_api_fields": 10,
        "n_spreadsheet_fields": 10,
        "n_hxl_tags": 0,
    }


def test_compare_api_to_samples_same():
    dataset_name = "insecurity-insight-crsv-overview"
    api_cache = {}
    api_cache[dataset_name] = fetch_json_from_samples(dataset_name)

    api_changed, change_list = compare_api_to_samples(api_cache, dataset_names=[dataset_name])

    assert not api_changed
    assert len(change_list) == 0


def test_compare_api_to_samples_changed():
    dataset_name = "insecurity-insight-crsv-overview"
    api_cache = {}
    api_cache[dataset_name] = []

    api_changed, change_list = compare_api_to_samples(api_cache, dataset_names=[dataset_name])

    assert api_changed
    assert len(change_list) == 1


def test_print_country_codes_analysis():
    dataset_name = "insecurity-insight-crsv-overview"
    api_response = fetch_json_from_samples(dataset_name)
    api_countries_not_in_hdx, hdx_countries_not_in_api = print_country_codes_analysis(api_response)

    print(api_countries_not_in_hdx, flush=True)

    print(hdx_countries_not_in_api, flush=True)

    assert (
        len(api_countries_not_in_hdx) == 57
    )  # Many countries appear in the API but do not have an HDX page
    assert len(hdx_countries_not_in_api) == 1  # IRQ appears in HDX but not API
