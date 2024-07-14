#!/usr/bin/env python
# encoding: utf-8

import csv
import logging
import os

from hdx_scraper_insecurity_insight.utilities import (
    fetch_json,
    fetch_json_from_api,
    fetch_json_from_samples,
    filter_json_rows,
    censor_location,
    list_entities,
    parse_commandline_arguments,
    print_banner_to_log,
    read_attributes,
    read_insecurity_insight_attributes_pages,
    read_insecurity_insight_resource_attributes,
    read_countries,
    read_field_mappings,
    read_schema,
    write_dictionary,
    write_schema,
)

LOGGER = logging.getLogger(__name__)


def test_read_schema():
    dataset_name = "insecurity-insight-crsv-incidents"
    hdx_row, row_template = read_schema(dataset_name)

    assert hdx_row.keys() == row_template.keys()
    assert len(hdx_row.keys()) == 22


def test_read_schema_overview():
    dataset_name = "insecurity-insight-crsv-overview"
    hdx_row, row_template = read_schema(dataset_name)

    assert hdx_row.keys() == row_template.keys()
    assert len(hdx_row.keys()) == 10


def test_read_attributes():
    dataset_name = "insecurity-insight-crsv-incidents"
    attributes = read_attributes(dataset_name)

    assert set(attributes.keys()) == set(
        [
            "legacy_resource_filename",
            "entity_type",
            "description",
            "api_url",
            "api_response_filename",
            "filename_template",
            "file_format",
        ]
    )


def test_read_attributes_list():
    dataset_name = "insecurity-insight-crsv-dataset"
    attributes = read_attributes(dataset_name)

    assert attributes["resource"] == [
        "insecurity-insight-crsv-incidents-current-year",
        "insecurity-insight-crsv-incidents",
        "insecurity-insight-crsv-overview",
    ]


def test_fetching_json():
    dataset_name = "insecurity-insight-crsv-incidents"

    samples_response = fetch_json_from_samples(dataset_name)
    api_response = fetch_json_from_api(dataset_name)

    assert samples_response[0].keys() == api_response[0].keys()


def test_fetch_json_generic():
    dataset_name = "insecurity-insight-crsv-overview"
    sample_response = fetch_json(dataset_name, use_sample=True)

    assert set(sample_response[0].keys()) == set(
        [
            "Country",
            "Year",
            "Country ISO",
            "Recorded SV Events",
            "CRSV events",
            "SV by Security Personnel",
            "Events Affecting Minors",
            "Events Affecting Aid Workers",
            "Events Affecting Health Workers",
            "Events Affecting Educators",
        ]
    )


def test_filter_json_rows():
    dataset_name = "insecurity-insight-crsv-incidents"
    sample_response = fetch_json_from_samples(dataset_name)
    filtered_response = filter_json_rows("NGA", "2021", sample_response)

    assert len(filtered_response) == 15


def test_censor_location():
    dataset_name = "insecurity-insight-healthcare-incidents"
    sample_response = fetch_json_from_samples(dataset_name)
    censored_response = censor_location(["PSE"], sample_response)

    censored_count = len([x for x in censored_response if x["Geo Precision"] == "censored"])
    pse_count = len([x for x in censored_response if x["Country ISO"] == "PSE"])

    for i, record in enumerate(censored_response):
        print(
            i,
            record["Country ISO"],
            record["Latitude"],
            record["Longitude"],
            record["Geo Precision"],
            flush=True,
        )
        if record["Country ISO"] == "PSE":
            assert len(record["Latitude"]) == 0
            assert len(record["Longitude"]) == 0
            assert record["Geo Precision"] == "censored"
            break
        else:
            assert float(record["Latitude"])
            assert float(record["Longitude"])
            assert record["Geo Precision"] != "censored"

    assert censored_count == pse_count

    assert len(censored_response) == len(sample_response)


def test_entity_list_datasets():
    dataset_list = list_entities()
    print(dataset_list, flush=True)

    assert dataset_list == [
        "insecurity-insight-crsv-dataset",
        "insecurity-insight-education-dataset",
        "insecurity-insight-explosive-dataset",
        "insecurity-insight-healthcare-dataset",
        "insecurity-insight-protection-dataset",
        "insecurity-insight-aidworkerKIKA-dataset",
        "insecurity-insight-country-dataset",
    ]


def test_read_insecurity_insight_attributes_pages():
    dataset_list = list_entities()

    for dataset_name in dataset_list:
        if dataset_name == "insecurity-insight-country-dataset":
            continue
        ii_attributes = read_insecurity_insight_attributes_pages(dataset_name)
        assert ii_attributes
        if ii_attributes:
            print(f"{dataset_name},{ii_attributes['Page']},{ii_attributes['legacy_name']}")
        else:
            print(dataset_name)


def test_read_insecurity_insight_attributes_pages_countries():
    dataset_template = "insecurity-insight-{country}-dataset"
    countries = read_countries()

    for country in countries:
        dataset_name = dataset_template.format(country=country.lower())

        ii_attributes = read_insecurity_insight_attributes_pages(dataset_name)
        assert ii_attributes
        if ii_attributes:
            print(f"{dataset_name},{ii_attributes['Page']},{ii_attributes['legacy_name']}")
        else:
            print(dataset_name)


def test_read_insecurity_insight_resource_attributes():
    dataset_list = list_entities()

    for dataset_name in dataset_list:
        if dataset_name == "insecurity-insight-country-dataset":
            continue
        resource_list = read_insecurity_insight_resource_attributes(dataset_name)

        assert len(resource_list) != 0
        print(dataset_name, flush=True)
        for resource_ in resource_list:
            print(f"\t{resource_['ih_name']}", flush=True)


def test_read_insecurity_insight_resource_attributes_countries():
    dataset_template = "insecurity-insight-{country}-dataset"
    countries = read_countries()

    for country in countries:
        dataset_name = dataset_template.format(country=country.lower())

        resource_list = read_insecurity_insight_resource_attributes(dataset_name)
        assert len(resource_list) != 0
        print(dataset_name, flush=True)
        for resource_ in resource_list:
            print(f"\t{resource_['ih_name']}", flush=True)


def test_entity_list_resourcess():
    resource_list = list_entities(type_="resource")

    assert len(resource_list) == 18


def test_commandline_argument_handling_two_arg(monkeypatch):
    monkeypatch.setattr("sys.argv", ["application.name", "test-dataset-name", "afg"])

    dataset_name, country_iso = parse_commandline_arguments()

    assert dataset_name == "test-dataset-name"
    assert country_iso == "afg"


def test_commandline_argument_handling_one_arg(monkeypatch):
    monkeypatch.setattr("sys.argv", ["application.name", "test-dataset-name"])

    dataset_name, country_iso = parse_commandline_arguments()

    assert dataset_name == "test-dataset-name"
    assert country_iso == ""


def test_print_banner_to_log(caplog):
    caplog.set_level(logging.INFO)
    print_banner_to_log(LOGGER, "test-banner")

    log_rows = caplog.text.split("\n")
    assert len(log_rows) == 5
    assert len(log_rows[0]) == len(log_rows[1])
    assert "test-banner" in caplog.text


def test_write_dictionary_to_local_file():
    temp_file_path = os.path.join(os.path.dirname(__file__), "fixtures", "test.csv")
    if os.path.isfile(temp_file_path):
        os.remove(temp_file_path)

    dict_list = [
        {"a": 1, "b": 2, "c": 3},
        {"a": 4, "b": 5, "c": 6},
        {"a": 7, "b": 8, "c": 9},
    ]

    status = write_dictionary(temp_file_path, dict_list)

    with open(temp_file_path, "r", encoding="utf-8") as file_handle:
        rows_read = list(csv.DictReader(file_handle))

    assert len(rows_read) == 3
    assert rows_read[0] == {"a": "1", "b": "2", "c": "3"}
    assert "New file" in status
    assert "is being created" in status


def test_write_schema():
    dataset_name = "insecurity-insight-crsv-incidents"
    file_status = write_schema(dataset_name, [])

    assert "Schema for insecurity-insight-crsv-incidents already in" in file_status


def test_read_field_mappings():
    field_mappings = read_field_mappings()

    assert len(field_mappings) == 1


def test_read_countries():
    countries = read_countries()

    assert len(countries) == 25
