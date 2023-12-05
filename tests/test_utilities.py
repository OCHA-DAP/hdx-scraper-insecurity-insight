#!/usr/bin/env python
# encoding: utf-8


from hdx_scraper_insecurity_insight.utilities import (
    read_schema,
    read_attributes,
    fetch_json_from_samples,
    fetch_json_from_api,
    list_entities,
    filter_json_rows,
)


def test_read_schema():
    dataset_name = "insecurity-insight-crsv-incidents"
    hdx_row, row_template = read_schema(dataset_name)

    assert hdx_row.keys() == row_template.keys()
    assert len(hdx_row.keys()) == 22


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
        "insecurity-insight-crsv-incidents",
        "insecurity-insight-crsv-overview",
    ]


def test_fetching_json():
    dataset_name = "insecurity-insight-crsv-incidents"

    samples_response = fetch_json_from_samples(dataset_name)
    api_response = fetch_json_from_api(dataset_name)

    assert samples_response[0].keys() == api_response[0].keys()


def test_filter_json_rows():
    dataset_name = "insecurity-insight-crsv-incidents"
    sample_response = fetch_json_from_samples(dataset_name)
    filtered_response = filter_json_rows("NGA", "2021", sample_response)

    assert len(filtered_response) == 15


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


def test_entity_list_resourcess():
    resource_list = list_entities(type_="resource")

    assert len(resource_list) == 11
