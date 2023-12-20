#!/usr/bin/env python
# encoding: utf-8

import logging
import re

from hdx.utilities.easy_logging import setup_logging

from hdx_scraper_insecurity_insight.generate_api_transformation_schema import (
    compare_api_to_samples,
)

from hdx_scraper_insecurity_insight.utilities import (
    list_entities,
    read_attributes,
    fetch_json_from_api,
    fetch_json_from_samples,
    print_banner_to_log,
)

from hdx_scraper_insecurity_insight.create_datasets import (
    create_or_fetch_base_dataset,
    get_date_range_from_api_response,
)

from hdx_scraper_insecurity_insight.create_spreadsheets import create_spreadsheet

setup_logging()
LOGGER = logging.getLogger(__name__)

API_CACHE = {}
DATASET_CACHE = {}


def fetch_and_cache_api_responses():
    global API_CACHE
    print_banner_to_log(LOGGER, "Populate API cache")

    resource_list = list_entities(type_="resource")
    for resource in resource_list:
        LOGGER.info(f"Fetching data for {resource} from **samples** not API")
        API_CACHE[resource] = fetch_json_from_samples(resource)

    LOGGER.info(f"Loaded {len(API_CACHE)} API responses to cache")
    assert len(API_CACHE) == 11, "Did not find data from expected 11 endpoints"


def fetch_and_cache_datasets():
    global DATASET_CACHE
    print_banner_to_log(LOGGER, "Populate dataset cache")
    dataset_list = list_entities(type_="dataset")
    for dataset in dataset_list:
        DATASET_CACHE[dataset] = create_or_fetch_base_dataset(dataset)

    # Load country datasets
    LOGGER.info(f"Loaded {len(DATASET_CACHE)} datasets to cache")

    assert len(DATASET_CACHE) == 7, "Did not find expected 7 datasets"


def check_api_has_not_changed():
    # Check API has not changed
    has_changed, changed_list = compare_api_to_samples(API_CACHE)
    LOGGER.info("\nChanged API endpoints:")
    for dataset_name in changed_list:
        LOGGER.info(dataset_name, flush=True)

    assert not has_changed, "!!One or more of the Insecurity Insight endpoints has changed format"


def decide_which_resources_have_fresh_data() -> list[str]:
    print_banner_to_log(LOGGER, "Identify updates")
    dataset_list = list_entities(type_="dataset")

    dataset_recency = {}

    for dataset in dataset_list:
        dataset_update_date = update_date_from_string(DATASET_CACHE[dataset]["dataset_date"])
        dataset_recency[dataset] = dataset_update_date

    resource_list = list_entities(type_="resource")

    resource_recency = {}
    for resource in resource_list:
        if not resource.endswith("-incidents"):
            continue
        _, end_date = get_date_range_from_api_response(API_CACHE[resource])
        end_date = update_date_from_string(end_date)
        resource_recency[resource] = end_date

    items_to_update = []
    LOGGER.info(f"{'item':<15} {'API Date':<10} {'Dataset Date':<8}")
    for item in ["crsv", "education", "explosive", "healthcare", "protection", "aidworkerKIKA"]:
        resource_key = f"insecurity-insight-{item}-incidents"
        dataset_key = f"insecurity-insight-{item}-dataset"
        update_str = ""
        if resource_recency[resource_key] > dataset_recency[dataset_key]:
            update_str = "*"
            items_to_update.append(item)

        LOGGER.info(
            f"{item:<15} "
            f"{resource_recency[resource_key]} "
            f"{dataset_recency[dataset_key]}"
            f"{update_str}"
        )

    return items_to_update


def update_date_from_string(date_str: str) -> list[str]:
    matched_strings = re.findall(r"(\d{4}-([0]\d|1[0-2])-([0-2]\d|3[01]))", date_str)

    update_date = ""
    if len(matched_strings) > 0:
        update_date = matched_strings[-1][0]

    return update_date


def refresh_spreadsheets_with_fresh_data(items_to_update: list[str]):
    print_banner_to_log(LOGGER, "Refresh spreadsheets")
    if len(items_to_update) == 0:
        LOGGER.info("No spreadsheets need to be updated")
        return

    resources = list_entities(type_="resource")

    for item in items_to_update:
        for resource in resources:
            if item in resource:
                LOGGER.info("**Really we should be generating many country files**")
                create_spreadsheet(resource, api_response=API_CACHE[resource])


def update_datasets_whose_resources_have_changed(items_to_update: list[str]):
    print_banner_to_log(LOGGER, "Update datasets")
    if len(items_to_update) == 0:
        LOGGER.info("No datasets need to be updated")
        return


if __name__ == "__main__":
    fetch_and_cache_api_responses()
    fetch_and_cache_datasets()
    check_api_has_not_changed()
    ITEMS_TO_UPDATE = decide_which_resources_have_fresh_data()
    refresh_spreadsheets_with_fresh_data(ITEMS_TO_UPDATE)
    update_datasets_whose_resources_have_changed(ITEMS_TO_UPDATE)
