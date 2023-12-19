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
)

from hdx_scraper_insecurity_insight.create_datasets import create_or_fetch_base_dataset

setup_logging()
LOGGER = logging.getLogger(__name__)

API_CACHE = {}
DATASET_CACHE = {}


def fetch_and_cache_datasets():
    global API_CACHE, DATASET_CACHE

    resource_list = list_entities(type_="resource")
    for resource in resource_list:
        LOGGER.info(f"Fetching data for {resource} from **samples** not API")
        API_CACHE[resource] = fetch_json_from_samples(resource)

    LOGGER.info(f"Loaded {len(API_CACHE)} API responses to cache")

    dataset_list = list_entities(type_="dataset")
    for dataset in dataset_list:
        DATASET_CACHE[dataset] = create_or_fetch_base_dataset(dataset)

    LOGGER.info(f"Loaded {len(DATASET_CACHE)} datasets to cache")

    assert len(API_CACHE) == 11, "Did not find data from expected 11 endpoints"
    assert len(DATASET_CACHE) == 7, "Did not find expected 7 datasets"


def check_api_has_not_changed():
    # Check API has not changed
    has_changed, changed_list = compare_api_to_samples(API_CACHE)
    LOGGER.info("\nChanged API endpoints:")
    for dataset_name in changed_list:
        LOGGER.info(dataset_name, flush=True)

    assert not has_changed, "!!One or more of the Insecurity Insight endpoints has changed format"


def decide_which_resources_have_fresh_data():
    dataset_list = list_entities(type_="dataset")

    dataset_recency = {}

    for dataset in dataset_list:
        dataset_update_date = update_date_from_string(DATASET_CACHE[dataset]["dataset_date"])
        LOGGER.info(f"{dataset} last updated {dataset_update_date}")
        dataset_recency[dataset] = dataset_update_date


def update_date_from_string(date_str: str) -> list[str]:
    matched_strings = re.findall(r"(\d{4}-([0]\d|1[0-2])-([0-2]\d|3[01]))", date_str)

    update_date = ""
    if len(matched_strings) > 0:
        update_date = matched_strings[-1][0]

    return update_date


def refresh_spreadsheets_with_fresh_data():
    pass


def update_datasets_whose_resources_have_changed():
    pass


if __name__ == "__main__":
    fetch_and_cache_datasets()
    check_api_has_not_changed()
    decide_which_resources_have_fresh_data()
