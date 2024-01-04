#!/usr/bin/env python
# encoding: utf-8

import json
import logging
import os
import re
import time

from hdx.utilities.easy_logging import setup_logging

from hdx_scraper_insecurity_insight.generate_api_transformation_schema import (
    compare_api_to_samples,
)

from hdx_scraper_insecurity_insight.utilities import (
    list_entities,
    read_attributes,
    fetch_json_from_api,
    #    fetch_json_from_samples,
    print_banner_to_log,
    read_countries,
)

from hdx_scraper_insecurity_insight.create_datasets import (
    create_or_fetch_base_dataset,
    get_date_range_from_api_response,
    get_countries_group_from_api_response,
    create_datasets_in_hdx,
)

from hdx_scraper_insecurity_insight.create_spreadsheets import create_spreadsheet

setup_logging()
LOGGER = logging.getLogger(__name__)
COUNTRY_DATASET_BASENAME = "insecurity-insight-country-dataset"


def fetch_and_cache_api_responses(save_response: bool = False) -> dict:
    api_cache = {}
    print_banner_to_log(LOGGER, "Populate API cache")

    resource_list = list_entities(type_="resource")
    for resource in resource_list:
        t0 = time.time()
        LOGGER.info(f"Fetching data for {resource} from API")
        api_cache[resource] = fetch_json_from_api(resource)

        if save_response:
            attributes = read_attributes(resource)
            with open(
                os.path.join(
                    os.path.dirname(__file__),
                    "api-samples",
                    attributes["api_response_filename"].replace(".json", "-tmp.json"),
                ),
                "w",
                encoding="UTF-8",
            ) as json_file_handle:
                json.dump(api_cache[resource], json_file_handle)
        LOGGER.info(
            f"... took {time.time()-t0:0.0f} seconds for {len(api_cache[resource])} records"
        )

    LOGGER.info(f"Loaded {len(api_cache)} API responses to cache")
    assert len(api_cache) == 11, "Did not find data from expected 11 endpoints"
    return api_cache


def fetch_and_cache_datasets():
    dataset_cache = {}
    print_banner_to_log(LOGGER, "Populate dataset cache")
    dataset_list = list_entities(type_="dataset")
    # load topic datasets
    n_topic_datasets = 0
    for dataset in dataset_list:
        if dataset == COUNTRY_DATASET_BASENAME:
            continue
        dataset_cache[dataset], _ = create_or_fetch_base_dataset(dataset)
        n_topic_datasets += 1

    # Load country datasets
    countries = read_countries()
    n_countries = 0
    for country in countries.keys():
        dataset_name = COUNTRY_DATASET_BASENAME.replace("country", country.lower())
        dataset_cache[dataset_name], _ = create_or_fetch_base_dataset(
            COUNTRY_DATASET_BASENAME, country_filter=country
        )
        n_countries += 1

    LOGGER.info(f"Loaded {len(dataset_cache)} datasets to cache")

    assert n_topic_datasets == 6, f"Found {n_topic_datasets} not 7 topic datasets"
    assert n_countries == 25, f"Found {n_countries} not 25 expected country datasets"
    return dataset_cache


def check_api_has_not_changed(api_cache: dict) -> (bool, list):
    has_changed, changed_list = compare_api_to_samples(api_cache)
    LOGGER.info("\nChanged API endpoints:")
    for dataset_name in changed_list:
        LOGGER.info(dataset_name)

    assert not has_changed, "!!One or more of the Insecurity Insight endpoints has changed format"
    return has_changed, changed_list


def decide_which_resources_have_fresh_data(dataset_cache: dict, api_cache: dict) -> list[str]:
    print_banner_to_log(LOGGER, "Identify updates")

    # Dates from dataset records
    dataset_list = list_entities(type_="dataset")
    dataset_recency = {}
    for dataset in dataset_list:
        if dataset == COUNTRY_DATASET_BASENAME:
            continue
        dataset_update_date = update_date_from_string(dataset_cache[dataset]["dataset_date"])
        dataset_recency[dataset] = dataset_update_date

    # Dates from resources
    resource_list = list_entities(type_="resource")

    resource_recency = {}
    resource_start_date = {}
    for resource in resource_list:
        if not resource.endswith("-incidents"):
            continue
        start_date, end_date = get_date_range_from_api_response(api_cache[resource])
        end_date = update_date_from_string(end_date)
        resource_recency[resource] = end_date
        resource_start_date[resource] = start_date

    # Compare
    items_to_update = []
    LOGGER.info(f"{'item':<15} {'API Date':<10} {'Dataset Date':<8}")
    for item in ["crsv", "education", "explosive", "healthcare", "protection", "aidworkerKIKA"]:
        resource_key = f"insecurity-insight-{item}-incidents"
        dataset_key = f"insecurity-insight-{item}-dataset"
        update_str = ""
        if resource_recency[resource_key] > dataset_recency[dataset_key]:
            update_str = "*"
            items_to_update.append(
                (item, resource_start_date[resource_key], resource_recency[resource_key])
            )

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


def refresh_spreadsheets_with_fresh_data(items_to_update: list[str], api_cache: dict):
    print_banner_to_log(LOGGER, "Refresh spreadsheets")
    if len(items_to_update) == 0:
        LOGGER.info("No spreadsheets need to be updated")
        return

    resources = list_entities(type_="resource")

    LOGGER.info(f"Refreshing {len(items_to_update)} topic spreadsheets")
    for item in items_to_update:
        for resource in resources:
            if item[0] in resource:
                status = create_spreadsheet(resource, api_response=api_cache[resource])
                LOGGER.info(status)

    LOGGER.info("Refreshing all country spreadsheets")
    countries = read_countries()
    country_attributes = read_attributes(COUNTRY_DATASET_BASENAME)
    resource_names = country_attributes["resource"]
    for country in countries:
        for resource in resource_names:
            status = create_spreadsheet(
                resource, country_filter=country, api_response=api_cache[resource]
            )
            LOGGER.info(status)
        break  # just do one spreadsheet for testing


def update_datasets_whose_resources_have_changed(
    items_to_update: list[str], api_cache: dict, dataset_cache: dict
):
    print_banner_to_log(LOGGER, "Update datasets")
    if len(items_to_update) == 0:
        LOGGER.info("No datasets need to be updated")
        return

    datasets = list_entities(type_="dataset")
    for item in items_to_update:
        for dataset_name in datasets:
            if item[0] in dataset_name:
                LOGGER.info("**Not handling country datasets**")
                countries_group = get_countries_group_from_api_response(
                    api_cache[f"insecurity-insight-{item[0]}-incidents"]
                )
                dataset_date = f"[{item[1]} TO {item[2]}]"
                create_datasets_in_hdx(
                    dataset_name,
                    dataset_cache=dataset_cache,
                    dataset_date=dataset_date,
                    countries_group=countries_group,
                    dry_run=True,
                )


if __name__ == "__main__":
    API_CACHE = fetch_and_cache_api_responses()
    DATASET_CACHE = fetch_and_cache_datasets()
    check_api_has_not_changed(API_CACHE)
    ITEMS_TO_UPDATE = decide_which_resources_have_fresh_data(DATASET_CACHE, API_CACHE)
    refresh_spreadsheets_with_fresh_data(ITEMS_TO_UPDATE, API_CACHE)
    update_datasets_whose_resources_have_changed(ITEMS_TO_UPDATE, API_CACHE, DATASET_CACHE)
