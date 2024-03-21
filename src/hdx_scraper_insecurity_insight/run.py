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
    fetch_json,
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
API_DELAY = 5


def fetch_and_cache_api_responses(save_response: bool = False, use_sample: bool = False) -> dict:
    api_cache = {}
    print_banner_to_log(LOGGER, "Populate API cache")

    resource_list = list_entities(type_="resource")
    for resource in resource_list:
        t0 = time.time()
        if not use_sample:
            LOGGER.info(f"Fetching data for {resource} from API")
        else:
            LOGGER.info(f"Fetching data for {resource} from samples")
        api_cache[resource] = fetch_json(resource, use_sample=use_sample)

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
        if not use_sample:
            LOGGER.info(f"Delaying next call for {API_DELAY} seconds")
            time.sleep(API_DELAY)

    LOGGER.info(f"Loaded {len(api_cache)} API responses to cache")
    assert len(api_cache) == 12, "Did not find data from expected 12 endpoints"
    return api_cache


def fetch_and_cache_datasets(use_legacy: bool = False) -> dict:
    dataset_cache = {}
    print_banner_to_log(LOGGER, "Populate dataset cache")
    dataset_list = list_entities(type_="dataset")
    # load topic datasets
    n_topic_datasets = 0
    for dataset in dataset_list:
        print(dataset, flush=True)
        if dataset == COUNTRY_DATASET_BASENAME:
            continue
        dataset_cache[dataset], _ = create_or_fetch_base_dataset(dataset, use_legacy=use_legacy)
        n_topic_datasets += 1

    # Load country datasets
    countries = read_countries()
    n_countries = 0
    for country in countries.keys():
        dataset_name = COUNTRY_DATASET_BASENAME.replace("country", country.lower())
        dataset_cache[dataset_name], _ = create_or_fetch_base_dataset(
            COUNTRY_DATASET_BASENAME, country_filter=country, use_legacy=use_legacy
        )
        n_countries += 1

    LOGGER.info(f"Loaded {len(dataset_cache)} datasets to cache")

    assert n_topic_datasets == 6, f"Found {n_topic_datasets} not 6 topic datasets"
    assert n_countries == 24, f"Found {n_countries} not 24 expected country datasets"
    return dataset_cache


def check_api_has_not_changed(api_cache: dict) -> (bool, list):
    has_changed, changed_list = compare_api_to_samples(api_cache)
    LOGGER.info("\nChanged API endpoints:")
    for dataset_name in changed_list:
        LOGGER.info(dataset_name)

    assert not has_changed, "!!One or more of the Insecurity Insight endpoints has changed format"
    return has_changed, changed_list


def decide_which_resources_have_fresh_data(
    dataset_cache: dict, api_cache: dict, refresh_all: bool = False
) -> list[str]:
    print_banner_to_log(LOGGER, "Identify updates")
    if refresh_all:
        LOGGER.info("`refresh_all` true so all resources will be refreshed")

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
        elif refresh_all:
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

    LOGGER.info(f"Refreshing topic spreadsheets for {','.join([x[0] for x in items_to_update])}")
    for item in items_to_update:
        for resource in resources:
            if item[0] in resource:
                status = create_spreadsheet(resource, api_response=api_cache[resource])
                LOGGER.info(status)

    LOGGER.info("Refreshing all country spreadsheets")
    # LOGGER.info("**ONLY DOING ONE COUNTRY FOR TEST**")
    countries = read_countries()
    country_attributes = read_attributes(COUNTRY_DATASET_BASENAME)
    resource_names = country_attributes["resource"]
    for country in countries:
        LOGGER.info(f"Processing for {country}")
        for resource in resource_names:
            status = create_spreadsheet(
                resource, country_filter=country, api_response=api_cache[resource]
            )
            LOGGER.info(status)

        # break  # just do one spreadsheet for testing


def update_datasets_whose_resources_have_changed(
    items_to_update: list[str],
    api_cache: dict,
    dataset_cache: dict,
    dry_run: bool = False,
    use_legacy: bool = False,
    hdx_site: str = "stage",
) -> list[list]:
    print_banner_to_log(LOGGER, "Update datasets")
    if len(items_to_update) == 0:
        LOGGER.info("No datasets need to be updated")
        return []

    missing_report = []
    datasets = list_entities(type_="dataset")
    for item in items_to_update:
        for dataset_name in datasets:
            if item[0] in dataset_name:
                countries_group = get_countries_group_from_api_response(
                    api_cache[f"insecurity-insight-{item[0]}-incidents"]
                )
                dataset_date = f"[{item[1]} TO {item[2]}]"
                dataset, n_missing_resources = create_datasets_in_hdx(
                    dataset_name,
                    dataset_cache=dataset_cache,
                    dataset_date=dataset_date,
                    countries_group=countries_group,
                    dry_run=dry_run,
                    use_legacy=use_legacy,
                    hdx_site=hdx_site,
                )
            if n_missing_resources != 0:
                missing_report.append([dataset["name"], n_missing_resources])

    # If any data has updated we update all of the country datasets
    # LOGGER.info("**ONLY DOING ONE COUNTRY FOR TEST**")
    countries = read_countries()
    start_date = min([x[1] for x in items_to_update])
    end_date = max([x[2] for x in items_to_update])
    dataset_date = f"[{start_date} TO {end_date}]"
    for country in countries:
        countries_group = [{"name": country.lower()}]

        dataset, n_missing_resources = create_datasets_in_hdx(
            COUNTRY_DATASET_BASENAME,
            country_filter=country,
            dataset_cache=dataset_cache,
            dataset_date=dataset_date,
            countries_group=countries_group,
            dry_run=dry_run,
        )
        if n_missing_resources != 0:
            missing_report.append([dataset["name"], n_missing_resources])

    return missing_report


if __name__ == "__main__":
    USE_SAMPLE = False
    DRY_RUN = False
    REFRESH_ALL = True
    USE_LEGACY = False
    HDX_SITE = "stage"
    T0 = time.time()
    print_banner_to_log(LOGGER, "Grand Run")
    API_CACHE = fetch_and_cache_api_responses(use_sample=USE_SAMPLE)
    DATASET_CACHE = fetch_and_cache_datasets(use_legacy=USE_LEGACY)
    HAS_CHANGED, CHANGED_LIST = check_api_has_not_changed(API_CACHE)
    ITEMS_TO_UPDATE = decide_which_resources_have_fresh_data(
        DATASET_CACHE, API_CACHE, refresh_all=REFRESH_ALL
    )
    refresh_spreadsheets_with_fresh_data(ITEMS_TO_UPDATE, API_CACHE)
    MISSING_REPORT = update_datasets_whose_resources_have_changed(
        ITEMS_TO_UPDATE,
        API_CACHE,
        DATASET_CACHE,
        dry_run=DRY_RUN,
        use_legacy=USE_LEGACY,
        hdx_site=HDX_SITE,
    )

    LOGGER.info(f"{len(ITEMS_TO_UPDATE)} items updated in API:")
    for ITEM in ITEMS_TO_UPDATE:
        LOGGER.info(f"{ITEM[0]:<20.20}:{ITEM[2]}")
    LOGGER.info("")
    LOGGER.info("Datasets with missing resources:")
    for MISSING in MISSING_REPORT:
        LOGGER.info(f"{MISSING[0]:<80.80}: {MISSING[1]}")

    LOGGER.info(f"Total run time: {time.time()-T0:0.0f} seconds")
