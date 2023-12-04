#!/usr/bin/env python
# encoding: utf-8

import datetime
import logging
import sys

from hdx.utilities.easy_logging import setup_logging

from hdx_scraper_insecurity_insight.utilities import (
    fetch_json_from_api,
    fetch_json_from_samples,
    list_entities,
    read_attributes,
)

setup_logging()
LOGGER = logging.getLogger(__name__)


def check_api_has_not_changed() -> bool:
    LOGGER.info("***************************************")
    LOGGER.info("* Insecurity Insight - Pipeline run   *")
    LOGGER.info(f"* Invoked at: {datetime.datetime.now().isoformat(): <23}*")
    LOGGER.info("*********************************************")

    dataset_names = list_entities(type_="resource")

    LOGGER.info(f"Found {len(dataset_names)} endpoints")

    api_changed = False
    for dataset_name in dataset_names:
        attributes = read_attributes(dataset_name)
        LOGGER.info("\n")
        LOGGER.info(f"Processing {dataset_name}")
        LOGGER.info(f"API endpoint: {attributes['api_url']}")
        LOGGER.info(f"API sample: {attributes['api_response_filename']}")

        samples_response = fetch_json_from_samples(dataset_name)
        api_response = fetch_json_from_api(dataset_name)

        sample_keys = samples_response[0].keys()
        api_keys = api_response[0].keys()
        LOGGER.info(f"Number of API records: {len(api_response)}")
        LOGGER.info(f"Number of sample keys: {len(samples_response)}")
        if sample_keys == api_keys:
            LOGGER.info(f"API response matches sample for {dataset_name}")
        else:
            LOGGER.info(f"**MISMATCH between API and sample for {dataset_name}")
            LOGGER.info(f"Number of API endpoint record keys: {len(api_keys)}")
            LOGGER.info(f"Number of sample record keys: {len(sample_keys)}")
            LOGGER.info(f"API keys: {api_keys}")
            LOGGER.info(f"Sample keys: {sample_keys}")
            show_response_overlap(api_keys, sample_keys)
            api_changed = True

    return api_changed


def show_response_overlap(api_keys: list[dict], sample_keys: list[dict]):
    LOGGER.info("Keys in API data but not in sample")
    LOGGER.info(set(api_keys).difference(set(sample_keys)))

    LOGGER.info("Keys in sample but not in API data")
    LOGGER.info(set(sample_keys).difference(api_keys))


if __name__ == "__main__":
    HAS_CHANGED = check_api_has_not_changed()
    assert not HAS_CHANGED, "!!One or more of the Insecurity Insight endpoints has changed format"
