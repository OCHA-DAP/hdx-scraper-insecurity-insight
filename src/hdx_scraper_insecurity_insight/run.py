#!/usr/bin/env python
# encoding: utf-8

import logging

from hdx.utilities.easy_logging import setup_logging

from hdx_scraper_insecurity_insight.generate_api_transformation_schema import (
    check_api_has_not_changed,
)

setup_logging()
LOGGER = logging.getLogger(__name__)


def main():
    # Check API has not changed
    has_changed, changed_list = check_api_has_not_changed()
    print("\nChanged API endpoints:", flush=True)
    for dataset_name in changed_list:
        print(dataset_name, flush=True)

    assert not has_changed, "!!One or more of the Insecurity Insight endpoints has changed format"

    # Fetch and cache_datasets


def fetch_and_cache_datasets():
    pass


def decide_which_resources_have_fresh_data():
    pass


def refresh_spreadsheets_with_fresh_data():
    pass


def update_datasets_whose_resources_have_changed():
    pass


if __name__ == "__main__":
    main()
