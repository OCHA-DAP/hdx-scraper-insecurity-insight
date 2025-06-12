#!/usr/bin/env python
# encoding: utf-8

import logging
import time
from os.path import expanduser, join

# from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import script_dir_plus_file

from hdx.scraper.insecurity_insight.insecurity_insight import (
    check_api_has_not_changed,
    decide_which_resources_have_fresh_data,
    fetch_and_cache_api_responses,
    fetch_and_cache_datasets,
    refresh_spreadsheets_with_fresh_data,
    update_datasets_whose_resources_have_changed,
)
from hdx.scraper.insecurity_insight.utilities import print_banner_to_log

setup_logging()
logger = logging.getLogger(__name__)

_USER_AGENT_LOOKUP = "hdx-scraper-insecurity-insight"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: Insecurity Insight"


def main(
    save: bool = True,
    use_saved: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to True.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """
    # configuration = Configuration.read()
    USE_SAMPLE = False
    DRY_RUN = False
    REFRESH = ["foodsecurity"]  # ["all"]
    # COUNTRIES = None  # ["PSE"]
    USE_LEGACY = True
    HDX_SITE = "prod"
    T0 = time.time()
    print_banner_to_log(logger, "Grand Run")
    API_CACHE = fetch_and_cache_api_responses(use_sample=USE_SAMPLE)
    DATASET_CACHE = fetch_and_cache_datasets(use_legacy=USE_LEGACY, hdx_site=HDX_SITE)
    HAS_CHANGED, CHANGED_LIST = check_api_has_not_changed(API_CACHE)
    # Using refresh here allows a forced refresh for particular datasets
    ITEMS_TO_UPDATE = decide_which_resources_have_fresh_data(
        DATASET_CACHE, API_CACHE, refresh=REFRESH
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

    logger.info(f"{len(ITEMS_TO_UPDATE)} items updated in API:")
    for ITEM in ITEMS_TO_UPDATE:
        logger.info(f"{ITEM[0]:<20.20}:{ITEM[2]}")
    logger.info("")
    logger.info("Datasets with missing resources:")
    for MISSING in MISSING_REPORT:
        logger.info(f"{MISSING[0]:<80.80}: {MISSING[1]}")

    logger.info(f"Total run time: {time.time() - T0:0.0f} seconds")


if __name__ == "__main__":
    facade(
        main,
        hdx_site="dev",
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_USER_AGENT_LOOKUP,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
