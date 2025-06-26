#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import script_dir_plus_file, temp_dir_batch
from hdx.utilities.retriever import Retrieve

from hdx.scraper.insecurity_insight.insecurity_insight import InsecurityInsight

setup_logging()
logger = logging.getLogger(__name__)

_USER_AGENT_LOOKUP = "hdx-scraper-insecurity-insight"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: Insecurity Insight"
_REFRESH = None  # Set to list of topics to refresh
_FORCE_REFRESH = False


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
    configuration = Configuration.read()
    with temp_dir_batch(folder=_USER_AGENT_LOOKUP) as info:
        temp_dir = info["folder"]
        with Download(rate_limit={"calls": 1, "period": 5}) as downloader:
            retriever = Retrieve(
                downloader=downloader,
                fallback_dir=temp_dir,
                saved_dir=_SAVED_DATA_DIR,
                temp_dir=temp_dir,
                save=save,
                use_saved=use_saved,
            )

            insecurity_insight = InsecurityInsight(configuration, retriever)
            api_cache = insecurity_insight.fetch_api_responses(_REFRESH)
            dataset_cache = insecurity_insight.fetch_datasets(_REFRESH)
            has_changed, changed_list = insecurity_insight.check_api_has_not_changed(
                api_cache, _REFRESH
            )
            items_to_update = insecurity_insight.decide_which_resources_have_fresh_data(
                dataset_cache, api_cache, _REFRESH, _FORCE_REFRESH
            )
            insecurity_insight.refresh_spreadsheets_with_fresh_data(
                items_to_update, api_cache
            )
            missing_report = (
                insecurity_insight.update_datasets_whose_resources_have_changed(
                    items_to_update,
                    api_cache,
                    dataset_cache,
                )
            )

            logger.info(f"{len(items_to_update)} items updated in API:")
            for ITEM in items_to_update:
                logger.info(f"{ITEM[0]:<20.20}:{ITEM[2]}")
            logger.info("")
            logger.info("Datasets with missing resources:")
            for missing in missing_report:
                logger.info(f"{missing[0]:<80.80}: {missing[1]}")

    logger.info("Finished processing")


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
