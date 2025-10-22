#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.utilities.dateparse import now_utc
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
_TOPICS = None  # Set to list of topics to fetch
_FORCE_REFRESH = False


def main(
    save: bool = False,
    use_saved: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
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
            current_year = now_utc().year

            insecurity_insight = InsecurityInsight(configuration, retriever)
            api_cache = insecurity_insight.fetch_api_responses()
            file_paths = insecurity_insight.refresh_spreadsheets_with_fresh_data(
                api_cache, current_year, topics_to_update=_TOPICS
            )
            datasets = insecurity_insight.update_datasets(
                api_cache,
                file_paths,
                topics_to_update=_TOPICS,
            )
            for dataset in datasets:
                dataset.update_from_yaml(
                    script_dir_plus_file(
                        join("config", "hdx_dataset_static.yaml"), main
                    )
                )
                dataset.create_in_hdx(
                    remove_additional_resources=False,
                    match_resource_order=False,
                    hxl_update=False,
                    updated_by_script=_UPDATED_BY_SCRIPT,
                    batch=info["batch"],
                )
                insecurity_insight.reorder_resources(dataset)

    logger.info("Finished processing")


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_USER_AGENT_LOOKUP,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
