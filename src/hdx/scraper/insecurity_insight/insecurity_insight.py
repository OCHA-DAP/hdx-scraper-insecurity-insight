#!/usr/bin/python
"""insecurity insight scraper"""

import logging
from os.path import dirname, join
from typing import Optional

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.base_downloader import DownloadError
from hdx.utilities.dictandlist import merge_two_dictionaries
from hdx.utilities.loader import load_json
from hdx.utilities.retriever import Retrieve

from hdx.scraper.insecurity_insight.utilities import (
    censor_event_description,
    censor_location,
    create_dataset,
    create_spreadsheet,
    get_countries_from_api_response,
    get_dates_from_api_response,
)

logger = logging.getLogger(__name__)


class InsecurityInsight:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_folder = retriever.temp_dir

    def fetch_api_responses(self) -> dict:
        api_cache = {}
        topics = self._configuration["topics"]
        for topic in topics:
            for topic_type in self._configuration["topic_types"]:
                resource = f"{topic}-{topic_type}"
                logger.info(f"Fetching data for {resource} from API")

                api_url = f"{self._configuration['base_url']}{topic}"
                if topic_type == "overview":
                    api_url = f"{api_url}Overview"
                try:
                    json_response = self._retriever.download_json(api_url)
                except DownloadError:
                    logger.error(f"Failed to download response for {resource}")
                    continue
                censored_location_response = censor_location(["PSE"], json_response)
                censored_response = censor_event_description(censored_location_response)
                api_cache[resource] = censored_response

        logger.info(f"Loaded {len(api_cache)} API responses to cache, expected 18")
        return api_cache

    def fetch_datasets(self, topics: Optional[list] = None) -> dict:
        dataset_cache = {}
        if topics is None:
            topics = self._configuration["topics"]

        # load topic datasets
        n_topic_datasets = 0
        for topic in topics:
            dataset_name = self._configuration["datasets"][topic]["name"]
            dataset = Dataset.read_from_hdx(dataset_name)
            if dataset:
                dataset_cache[topic] = dataset
                n_topic_datasets += 1

        # Load country datasets
        n_countries = 0
        for country, dataset_info in self._configuration["country_datasets"].items():
            if country == "all":
                continue
            dataset_name = dataset_info["name"]
            dataset = Dataset.read_from_hdx(dataset_name)
            if dataset:
                dataset_cache[country] = dataset
                n_countries += 1

        logger.info(f"Loaded {len(dataset_cache)} datasets to cache")
        logger.info(f"Found {n_topic_datasets}, expected 7 topic datasets")
        logger.info(f"Found {n_countries}, expected 25 country datasets")
        return dataset_cache

    def check_api_has_not_changed(
        self, api_cache: dict, topics: Optional[list] = None
    ) -> tuple[bool, list]:
        if topics is None:
            topics = self._configuration["topics"]

        has_changed = False
        changed_list = []
        for topic in topics:
            for topic_type in self._configuration["topic_types"]:
                if "current-year" in topic_type:
                    continue
                resource = f"{topic}-{topic_type}"
                api_response = api_cache.get(resource, {})
                if len(api_response) == 0:
                    logger.error(f"Cannot compare {resource}")
                    continue
                if topic_type == "incidents":
                    resource = f"{topic}"
                samples_response = load_json(
                    join(dirname(__file__), "api-samples", f"{resource}.json")
                )

                sample_keys = samples_response[0].keys()
                if len(api_response) != 0:
                    api_keys = api_response[0].keys()
                else:
                    api_keys = []

                if sample_keys != api_keys:
                    changed_list.append(resource)
                    logger.info(f"**MISMATCH between API and sample for {resource}")
                    logger.info("Keys in API data but not in sample:")
                    logger.info(set(api_keys).difference(set(sample_keys)))
                    logger.info("Keys in sample but not in API data:")
                    logger.info(set(sample_keys).difference(api_keys))
                    has_changed = True

        if len(changed_list) > 0:
            logger.error("Changed API endpoints:")
            for resource in changed_list:
                logger.error(resource)

        assert not has_changed, (
            "!!One or more of the Insecurity Insight endpoints has changed format"
        )
        return has_changed, changed_list

    def decide_which_resources_have_fresh_data(
        self,
        dataset_cache: dict,
        api_cache: dict,
        topics: Optional[list] = None,
        force_refresh: Optional[bool] = False,
    ) -> list[str]:
        if topics is None:
            topics = self._configuration["topics"]

        topics_to_update = []
        for topic in topics:
            dataset = dataset_cache[topic]
            dataset_date = dataset.get_time_period(date_format="%Y-%m-%d")
            dataset_start_date = dataset_date["startdate_str"]
            dataset_end_date = dataset_date["enddate_str"]

            api_resource = api_cache[f"{topic}-incidents"]
            api_start_date, api_end_date = get_dates_from_api_response(api_resource)

            if api_end_date > dataset_end_date:
                topics_to_update.append(topic)
            elif api_start_date < dataset_start_date:
                topics_to_update.append(topic)
            elif force_refresh:
                topics_to_update.append(topic)

        return topics_to_update

    def refresh_spreadsheets_with_fresh_data(
        self,
        topics_to_update: list[str],
        api_cache: dict,
        current_year: int,
        countries: Optional[list] = None,
    ) -> dict:
        file_paths = {}
        if len(topics_to_update) == 0:
            logger.info("No spreadsheets need to be updated")
            return file_paths

        logger.info("Refreshing topic spreadsheets")
        for topic in topics_to_update:
            for topic_type in self._configuration["topic_types"]:
                year_filter = ""
                if topic_type == "incidents-current-year":
                    year_filter = str(current_year)
                file_path = create_spreadsheet(
                    topic=topic,
                    topic_type=topic_type,
                    proper_name=self._configuration["topics"][topic],
                    api_response=api_cache[f"{topic}-{topic_type}"],
                    output_dir=self._retriever.temp_dir,
                    year_filter=year_filter,
                )
                file_paths[f"{topic}-{topic_type}"] = file_path

        logger.info("Refreshing all country spreadsheets")
        if countries is None:
            countries = self._configuration["country_datasets"]
        for country in countries:
            if country == "all":
                continue
            for topic in self._configuration["topics"]:
                file_path = create_spreadsheet(
                    topic=topic,
                    topic_type="incidents",
                    proper_name=self._configuration["topics"][topic],
                    api_response=api_cache[f"{topic}-incidents"],
                    output_dir=self._retriever.temp_dir,
                    country_filter=country,
                )
                file_paths[f"{country}-{topic}-incidents"] = file_path

        return file_paths

    def update_datasets(
        self,
        topics_to_update: list[str],
        api_cache: dict,
        file_paths: dict,
        countries_to_update: Optional[list] = None,
    ) -> list:
        datasets_to_update = []

        if len(topics_to_update) == 0:
            logger.info("No datasets need to be updated")
            return datasets_to_update

        # update topic datasets
        for topic in topics_to_update:
            countries = get_countries_from_api_response(api_cache[f"{topic}-incidents"])
            topic_file_paths = {
                key: value for key, value in file_paths.items() if key.startswith(topic)
            }
            dataset = create_dataset(
                topic=topic,
                dataset_template=self._configuration["datasets"][topic],
                countries=countries,
                file_paths=topic_file_paths,
                api_cache=api_cache,
            )
            datasets_to_update.append(dataset)

        # update all country datasets
        country_datasets = self._configuration["country_datasets"]
        template = self._configuration["country_datasets"]["all"]
        for country, dataset_template in country_datasets.items():
            if country == "all":
                continue
            if countries_to_update is not None and country not in countries_to_update:
                continue
            topics = dataset_template["topics"]
            dataset_template = merge_two_dictionaries(dataset_template, template)
            tags = []
            for topic in topics:
                tag_list = dataset_template["tags"][topic]
                tags.extend(tag_list)
            dataset_template["tags"] = sorted(list(set(tags)))
            country_file_paths = {
                key: value
                for key, value in file_paths.items()
                if key.startswith(country)
            }
            dataset = create_dataset(
                topic="all",
                dataset_template=dataset_template,
                countries=[country],
                file_paths=country_file_paths,
                api_cache=api_cache,
            )
            datasets_to_update.append(dataset)

        return datasets_to_update

    def reorder_resources(self, dataset: Dataset) -> None:
        # Reorder resources so that the datasets from the API come first
        resource_list_names = [x["name"] for x in dataset.get_resources()]

        dataset_name = dataset["name"]
        revised_dataset = Dataset.read_from_hdx(dataset_name)
        resources_check = revised_dataset.get_resources()

        reordered_resource_ids = [
            x["id"] for x in resources_check if x["name"] in resource_list_names
        ]
        reordered_resource_ids.extend(
            [x["id"] for x in resources_check if x["name"] not in resource_list_names]
        )

        revised_dataset.reorder_resources(
            hxl_update=False, resource_ids=reordered_resource_ids
        )
        return
