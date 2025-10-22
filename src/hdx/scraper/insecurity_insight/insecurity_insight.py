#!/usr/bin/python
"""insecurity insight scraper"""

import logging
from typing import List, Optional

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.base_downloader import DownloadError
from hdx.utilities.dictandlist import merge_two_dictionaries
from hdx.utilities.retriever import Retrieve

from hdx.scraper.insecurity_insight.utilities import (
    censor_event_description,
    censor_location,
    create_dataset,
    create_spreadsheet,
    get_countries_from_api_response,
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

    def refresh_spreadsheets_with_fresh_data(
        self,
        api_cache: dict,
        current_year: int,
        topics_to_update: Optional[List] = None,
        countries: Optional[List] = None,
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
        api_cache: dict,
        file_paths: dict,
        topics_to_update: Optional[List] = None,
        countries_to_update: Optional[List] = None,
    ) -> List[Dataset]:
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
