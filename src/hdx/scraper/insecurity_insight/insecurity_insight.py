#!/usr/bin/python
"""insecurity insight scraper"""

import logging
from typing import Dict, List, Optional

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

    def fetch_api_responses(self) -> Dict:
        api_cache = {}
        for topic in self._configuration["topics"] | self._configuration["subtopics"]:
            for topic_type in self._configuration["topic_types"]:
                resource = f"{topic}-{topic_type}"
                logger.info(f"Fetching data for {resource} from API")

                api_url = f"{self._configuration['base_url']}{topic}"
                if topic_type == "overview":
                    api_url = f"{api_url}Overview"
                try:
                    json_response = self._retriever.download_json(api_url)
                except (DownloadError, FileNotFoundError):
                    logger.error(f"Failed to download response for {resource}")
                    continue
                censored_location_response = censor_location(["PSE"], json_response)
                censored_response = censor_event_description(censored_location_response)
                api_cache[resource] = censored_response

        logger.info(f"Loaded {len(api_cache)} API responses to cache")
        return api_cache

    def check_for_updates(
        self,
        api_cache: Dict,
        force_refresh: Optional[bool] = False,
    ) -> List[str]:
        topics_to_update = []
        for topic in self._configuration["topics"]:
            if force_refresh:
                topics_to_update.append(topic)
                continue
            dataset_name = self._configuration["datasets"][topic]["name"]
            dataset = Dataset.read_from_hdx(dataset_name)
            if not dataset:
                topics_to_update.append(topic)
                continue
            dataset_date = dataset.get_time_period(date_format="%Y-%m-%d")
            dataset_start_date = dataset_date["startdate_str"]
            dataset_end_date = dataset_date["enddate_str"]

            api_resource = api_cache.get(f"{topic}-incidents", [])
            api_start_dates = []
            api_end_dates = []
            if len(api_resource) > 0:
                api_start_date, api_end_date = get_dates_from_api_response(api_resource)
                api_start_dates.append(api_start_date)
                api_end_dates.append(api_end_date)
            if topic == "sv":
                for subtopic in self._configuration["subtopics"]:
                    sub_start_date, sub_end_date = get_dates_from_api_response(
                        api_cache[f"{subtopic}-incidents"]
                    )
                    api_start_dates.append(sub_start_date)
                    api_end_dates.append(sub_end_date)
            api_start_date = min(api_start_dates)
            api_end_date = max(api_end_dates)

            if api_end_date > dataset_end_date:
                topics_to_update.append(topic)
            elif api_start_date < dataset_start_date:
                topics_to_update.append(topic)

        return topics_to_update

    def refresh_spreadsheets(
        self,
        api_cache: Dict,
        current_year: int,
        topics_to_update: Optional[List] = None,
        countries: Optional[List] = None,
    ) -> Dict:
        file_paths = {}
        if len(topics_to_update) == 0:
            logger.info("No spreadsheets need to be updated")
            return file_paths

        if "sv" in topics_to_update:
            topics_to_update = topics_to_update + list(
                self._configuration["subtopics"].keys()
            )
        logger.info("Refreshing topic spreadsheets")
        for topic in topics_to_update:
            for topic_type in self._configuration["topic_types"]:
                year_filter = ""
                if topic_type == "incidents-current-year":
                    year_filter = str(current_year)
                api_response = api_cache.get(f"{topic}-{topic_type}", [])
                if len(api_response) == 0:
                    logger.info(f"No API response for {topic}-{topic_type}")
                    continue
                proper_name = self._configuration["topics"].get(topic)
                if not proper_name:
                    proper_name = self._configuration["subtopics"].get(topic)
                file_path = create_spreadsheet(
                    topic=topic,
                    topic_type=topic_type,
                    proper_name=proper_name,
                    api_response=api_response,
                    output_dir=self._retriever.temp_dir,
                    year_filter=year_filter,
                )
                if file_path is not None:
                    file_paths[f"{topic}-{topic_type}"] = file_path

        logger.info("Refreshing all country spreadsheets")
        if countries is None:
            countries = self._configuration["country_datasets"]
        for country in countries:
            if country == "all":
                continue
            for topic in (
                self._configuration["topics"] | self._configuration["subtopics"]
            ):
                api_response = api_cache.get(f"{topic}-{topic_type}", [])
                if len(api_response) == 0:
                    logger.info(
                        f"No API response for {topic}-{topic_type} for {country}"
                    )
                    continue
                proper_name = self._configuration["topics"].get(topic)
                if not proper_name:
                    proper_name = self._configuration["subtopics"].get(topic)
                file_path = create_spreadsheet(
                    topic=topic,
                    topic_type="incidents",
                    proper_name=proper_name,
                    api_response=api_response,
                    output_dir=self._retriever.temp_dir,
                    country_filter=country,
                )
                if file_path is not None:
                    file_paths[f"{country}-{topic}-incidents"] = file_path

        return file_paths

    def update_datasets(
        self,
        api_cache: Dict,
        file_paths: Dict,
        topics_to_update: Optional[List] = None,
        countries_to_update: Optional[List] = None,
    ) -> List[Dataset]:
        datasets_to_update = []

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

    def remove_old_resources(self, dataset: Dataset, current_year: int) -> None:
        # Remove resources from old API uploads
        resource_list_names = [x["name"] for x in dataset.get_resources()]
        dataset_name = dataset["name"]
        revised_dataset = Dataset.read_from_hdx(dataset_name)
        resources_check = revised_dataset.get_resources()
        resources_to_delete = []
        for resource in resources_check:
            # TODO: update logic to detect resources that were from the API but are out of date
            pass
        for resource in resources_to_delete:
            resource.delete_from_hdx()
        return

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
