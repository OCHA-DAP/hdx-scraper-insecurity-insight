#!/usr/bin/python
"""insecurity insight scraper"""

import logging

from hdx.api.configuration import Configuration
from hdx.utilities.base_downloader import DownloadError
from hdx.utilities.retriever import Retrieve

from hdx.scraper.insecurity_insight.censoring import (
    censor_event_description,
    censor_location,
)
from hdx.scraper.insecurity_insight.utilities import (
    create_spreadsheet,
    pick_date_and_iso_country_fields,
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
        self._api_cache = {}
        self._file_paths = {}

    def fetch_api_responses(self) -> dict:
        def add_cache(topic, topic_type, api_url):
            resource = f"{topic}-{topic_type}"
            logger.info(f"Fetching data for {resource} from API")

            if topic_type == "overview":
                api_url = f"{api_url}Overview"
            try:
                json_response = self._retriever.download_json(api_url)
            except DownloadError:
                logger.error(f"Failed to download response for {resource}")
                return

            censored_location_response = censor_location(["PSE"], json_response)
            censored_response = censor_event_description(censored_location_response)
            self._api_cache[resource] = censored_response

        for topic_type in self._configuration["topic_types"]:
            for maintopic, value in self._configuration["topics"].items():
                if isinstance(value, str) or topic_type == "overview":
                    api_url = f"{self._configuration['base_url']}{maintopic}"
                    add_cache(maintopic, topic_type, api_url)
                    continue
                for topic in value:
                    if topic == "overview":
                        continue
                    api_url = f"{self._configuration['base_url']}{topic}"
                    add_cache(topic, topic_type, api_url)

        logger.info(
            f"Loaded {len(self._api_cache)} API responses to cache, expected 23"
        )
        return self._api_cache

    def filter_json_rows(
        self, topic: str, topic_type: str, country_filter: str, year_filter: str
    ) -> list[dict]:
        filtered_rows = []

        api_response = self._api_cache[f"{topic}-{topic_type}"]
        date_field, iso_country_field = pick_date_and_iso_country_fields(
            api_response[0]
        )

        for api_row in api_response:
            if (
                country_filter is not None
                and len(country_filter) != 0
                and api_row[iso_country_field] != country_filter
            ):
                continue
            if (
                year_filter is not None
                and len(year_filter) != 0
                and api_row[date_field][0:4] != year_filter
            ):
                continue
            filtered_rows.append(api_row)

        return filtered_rows

    def create_spreadsheet(
        self,
        topic: str,
        topic_type: str,
        proper_name: str,
        year_filter: str = "",
        country_filter: str | None = None,
    ):
        filtered_rows = self.filter_json_rows(
            topic, topic_type, country_filter, year_filter
        )
        if len(filtered_rows) == 0:
            logger.info(
                f"API response for `{topic}-{topic_type}` with country_filter {country_filter} contained no data"
            )
            file_path = None
        else:
            file_path = create_spreadsheet(
                filtered_rows=filtered_rows,
                topic_type=topic_type,
                proper_name=proper_name,
                output_dir=self._retriever.temp_dir,
                country_filter=country_filter,
            )
        if country_filter:
            self._file_paths[f"{country_filter}-{topic}-{topic_type}"] = file_path
        else:
            self._file_paths[f"{topic}-{topic_type}"] = file_path

    def refresh_spreadsheets_with_fresh_data(
        self,
        current_year: int,
        countries: list | None = None,
    ) -> dict:
        topics_to_update = self._configuration["topics"]
        logger.info("Refreshing topic spreadsheets")
        for topic_type in self._configuration["topic_types"]:
            year_filter = ""
            if topic_type == "incidents-current-year":
                year_filter = str(current_year)
            for maintopic, value in topics_to_update.items():
                if isinstance(value, str):
                    self.create_spreadsheet(
                        maintopic, topic_type, value, year_filter=year_filter
                    )
                    continue

                for topic, proper_name in value.items():
                    if topic == "overview":
                        if topic_type != "overview":
                            continue
                        self.create_spreadsheet(
                            maintopic, topic_type, proper_name, year_filter=year_filter
                        )
                        continue
                    if topic_type == "overview":
                        continue
                    self.create_spreadsheet(
                        topic, topic_type, proper_name, year_filter=year_filter
                    )

        logger.info("Refreshing all country spreadsheets")
        if countries is None:
            countries = self._configuration["country_datasets"]
        for country in countries:
            if country == "all":
                continue
            for maintopic, value in topics_to_update.items():
                if isinstance(value, str):
                    self.create_spreadsheet(
                        maintopic, "incidents", value, country_filter=country
                    )
                    continue

                for topic, proper_name in value.items():
                    if topic == "overview":
                        continue
                    self.create_spreadsheet(
                        topic, "incidents", proper_name, country_filter=country
                    )
        return self._file_paths
