#!/usr/bin/python
"""insecurity insight scraper"""

import logging
from os.path import dirname, join
from typing import Optional

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.loader import load_json
from hdx.utilities.retriever import Retrieve

from hdx.scraper.insecurity_insight.create_datasets import (
    create_datasets_in_hdx,
    get_countries_group_from_api_response,
)
from hdx.scraper.insecurity_insight.create_spreadsheets import create_spreadsheet
from hdx.scraper.insecurity_insight.utilities import (
    censor_event_description,
    censor_location,
    list_entities,
    pick_date_and_iso_country_fields,
    print_banner_to_log,
    read_attributes,
    read_countries,
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

    def fetch_api_responses(self, refresh: Optional[list] = None) -> dict:
        api_cache = {}
        if refresh is None:
            refresh = self._configuration["topics"]
        for topic in refresh:
            for topic_type in ["incidents", "incidents-current-year", "overview"]:
                resource = f"{topic}-{topic_type}"
                logger.info(f"Fetching data for {resource} from API")

                api_url = f"{self._configuration['base_url']}{topic}"
                if topic_type == "overview":
                    api_url = f"{api_url}Overview"
                json_response = self._retriever.download_json(api_url)
                censored_location_response = censor_location(["PSE"], json_response)
                censored_response = censor_event_description(censored_location_response)
                api_cache[resource] = censored_response

        logger.info(f"Loaded {len(api_cache)} API responses to cache, expected 18")
        return api_cache

    def fetch_datasets(self, refresh: Optional[list] = None) -> dict:
        dataset_cache = {}
        if refresh is None:
            refresh = self._configuration["topics"]

        # load topic datasets
        n_topic_datasets = 0
        for topic in refresh:
            dataset_name = self._configuration["datasets"][topic]["name"]
            dataset = Dataset.read_from_hdx(dataset_name)
            if dataset:
                dataset_cache[topic] = dataset
                n_topic_datasets += 1

        # Load country datasets
        n_countries = 0
        for country, dataset_name in self._configuration["country_datasets"].items():
            dataset = Dataset.read_from_hdx(dataset_name)
            if dataset:
                dataset_cache[country] = dataset
                n_countries += 1

        logger.info(f"Loaded {len(dataset_cache)} datasets to cache")
        logger.info(f"Found {n_topic_datasets}, expected 7 topic datasets")
        logger.info(f"Found {n_countries}, expected 25 country datasets")
        return dataset_cache

    def check_api_has_not_changed(
        self, api_cache: dict, refresh: Optional[list] = None
    ) -> tuple[bool, list]:
        if refresh is None:
            refresh = self._configuration["topics"]

        has_changed = False
        changed_list = []
        for topic in refresh:
            for topic_type in ["incidents", "overview"]:
                resource = f"{topic}-{topic_type}"
                api_response = api_cache[resource]
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
        refresh: Optional[list] = None,
        force_refresh: Optional[bool] = False,
    ) -> list[str]:
        if refresh is None:
            refresh = self._configuration["topics"]

        items_to_update = []
        for topic in refresh:
            dataset = dataset_cache[topic]
            dataset_date = dataset.get_time_period(date_format="%Y-%m-%d")
            dataset_start_date = dataset_date["startdate_str"]
            dataset_end_date = dataset_date["enddate_str"]

            api_resource = api_cache[f"{topic}-incidents"]
            dates = []
            date_field, _ = pick_date_and_iso_country_fields(api_resource[0])
            for row in api_resource:
                dates.append(row[date_field])
            api_start_date = None
            api_end_date = None
            if len(dates) != 0:
                api_start_date = min(dates).replace("Z", "")[0:10]
                api_end_date = max(dates).replace("Z", "")[0:10]

            if api_end_date > dataset_end_date:
                items_to_update.append(topic)
            elif api_start_date < dataset_start_date:
                items_to_update.append(topic)
            elif force_refresh:
                items_to_update.append(topic)

        return items_to_update

    def refresh_spreadsheets_with_fresh_data(
        self, items_to_update: list[str], api_cache: dict
    ):
        print_banner_to_log(logger, "Refresh spreadsheets")
        if len(items_to_update) == 0:
            logger.info("No spreadsheets need to be updated")
            return

        resources = list_entities(type_="resource")

        logger.info(
            f"Refreshing topic spreadsheets for {','.join([x[0] for x in items_to_update])}"
        )
        for item in items_to_update:
            for resource in resources:
                if item[0] in resource:
                    # This is where we would create a year spreadsheet
                    try:
                        status = create_spreadsheet(
                            resource, api_response=api_cache[resource]
                        )
                    except KeyError:
                        pass

                    logger.info(status)

        logger.info("Refreshing all country spreadsheets")
        # logger.info("**ONLY DOING ONE COUNTRY FOR TEST**")
        countries = read_countries()
        country_attributes = read_attributes(
            self._configuration["country_dataset_basename"]
        )
        resource_names = country_attributes["resource"]
        for country in countries:
            logger.info(f"Processing for {country}")
            for resource in resource_names:
                try:
                    status = create_spreadsheet(
                        resource,
                        country_filter=country,
                        api_response=api_cache[resource],
                    )
                except KeyError:
                    pass
                logger.info(status)

            # break  # just do one spreadsheet for testing

    def update_datasets_whose_resources_have_changed(
        self,
        items_to_update: list[str],
        api_cache: dict,
        dataset_cache: dict,
        dry_run: bool = False,
        use_legacy: bool = True,
        hdx_site: str = None,
    ) -> list[list]:
        print_banner_to_log(logger, "Update datasets")
        if len(items_to_update) == 0:
            logger.info("No datasets need to be updated")
            return []

        missing_report = []
        datasets = list_entities(type_="dataset")
        n_missing_resources = 0
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
        # logger.info("**ONLY DOING ONE COUNTRY FOR TEST**")
        countries = read_countries()
        # Make a default dataset_date in case a country dataset has no data
        start_date = min([x[1] for x in items_to_update])
        end_date = max([x[2] for x in items_to_update])
        dataset_date = f"[{start_date} TO {end_date}]"
        for country in countries:
            countries_group = [{"name": country.lower()}]

            # start_date, end_date = get_date_range_from_api_response(
            #     api_cache[f"insecurity-insight-{item[0]}-incidents"], country_filter=country
            # )
            # if start_date is not None and end_date is not None:
            #     dataset_date = f"[{start_date} TO {end_date}]"
            dataset, n_missing_resources = create_datasets_in_hdx(
                self._configuration["country_dataset_basename"],
                country_filter=country,
                dataset_cache=dataset_cache,
                dataset_date=dataset_date,
                countries_group=countries_group,
                dry_run=dry_run,
                hdx_site=hdx_site,
            )
            if n_missing_resources != 0:
                missing_report.append([dataset["name"], n_missing_resources])

        return missing_report
