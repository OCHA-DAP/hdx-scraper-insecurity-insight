#!/usr/bin/python
"""insecurity insight scraper"""

import logging
import re
from typing import Optional

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.retriever import Retrieve

from hdx.scraper.insecurity_insight.create_datasets import (
    create_datasets_in_hdx,
    get_countries_group_from_api_response,
    get_date_range_from_api_response,
)
from hdx.scraper.insecurity_insight.create_spreadsheets import create_spreadsheet
from hdx.scraper.insecurity_insight.generate_api_transformation_schema import (
    compare_api_to_samples,
)
from hdx.scraper.insecurity_insight.utilities import (
    censor_event_description,
    censor_location,
    list_entities,
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
        dataset_names = None
        if refresh is None or "all" in refresh:
            pass
        else:
            dataset_names = []
            for item in refresh:
                dataset_names.append(f"insecurity-insight-{item}-incidents")
        print(dataset_names, flush=True)
        has_changed, changed_list = compare_api_to_samples(
            api_cache, dataset_names=dataset_names
        )
        logger.info("\nChanged API endpoints:")
        for dataset_name in changed_list:
            logger.info(dataset_name)

        assert not has_changed, (
            "!!One or more of the Insecurity Insight endpoints has changed format"
        )
        return has_changed, changed_list

    def decide_which_resources_have_fresh_data(
        self,
        dataset_cache: dict,
        api_cache: dict,
        refresh: Optional[list] = None,
        dataset_list: Optional[list[str]] = None,
        resource_list: Optional[list[str]] = None,
        topic_list: Optional[list[str]] = None,
    ) -> list[str]:
        """This function returns a list of tuples for datasets that need updating containing:
        (topic, api start date, api end date)
        The api start and end date are used to populate dataset_date later in the process.

        Arguments:
            dataset_cache {dict} -- _description_
            api_cache {dict} -- _description_

        Keyword Arguments:
            refresh {bool} -- _description_ (default: {False})
            dataset_list {Optional[list[str]]} -- _description_ (default: {None})
            resource_list {Optional[list[str]]} -- _description_ (default: {None})
            topic_list {Optional[list[str]]} -- _description_ (default: {None})

        Returns:
            list[str] -- _description_
        """
        print_banner_to_log(logger, "Identify updates")
        if refresh is None:
            refresh = []
        if len(refresh) != 0 and "all" in refresh:
            logger.info("`refresh` is set to `all` all resources will be refreshed")

        # Dates from dataset records
        if dataset_list is None:
            dataset_list = list_entities(type_="dataset")

        dataset_end_date = {}
        dataset_start_date = {}
        for dataset in dataset_list:
            if dataset == self._configuration["country_dataset_basename"]:
                continue
            try:
                start_date, end_date = parse_dates_from_string(
                    dataset_cache[dataset]["dataset_date"]
                )
                dataset_end_date[dataset] = end_date
                dataset_start_date[dataset] = start_date
            except KeyError:
                dataset_end_date[dataset] = ""
                dataset_start_date[dataset] = ""

        # Dates from resources
        if resource_list is None:
            resource_list = list_entities(type_="resource")

        resource_end_date = {}
        resource_start_date = {}
        for resource in resource_list:
            if not resource.endswith("-incidents"):
                continue

            try:
                start_date, end_date = get_date_range_from_api_response(
                    api_cache[resource]
                )
                _, resource_end_date[resource] = parse_dates_from_string(end_date)
                _, resource_start_date[resource] = parse_dates_from_string(start_date)
            except KeyError:
                resource_end_date[resource] = ""
                resource_start_date[resource] = ""
        # Compare
        if topic_list is None:
            topic_list = [
                "crsv",
                "education",
                "explosive",
                "healthcare",
                "protection",
                "aidworkerKIKA",
                "foodsecurity",
            ]

        items_to_update = []
        logger.info(
            f"{'item':<15} {'API Start ':<10} {'API End':<10} "
            f"{'Dataset Start':<15} {'Dataset End':<10} "
        )
        for item in topic_list:
            resource_key = f"insecurity-insight-{item}-incidents"
            dataset_key = f"insecurity-insight-{item}-dataset"
            update_str = ""
            try:
                if resource_end_date[resource_key] > dataset_end_date[dataset_key]:
                    update_str = update_str + "*>"
                    items_to_update.append(
                        (
                            item,
                            resource_start_date[resource_key],
                            resource_end_date[resource_key],
                        )
                    )
                elif (
                    resource_start_date[resource_key] < dataset_start_date[dataset_key]
                ):
                    update_str = update_str + "*<"
                    items_to_update.append(
                        (
                            item,
                            resource_start_date[resource_key],
                            resource_end_date[resource_key],
                        )
                    )
                elif len(refresh) != 0 and "all" in refresh or item in refresh:
                    update_str = update_str + "*"
                    items_to_update.append(
                        (
                            item,
                            resource_start_date[resource_key],
                            resource_end_date[resource_key],
                        )
                    )
            except KeyError:
                pass

            logger.info(
                f"{item:<15} "
                f"{resource_start_date[resource_key]:<10} "
                f"{resource_end_date[resource_key]:<10} "
                f"{dataset_start_date[dataset_key]:<15} "
                f"{dataset_end_date[dataset_key]:<10} "
                f"{update_str}"
            )

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


def parse_dates_from_string(date_str: str) -> tuple:
    matched_strings = re.findall(r"(\d{4}-([0]\d|1[0-2])-([0-2]\d|3[01]))", date_str)
    start_date = ""
    end_date = ""
    if len(matched_strings) == 1:
        end_date = matched_strings[-1][0]
    elif len(matched_strings) == 2:
        start_date = matched_strings[0][0]
        end_date = matched_strings[-1][0]

    return start_date, end_date
