#!/usr/bin/env python
# encoding: utf-8

import datetime
import logging
import os
import re
import time

from pathlib import Path

from hdx.utilities.easy_logging import setup_logging
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.location.country import Country
from hdx_scraper_insecurity_insight.utilities import (
    fetch_json,
    read_attributes,
    list_entities,
    parse_commandline_arguments,
    filter_json_rows,
    pick_date_and_iso_country_fields,
)

setup_logging()
LOGGER = logging.getLogger(__name__)
# Configuration.create(
#     hdx_site="stage", user_agent="hdxds_insecurity_insight", hdx_key=os.getenv("HDX_KEY")
# )

Configuration.create(
    user_agent_config_yaml=os.path.join(os.path.expanduser("~"), ".useragents.yaml"),
    user_agent_lookup="hdx-scraper-insecurity-insight",
)


def marshall_datasets(dataset_name_pattern: str, country_pattern: str):
    if dataset_name_pattern.lower() != "all":
        create_datasets_in_hdx(dataset_name_pattern, country_pattern)
    else:
        dataset_names = list_entities(type_="dataset")

        LOGGER.info(f"Attributes file contains {len(dataset_names)} dataset names")

        for dataset_name in dataset_names:
            create_datasets_in_hdx(dataset_name)


def create_datasets_in_hdx(dataset_name: str, country_filter: str = ""):
    LOGGER.info("*********************************************")
    LOGGER.info("* Insecurity Insight - Create dataset   *")
    LOGGER.info(f"* Invoked at: {datetime.datetime.now().isoformat(): <23}    *")
    LOGGER.info("*********************************************")
    LOGGER.info(f"Dataset name: {dataset_name}")
    LOGGER.info(f"Country filter: {country_filter}")
    t0 = time.time()
    dataset_attributes = read_attributes(dataset_name)

    dataset = create_or_fetch_base_dataset(dataset_name, dataset_attributes)

    # Modify name and title if a country page
    if country_filter is not None and country_filter != "":
        country_name = Country.get_country_name_from_iso3(country_filter)
        dataset["name"] = dataset["name"].replace("country", country_filter.lower())
        dataset["title"] = dataset["title"].replace("country", f"{country_name}({country_filter})")

    LOGGER.info(f"Dataset title: {dataset['title']}")
    resource_names = dataset_attributes["resource"]
    # This is a bit nasty since it reads the API for every resource in a dataset
    dataset_date, countries_group = get_date_and_country_ranges_from_resources(
        resource_names, country_filter=country_filter
    )

    dataset["dataset_date"] = dataset_date
    dataset["groups"] = countries_group

    resource_list = []

    for resource_name in resource_names:
        attributes = read_attributes(resource_name)
        resource_filepath = find_resource_filepath(
            resource_name, attributes, country_filter=country_filter
        )

        if resource_filepath is None:
            continue
        resource = Resource(
            {
                "name": os.path.basename(resource_filepath),
                "description": attributes["description"],
                "format": attributes["file_format"],
            }
        )
        resource.set_file_to_upload(resource_filepath)
        resource_list.append(resource)

    dataset.add_update_resources(resource_list)
    dataset.create_in_hdx()
    LOGGER.info(f"Processing finished at {datetime.datetime.now().isoformat()}")
    LOGGER.info(f"Elapsed time: {time.time() - t0: 0.2f} seconds")


def create_or_fetch_base_dataset(dataset_name, dataset_attributes):
    dataset = Dataset.read_from_hdx(dataset_name)
    if dataset is not None:
        LOGGER.info(f"Dataset already exists in hdx_site: `{Configuration.read().hdx_site}`")
        LOGGER.info("Updating")
    else:
        LOGGER.info(
            f"`{dataset_name}` does not exist in hdx_site: `{Configuration.read().hdx_site}`"
        )
        LOGGER.info(
            f"Using {dataset_attributes['dataset_template']} as a template for a new dataset"
        )
        dataset_template_filepath = os.path.join(
            os.path.dirname(__file__),
            "new-dataset-templates",
            dataset_attributes["dataset_template"],
        )

        dataset = Dataset.load_from_json(dataset_template_filepath)
    return dataset


def find_resource_filepath(resource_name: list[str], attributes: [], country_filter: str = ""):
    # this regex will fail on spreadsheets with an country ISO code in the filename
    # And also single year spreadsheets
    spreadsheet_directory = os.path.join(os.path.dirname(__file__), "output-spreadsheets")
    file_list = Path(spreadsheet_directory)
    files = []

    # Finds year range files
    country_iso = ""
    if (country_filter is not None) and (len(country_filter) != 0):
        country_iso = f"-{country_filter}"
    spreadsheet_regex_range = (
        attributes["filename_template"]
        .replace("{start_year}", "[0-9]{4}")
        .replace("{end_year}", "[0-9]{4}")
        .replace("{country_iso}", country_iso)
    )

    for file_ in file_list.iterdir():
        matching_files = re.search(spreadsheet_regex_range, str(file_))
        if matching_files is not None:
            files.append(matching_files.group())

    # Finds year range files
    if len(files) == 0:
        spreadsheet_regex_single_year = (
            attributes["filename_template"]
            .replace("{start_year}", "[0-9]{4}")
            .replace("-{end_year}", "")
            .replace("{country_iso}", country_filter)
        )
        for file_ in file_list.iterdir():
            matching_files = re.search(spreadsheet_regex_single_year, str(file_))
            if matching_files is not None:
                files.append(matching_files.group())

    filepath = None
    if len(files) != 1:
        LOGGER.info(
            f"{len(files)} spreadsheets matching `{spreadsheet_regex_range}` or "
            f"`{spreadsheet_regex_single_year}` "
            "found in `output-spreadsheets`, 1 was expected"
        )
        # raise FileNotFoundError
    else:
        filename = files[0]
        LOGGER.info(f"`{filename}` found for resource `{resource_name}`")
        filepath = os.path.join(spreadsheet_directory, filename)
    return filepath


def get_date_and_country_ranges_from_resources(
    resource_names: list[str], country_filter: str = "", use_sample=False
):
    dates = []
    countries = []
    LOGGER.info(f"Scanning {len(resource_names)} resources for date and country ranges")
    for resource_dataset_name in resource_names:
        LOGGER.info(f"Processing {resource_dataset_name}")
        resource_json = fetch_json(resource_dataset_name, use_sample=use_sample)
        filtered_json = filter_json_rows(country_filter, "", resource_json)
        date_field, iso_country_field = pick_date_and_iso_country_fields(filtered_json[0])

        for row in filtered_json:
            dates.append(row[date_field])
            if row[iso_country_field] != "":
                countries.append(row[iso_country_field].lower())

    start_date = min(dates).replace("Z", "")
    end_date = max(dates).replace("Z", "")

    dataset_date = f"[{start_date} TO {end_date}]"
    LOGGER.info(f"Dataset_date: {dataset_date}")

    # Possibly we want to run a counter here to work out the significant countries in the dataset
    countries_group = [{"name": x} for x in set(countries)]
    LOGGER.info(f"Data from {len(countries_group)} countries found")
    return dataset_date, countries_group


if __name__ == "__main__":
    DATASET_NAME, COUNTRY_CODE = parse_commandline_arguments()
    marshall_datasets(DATASET_NAME, COUNTRY_CODE)
