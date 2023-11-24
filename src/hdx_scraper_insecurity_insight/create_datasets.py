#!/usr/bin/env python
# encoding: utf-8

import logging
import os
import re

from pathlib import Path

from hdx.utilities.easy_logging import setup_logging
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource

from hdx_scraper_insecurity_insight.utilities import fetch_json_from_api, read_attributes

setup_logging()
LOGGER = logging.getLogger(__name__)
Configuration.create(hdx_site="stage", user_agent="hdxds_insecurity_insight")


def create_datasets_in_hdx(dataset_name):
    LOGGER.info(f"Creating/updating `{dataset_name}`")
    dataset_attributes = read_attributes(dataset_name)

    # Check for existence of dataset
    existing_dataset = Dataset.read_from_hdx(dataset_name)

    # if newer then update else exit
    if existing_dataset is not None:
        LOGGER.info(f"Dataset already exists in hdx_site: `{Configuration.read().hdx_site}`")
    else:
        LOGGER.info(
            f"`{dataset_name}` does not exist in hdx_site: `{Configuration.read().hdx_site}`"
        )

    dates = []
    countries = []
    resource_names = dataset_attributes["resource"]
    for resource_dataset_name in resource_names:
        resource_json = fetch_json_from_api(resource_dataset_name)
        date_field = "Date"
        if date_field not in resource_json[0].keys():
            date_field = "Year"
        for row in resource_json:
            dates.append(row[date_field])
            countries.append(row["Country ISO"].lower())

    start_date = min(dates)
    end_date = max(dates)

    dataset_date = f"[{start_date} TO {end_date}]"
    LOGGER.info(f"Dataset_date: {dataset_date}")

    # Possibly we want to run a counter here to work out the significant countries in the dataset
    countries_group = [{"name": x} for x in countries]
    LOGGER.info(f"Data from {len(countries_group)} countries found")

    dataset_template_filepath = os.path.join(
        os.path.dirname(__file__), "new-dataset-templates", dataset_attributes["dataset_template"]
    )

    dataset = Dataset.load_from_json(dataset_template_filepath)

    dataset["dataset_date"] = dataset_date
    dataset["groups"] = countries_group

    # What do we do about quickcharts?
    resource_list = []

    spreadsheet_directory = os.path.join(os.path.dirname(__file__), "output-spreadsheets")
    for resource_name in resource_names:
        attributes = read_attributes(resource_name)
        # this regex will fail on spreadsheets with an country ISO code in the filename
        # And also single year spreadsheets
        spreadsheet_regex = (
            attributes["filename_template"]
            .replace("{start_year}", "[0-9]{4}")
            .replace("{end_year}", "[0-9]{4}")
            .replace("{country_iso}", "")
        )

        file_list = Path(spreadsheet_directory)
        files = []
        for file_ in file_list.iterdir():
            matching_files = re.search(spreadsheet_regex, str(file_))
            if matching_files is not None:
                files.append(matching_files.group())

        if len(files) == 0:
            LOGGER.error(
                f"No spreadsheets matching {spreadsheet_regex} found in `output-spreadsheets`"
            )
            raise FileNotFoundError
        elif len(files) == 1:
            filename = files[0]
            LOGGER.info(f"`{filename}` found for resource `{resource_name}`")
        else:
            LOGGER.error(
                f"Multiple (n={len(files)}) spreadsheets matching {spreadsheet_regex}"
                " found in `output-spreadsheets`"
            )
            raise FileNotFoundError

        resource = Resource(
            {
                "name": filename,
                "description": attributes["description"],
                "format": attributes["file_format"],
            }
        )
        resource.set_file_to_upload(
            os.path.join(
                spreadsheet_directory,
                filename,
            )
        )
        resource_list.append(resource)

    dataset.add_update_resources(resource_list)
    # print(dataset, flush=True)
    # dataset.create_in_hdx()


if __name__ == "__main__":
    DATASET_NAME = "insecurity-insight-crsv-dataset"
    create_datasets_in_hdx(DATASET_NAME)
