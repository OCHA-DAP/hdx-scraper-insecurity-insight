#!/usr/bin/env python
# encoding: utf-8

import logging
import os
import sys

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

    # Read API for date range and countries (use the full data rather than the Overview, although the Overview has a longer timescale)
    resource_dataset_name = "insecurity-insight-crsv"

    resource_json = fetch_json_from_api(resource_dataset_name)

    dates = [x["Date"] for x in resource_json]
    start_date = min(dates)
    end_date = max(dates)

    dataset_date = f"[{start_date} TO {end_date}]"
    LOGGER.info(f"Dataset_date: {dataset_date}")

    # Possibly we want to run a counter here to work out the significant countries in the dataset
    countries = {x["Country ISO"].lower() for x in resource_json}

    countries_group = [{"name": x} for x in countries]

    LOGGER.info(f"Data from {len(countries_group)} countries found")

    dataset_template_filepath = os.path.join(
        os.path.dirname(__file__), "new-dataset-templates", "conflict-related-sexual-violence.json"
    )

    dataset = Dataset.load_from_json(dataset_template_filepath)

    # dataset_date - this needs updated according to the data
    dataset["dataset_date"] = dataset_date
    # Need to add groups on the basis of data
    dataset["groups"] = countries_group
    # Does license_url and license_title get generated from license_id

    # What do we do about quickcharts?
    resource_names = ["insecurity-insight-crsv", "insecurity-insight-crsv-overview"]

    resource_list = []

    for resource_name in resource_names:
        attributes = read_attributes(resource_name)
        country_iso = ""
        if resource_name == "insecurity-insight-crsv":
            filename = "2016-2022-conflict-related-sexual-violence-crsv-overview-data.xlsx"
        elif resource_name == "insecurity-insight-crsv-overview":
            filename = "2020-2023-conflict-related-sexual-violence-crsv-incident-data.xlsx"

        resource = Resource(
            {
                "name": filename,
                "description": attributes["description"],
                "format": attributes["file_format"],
            }
        )
        resource.set_file_to_upload(
            os.path.join(
                os.path.dirname(__file__),
                "output-spreadsheets",
                filename,
            )
        )
        resource_list.append(resource)

    dataset.add_update_resources(resource_list)
    print(dataset, flush=True)
    dataset.create_in_hdx()


if __name__ == "__main__":
    DATASET_NAME = "insecurity-insight-crsv-dataset"
    create_datasets_in_hdx(DATASET_NAME)
