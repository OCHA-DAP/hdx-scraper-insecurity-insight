#!/usr/bin/env python
# encoding: utf-8

"""
This code is for exploring what data from Insecurity Insight is available in HDX
Ian Hopkinson 2023-11-18
"""
import os
import logging

from hdx.utilities.easy_logging import setup_logging
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.organization import Organization

setup_logging()
logger = logging.getLogger(__name__)

Configuration.create(hdx_site="prod", user_agent="hdxds_insecurity_insight", hdx_read_only=True)


def create_insecurity_insight_resource_list():
    organization = Organization.read_from_hdx("insecurity-insight")
    datasets = organization.get_datasets()

    n_datasets = len(datasets)
    n_resources = 0
    summary = []
    resources_dict = {}
    for dataset in datasets:
        latest_date = dataset["dataset_date"][24:34]
        resources = Dataset.get_resources(dataset)
        for resource in resources:
            if dataset["name"] not in resources_dict:
                resources_dict[dataset["name"]] = [resource["name"]]
            else:
                resources_dict[dataset["name"]].append(resource["name"])
        n_resources += dataset["num_resources"]
        summary.append([dataset["title"], dataset["name"], latest_date, dataset["num_resources"]])
        # dataset.save_to_json(
        #     os.path.join(
        #         os.path.dirname(__file__), "legacy-dataset-json", f"{dataset['name']}.json"
        #     )
        # )

    summary.sort(key=lambda x: x[2], reverse=True)

    for entry in summary:
        print(f"\n{entry[0]}", flush=True)
        print(f"{entry[1]}:{entry[2]}({entry[3]})", flush=True)
        for resource in resources_dict[entry[1]]:
            print(f"\t{resource}", flush=True)

    logger.info(f"Number of datasets: {n_datasets}")
    logger.info(f"Number of resources: {n_resources}")

    return n_datasets, n_resources


if __name__ == "__main__":
    n_datasets, n_resources = create_insecurity_insight_resource_list()
