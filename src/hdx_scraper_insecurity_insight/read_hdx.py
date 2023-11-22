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


def main():
    organization = Organization.read_from_hdx("insecurity-insight")
    # for k, v in organization.items():
    #     print(f"{k}: {v}", flush=True)
    # print(organization, flush=True)

    datasets = organization.get_datasets()

    # for k, v in datasets[0].items():
    #     print(f"{k}: {v}", flush=True)

    n_datasets = len(datasets)
    n_resources = 0
    summary = []
    resources_dict = {}
    for dataset in datasets:
        latest_date = dataset["dataset_date"][24:34]
        # print(
        #     f"{dataset['name']}:{latest_date}({dataset['num_resources']})",
        #     flush=True,
        # )
        resources = Dataset.get_resources(dataset)
        # print(resources, flush=True)
        for resource in resources:
            # print(f"\t{resource['name']}", flush=True)
            if dataset["name"] not in resources_dict:
                resources_dict[dataset["name"]] = [resource["name"]]
            else:
                resources_dict[dataset["name"]].append(resource["name"])
            # for k, v in resource.items():
            #     print(f"\t{k}: {v}", flush=True)
            # break
        n_resources += dataset["num_resources"]
        summary.append([dataset["title"], dataset["name"], latest_date, dataset["num_resources"]])
        dataset.save_to_json(
            os.path.join(
                os.path.dirname(__file__), "legacy-dataset-json", f"{dataset['name']}.json"
            )
        )

    summary.sort(key=lambda x: x[2], reverse=True)

    for entry in summary:
        print(f"\n{entry[0]}", flush=True)
        print(f"{entry[1]}:{entry[2]}({entry[3]})", flush=True)
        for resource in resources_dict[entry[1]]:
            print(f"\t{resource}", flush=True)

    logger.info(f"Number of datasets: {n_datasets}")
    logger.info(f"Number of resources: {n_resources}")

    # dataset = Dataset.read_from_hdx("novel-coronavirus-2019-ncov-cases")
    # for method in dir(dataset):
    #     print(method, flush=True)

    # for k, v in dataset.items():
    #     print(f"{k}: {v}", flush=True)


if __name__ == "__main__":
    main()
