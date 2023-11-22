#!/usr/bin/env python
# encoding: utf-8

import os
import sys

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.data.vocabulary import Vocabulary


# required_fields:
#     - name
#     - private
#     - title
#     - notes
#     - dataset_source
#     - owner_org
#     - maintainer
#     - dataset_date
#     - data_update_frequency
#     - groups
#     - license_id
#     - methodology
#     - tags


def create_datasets_in_hdx():
    Configuration.create(hdx_site="stage", user_agent="hdxds_insecurity_insight")
    dataset = Dataset(
        {
            "name": "test-dataset-ian-hopkinson",
            "title": "Test dataset Ian Hopkinson",
            "private": False,
            "notes": "no field notes",
            "dataset_source": "Insecurity Insight",
            "dataset_date": "[2020-01-01T00:00:00 TO 2023-10-17T23:59:59]",
            "data_update_frequency": "30",  #
            "groups": [{"name": "afg"}, {"name": "world"}],
            "license_id": "cc-by-sa",
            "methodology": "Other",
            "methodology_other": "I wrote it myself",
        }
    )

    # Maintainer UID was found by man
    dataset.set_maintainer("972627a5-4f23-4922-8892-371ece6531b6")  # it me
    dataset.set_organization("hdx")

    approved_tags = Vocabulary.approved_tags()

    print("\nApproved tags", flush=True)
    print(approved_tags, flush=True)

    dataset.add_tags(["hxl", "affected area"])

    resource = Resource(
        {"name": "Resource1", "description": "description of resource1", "format": "CSV"}
    )
    resource.set_file_to_upload(
        os.path.join(
            os.path.dirname(__file__),
            "metadata",
            "schema.csv",
        )
    )
    dataset.add_update_resource(resource)
    print(dataset, flush=True)
    dataset.create_in_hdx()


if __name__ == "__main__":
    create_datasets_in_hdx()
