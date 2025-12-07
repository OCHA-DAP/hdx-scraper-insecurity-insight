from datetime import datetime
from os.path import basename
from typing import Tuple

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.utilities.dateparse import parse_date, default_date, default_enddate, \
    parse_date_range
from hdx.utilities.dictandlist import dict_of_dicts_add, merge_two_dictionaries

from hdx.scraper.insecurity_insight.utilities import (
    get_countries_from_api_response,
    get_dates_from_api_response,
)


class DatasetGenerator:
    def __init__(self, configuration: Configuration, api_cache: dict, file_paths: dict):
        self._configuration = configuration
        self._api_cache = api_cache
        self._file_paths = file_paths

    def get_start_end_dates(
        self,
        file_type: str,
        countries: list,
    ) -> Tuple[datetime | None, datetime | None]:
        start_date_str, end_date_str = get_dates_from_api_response(
            self._api_cache[file_type], countries
        )
        if not start_date_str:
            return None, None
        if len(start_date_str) == 4:
            start_date = parse_date(f"{start_date_str}-01-01")
            end_date = parse_date(f"{end_date_str}-12-31")
        else:
            start_date = parse_date(start_date_str)
            end_date = parse_date(end_date_str)
        return start_date, end_date

    @staticmethod
    def create_dataset(
        dataset_template: dict,
        countries: list,
        resources_info: dict,
    ) -> Dataset:
        dataset = Dataset(
            {
                "name": dataset_template["name"],
                "title": dataset_template["title"],
                "caveats": dataset_template["caveats"],
                "notes": dataset_template["notes"],
                "license_id": dataset_template["license_id"],
                "methodology": "Other",
                "methodology_other": dataset_template["methodology_other"],
            }
        )
        dataset.add_tags(dataset_template["tags"])
        if "xkx" in countries:
            countries.remove("xkx")
            dataset.add_other_location("xkx")
        dataset.add_country_locations(countries)

        resource_list = []
        min_start_date = default_enddate
        max_end_date = default_date
        for file_type, metadata in resources_info.items():
            file_path, resource_description, start_date, end_date = metadata
            if start_date < min_start_date:
                min_start_date = start_date
            if end_date > max_end_date:
                max_end_date = end_date
            start_date_str = start_date.strftime("%d %B %Y")
            end_date_str = end_date.strftime("%d %B %Y")
            resource_name = basename(file_path)
            resource_description = resource_description.format(
                start_date=start_date_str,
                end_date=end_date_str,
            )
            resource = Resource(
                {
                    "name": resource_name,
                    "description": resource_description,
                }
            )
            resource.set_format("xlsx")
            resource.set_file_to_upload(file_path)
            resource_list.append(resource)

        dataset.set_time_period(min_start_date, max_end_date)
        dataset.add_update_resources(resource_list)
        return dataset

    def get_topic_datasets(self):
        datasets_to_update = []
        # update topic datasets
        def update_topic_resource_info(dataset_template, topic_resources_info, topic, all_countries):
            for file_type, file_path in self._file_paths.items():
                if not file_type.startswith(topic) or not file_path:
                    continue
                topic_type = "-".join(file_type.split("-")[1:])
                resource_descriptions = dataset_template["resource_descriptions"]
                if topic_type in resource_descriptions:
                    resource_description = resource_descriptions[topic_type]
                else:
                    resource_description = resource_descriptions[topic][topic_type]

                countries = get_countries_from_api_response(self._api_cache[file_type])
                start_date, end_date = self.get_start_end_dates(
                    file_type, sorted(countries)
                )
                if not start_date:
                    continue
                all_countries.update(countries)
                topic_resources_info[file_type] = file_path, resource_description, start_date, end_date

        for maintopic, value in self._configuration["topics"].items():
            dataset_template = self._configuration["datasets"][maintopic]
            topic_resources_info = {}
            all_countries = set()
            if isinstance(value, str):
                update_topic_resource_info(dataset_template, topic_resources_info, maintopic, all_countries)
            else:
                for subtopic in value:
                    if subtopic == "overview":
                        update_topic_resource_info(dataset_template, topic_resources_info, maintopic, all_countries)
                        continue
                    update_topic_resource_info(dataset_template, topic_resources_info, subtopic, all_countries)

            dataset = self.create_dataset(
                dataset_template=self._configuration["datasets"][maintopic],
                countries=sorted(all_countries),
                resources_info=topic_resources_info,
            )
            datasets_to_update.append(dataset)
        return datasets_to_update

    def get_country_datasets(self, countries_to_update: list or None = None):
        datasets_to_update = []

        # update all country datasets
        country_datasets = self._configuration["country_datasets"]
        template = self._configuration["country_datasets"]["all"]
        for country, dataset_template in country_datasets.items():
            if country == "all":
                continue
            if countries_to_update is not None and country not in countries_to_update:
                continue
            dataset_template = merge_two_dictionaries(dataset_template, template)
            tags = set()
            country_resources_info = {}
            for file_type, file_path in self._file_paths.items():
                if not file_type.startswith(country) or not file_path:
                    continue
                _, topic, topic_type = file_type.split("-")
                if topic not in dataset_template["topics"]:
                    continue
                tag_list = dataset_template["tags"][topic]
                tags.update(tag_list)
                resource_description = dataset_template["resource_descriptions"][topic]
                start_date, end_date = self.get_start_end_dates(
                    f"{topic}-{topic_type}", [country.lower()]
                )
                if not start_date:
                    continue
                country_resources_info[file_type] = file_path, resource_description, start_date, end_date

            dataset_template["tags"] = sorted(tags)
            dataset = self.create_dataset(
                dataset_template=dataset_template,
                countries=[country],
                resources_info=country_resources_info,
            )
            datasets_to_update.append(dataset)
        return datasets_to_update


    def get_datasets(
        self,
        countries_to_update: list or None = None,
    ) -> list[Dataset]:
        topic_datasets_to_update = self.get_topic_datasets()
        country_datasets_to_update = self.get_country_datasets(countries_to_update)
        return topic_datasets_to_update + country_datasets_to_update

    @staticmethod
    def reorder_resources(dataset: Dataset) -> None:
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
