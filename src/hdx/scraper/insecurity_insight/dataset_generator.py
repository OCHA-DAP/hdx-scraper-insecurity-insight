import re
from datetime import datetime
from os.path import basename

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.utilities.dateparse import (
    default_date,
    default_enddate,
    parse_date,
)
from hdx.utilities.dictandlist import merge_two_dictionaries

from hdx.scraper.insecurity_insight.utilities import (
    get_countries_from_api_response,
    get_dates_from_api_response,
)


class DatasetGenerator:
    def __init__(
        self,
        configuration: Configuration,
        api_cache: dict,
        file_paths: dict,
        today: datetime,
    ):
        self._configuration = configuration
        self._api_cache = api_cache
        self._file_paths = file_paths
        self._today = today

    def get_start_end_dates(
        self,
        file_type: str,
        countries: list,
    ) -> tuple[datetime | None, datetime | None]:
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

    def create_dataset(
        self,
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
            if end_date > self._today:
                end_date = self._today
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

        if max_end_date > self._today:
            max_end_date = self._today
        dataset.set_time_period(min_start_date, max_end_date)
        dataset.add_update_resources(resource_list)
        return dataset

    def get_topic_datasets(self, countries_to_include: list or None = None):
        datasets_to_update = []

        # update topic datasets
        def update_topic_resource_info(
            dataset_template, topic_resources_info, topic, all_countries
        ):
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
                if countries_to_include:
                    for country in countries_to_include:
                        if country in countries:
                            all_countries.append(country)
                else:
                    all_countries.update(countries)
                topic_resources_info[file_type] = (
                    file_path,
                    resource_description,
                    start_date,
                    end_date,
                )

        for maintopic, value in self._configuration["topics"].items():
            if maintopic == "countryYear":
                continue
            dataset_template = self._configuration["datasets"].get(maintopic)
            if not dataset_template:
                continue
            topic_resources_info = {}
            all_countries = set()
            if isinstance(value, str):
                update_topic_resource_info(
                    dataset_template, topic_resources_info, maintopic, all_countries
                )
            else:
                for subtopic in value:
                    if subtopic == "overview":
                        update_topic_resource_info(
                            dataset_template,
                            topic_resources_info,
                            maintopic,
                            all_countries,
                        )
                        continue
                    update_topic_resource_info(
                        dataset_template, topic_resources_info, subtopic, all_countries
                    )

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
                try:
                    _, topic, topic_type = file_type.split("-")
                    resource = f"{topic}-{topic_type}"
                except ValueError:
                    _, topic = file_type.split("-")
                    resource = topic
                if topic not in dataset_template["topics"]:
                    continue
                tag_list = dataset_template["tags"].get(topic, [])
                tags.update(tag_list)
                resource_description = dataset_template["resource_descriptions"][topic]
                start_date, end_date = self.get_start_end_dates(
                    resource, [country.lower()]
                )
                if not start_date:
                    continue
                country_resources_info[file_type] = (
                    file_path,
                    resource_description,
                    start_date,
                    end_date,
                )

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
        topic_datasets_to_update = self.get_topic_datasets(countries_to_update)
        country_datasets_to_update = self.get_country_datasets(countries_to_update)
        return topic_datasets_to_update + country_datasets_to_update

    def delete_and_reorder_resources(
        self, dataset: Dataset, new_resources: list[Resource]
    ) -> None:
        resource_list_names = [x["name"] for x in new_resources]
        resources_check = dataset.get_resources()

        # Delete old API resources
        countries = dataset.get_location_iso3s()
        iso_match = ""
        if len(countries) == 1:
            iso_match = f"({countries[0].lower()}\\s)?"
        proper_names = []
        for _, value in self._configuration["topics"].items():
            if isinstance(value, str):
                if value == "":
                    proper_names.append("")
                else:
                    proper_names.append(value.lower().replace(" ", "\\s") + "\\s")
            else:
                for _, subtopic in value.items():
                    proper_names.append(subtopic.lower().replace(" ", "\\s") + "\\s")

        old_resource_patterns = [
            "[0-9]{4}(-[0-9]{4})?(\\s|-)"
            + iso_match
            + proper_name
            + "(incident|overview)\\sdata.xlsx"
            for proper_name in proper_names
        ]
        for resource in resources_check:
            matches = [
                re.match(old_resource_pattern, resource["name"], re.IGNORECASE)
                for old_resource_pattern in old_resource_patterns
            ]
            if resource["name"] not in resource_list_names and any(matches):
                dataset.delete_resource(resource)

        # Reorder resources so that the datasets from the API come first
        revised_dataset = Dataset.read_from_hdx(dataset["name"])
        resources_check = revised_dataset.get_resources()
        reordered_resource_ids = [
            x["id"] for x in resources_check if x["name"] in resource_list_names
        ]
        reordered_resource_ids.extend(
            [x["id"] for x in resources_check if x["name"] not in resource_list_names]
        )

        revised_dataset.reorder_resources(resource_ids=reordered_resource_ids)

        return
