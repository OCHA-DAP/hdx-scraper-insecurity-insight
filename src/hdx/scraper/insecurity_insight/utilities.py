#!/usr/bin/python
"""insecurity insight utilities"""

import csv
import logging
import os
from os.path import dirname, join

from hdx.data.dataset import Dataset
from hdx.utilities.dictandlist import read_list_from_csv
from pandas import DataFrame
from pandas.io.formats import excel

logger = logging.getLogger(__name__)

ATTRIBUTES_FILEPATH = os.path.join(
    os.path.dirname(__file__), "metadata", "attributes.csv"
)
INSECURITY_INSIGHTS_FILEPATH_PAGES = os.path.join(
    os.path.dirname(__file__), "metadata", "New-HDX-APIs-1-HDX-Home-Page.csv"
)
INSECURITY_INSIGHTS_FILEPATH_TOPICS = os.path.join(
    os.path.dirname(__file__), "metadata", "New-HDX-APIs-2-Topics.csv"
)
INSECURITY_INSIGHTS_FILEPATH_COUNTRIES = os.path.join(
    os.path.dirname(__file__), "metadata", "New-HDX-APIs-3-Country.csv"
)


def filter_json_rows(
    country_filter: str, year_filter: str, api_response: list[dict]
) -> list[dict]:
    filtered_rows = []

    date_field, iso_country_field = pick_date_and_iso_country_fields(api_response[0])

    for api_row in api_response:
        if (
            country_filter is not None
            and len(country_filter) != 0
            and api_row[iso_country_field] != country_filter
        ):
            continue
        if (
            year_filter is not None
            and len(year_filter) != 0
            and api_row[date_field][0:4] != year_filter
        ):
            continue
        filtered_rows.append(api_row)

    return filtered_rows


def censor_location(countries: list[str], api_response: list[dict]) -> list[dict]:
    censored_rows = []

    if "Latitude" not in api_response[0].keys():
        logging.info("API response does not contain latitude/longitude fields")
        return api_response
    else:
        logging.info(
            f"API response contains latitude/longitude fields, censoring for {countries}"
        )
    _, iso_country_field = pick_date_and_iso_country_fields(api_response[0])

    # Geo fields are Latitude, Longitude and Geo Precision
    n_censored = 0
    n_records = 0
    for api_row in api_response:
        n_records += 1
        if api_row[iso_country_field] in countries:
            n_censored += 1
            api_row["Latitude"] = None
            api_row["Longitude"] = None
            api_row["Geo Precision"] = "censored"
        censored_rows.append(api_row)

    logging.info(f"{n_censored} of {n_records} censored for {countries}")
    return censored_rows


def censor_event_description(api_response: list[dict]) -> list[dict]:
    censored_rows = []

    if "Event Description" not in api_response[0].keys():
        logging.info("API response does not contain Event Description fields")
        return api_response
    else:
        logging.info(
            "API response contains Event Description, censoring for all countries"
        )

    # Geo fields are Latitude, Longitude and Geo Precision
    n_censored = 0
    n_records = 0
    for api_row in api_response:
        n_records += 1
        n_censored += 1
        api_row["Event Description"] = ""
        # api_row.pop("Event Description", None)
        censored_rows.append(api_row)

    logging.info(f"{n_censored} of {n_records} Event Description blanked")
    return censored_rows


def read_attributes(dataset_name: str) -> dict:
    with open(ATTRIBUTES_FILEPATH, "r", encoding="UTF-8") as attributes_filehandle:
        attribute_rows = csv.DictReader(attributes_filehandle)

        attributes = {}
        for row in attribute_rows:
            if row["dataset_name"] != dataset_name:
                continue
            if row["attribute"] == "resource":
                if "resource" not in attributes:
                    attributes["resource"] = [row["value"]]
                else:
                    attributes["resource"].append(row["value"])
            else:
                attributes[row["attribute"]] = row["value"]

    return attributes


def read_insecurity_insight_attributes_pages(dataset_name: str) -> dict:
    ii_attributes = {}
    with open(
        INSECURITY_INSIGHTS_FILEPATH_PAGES, "r", encoding="UTF-8"
    ) as attributes_filehandle:
        attribute_rows = csv.DictReader(attributes_filehandle)
        for row in attribute_rows:
            if row["ih_name"] != dataset_name:
                continue
            ii_attributes = row
            break

    if ii_attributes:
        legacy_name = ii_attributes["HDX link"].split("/")[-1]
        ii_attributes["legacy_name"] = legacy_name
    return ii_attributes


def read_insecurity_insight_resource_attributes(dataset_name: str) -> list[dict]:
    resource_attributes = []

    with open(
        INSECURITY_INSIGHTS_FILEPATH_TOPICS, "r", encoding="UTF-8"
    ) as attributes_filehandle:
        attribute_rows = csv.DictReader(attributes_filehandle)
        for row in attribute_rows:
            if row["parent_name"] != dataset_name:
                continue
            resource_attributes.append(row)

    if len(resource_attributes) == 0:
        with open(
            INSECURITY_INSIGHTS_FILEPATH_COUNTRIES, "r", encoding="UTF-8"
        ) as attributes_filehandle:
            attribute_rows = csv.DictReader(attributes_filehandle)
            for row in attribute_rows:
                if row["parent_name"] != dataset_name:
                    continue
                resource_attributes.append(row)

    return resource_attributes


def list_entities(type_: str = "dataset") -> list[str]:
    entity_list = []
    with open(ATTRIBUTES_FILEPATH, "r", encoding="UTF-8") as attributes_filehandle:
        attribute_rows = csv.DictReader(attributes_filehandle)
        for row in attribute_rows:
            if row["attribute"] == "entity_type" and row["value"] == type_:
                entity_list.append(row["dataset_name"])

    return entity_list


def read_schema(dataset_name: str) -> list[dict]:
    dataset_name = dataset_name.replace("-current-year", "")
    row_template = []
    schema_filepath = join(dirname(__file__), "config", "schema.csv")
    schema_rows = read_list_from_csv(schema_filepath, headers=1, dict_form=True)
    for row in schema_rows:
        if row["dataset_name"] != dataset_name:
            continue
        row["field_number"] = int(row["field_number"])
        row_template.append(row)
    row_template = sorted(row_template, key=lambda x: x["field_number"])
    return row_template


def pick_date_and_iso_country_fields(row_dictionary: dict) -> tuple[str, str]:
    iso_country_field = "Country ISO"
    if iso_country_field not in row_dictionary.keys():
        iso_country_field = "country_iso"

    date_field = "Date"
    for date_field_option in ["Date", "Year", "date"]:
        if date_field_option in row_dictionary:
            date_field = date_field_option
            break

    return date_field, iso_country_field


def create_spreadsheet(
    topic: str,
    topic_type: str,
    proper_name: str,
    api_response: list[dict],
    output_dir: str,
    year_filter: str = "",
    country_filter: str = None,
) -> None | str:
    filtered_rows = filter_json_rows(country_filter, year_filter, api_response)
    if len(filtered_rows) == 0:
        logger.info(
            f"API reponse for `{topic}-{topic_type}` with country_filter {country_filter} contained no data"
        )
        return None

    # get columns in correct order and with correct type
    field_templates = read_schema(f"{topic}-{topic_type}")
    field_order = [field_template["field_name"] for field_template in field_templates]
    field_types = {
        field_template["field_name"]: field_template["field_type"]
        for field_template in field_templates
    }

    output_dataframe = DataFrame.from_dict(filtered_rows)
    output_dataframe = output_dataframe[field_order]
    output_dataframe.replace("", None, inplace=True)
    output_dataframe = output_dataframe.astype(field_types, errors="ignore")
    for key, value in field_types.items():
        if value == "datetime64[ns, UTC]":
            output_dataframe[key] = output_dataframe[key].dt.date

    # Generate filename
    date_field, _ = pick_date_and_iso_country_fields(api_response[0])
    start_year = min([x[date_field] for x in api_response])[0:4]
    end_year = max([x[date_field] for x in api_response])[0:4]
    country_iso = ""
    if (country_filter is not None) and (len(country_filter) != 0):
        country_iso = f"-{country_filter}"

    if topic_type == "incidents":
        filename = (
            f"{start_year}-{end_year}{country_iso} {proper_name} Incident Data.xlsx"
        )
    elif topic_type == "incidents-current-year":
        filename = f"{start_year} {proper_name} Incident Data.xlsx"
    elif topic_type == "overview":
        filename = (
            f"{start_year}-{end_year}{country_iso} {proper_name} Overview Data.xlsx"
        )

    if start_year == end_year:
        filename = filename.replace(f"-{end_year}", "")

    # We can make the output an Excel table:
    # https://stackoverflow.com/questions/58326392/how-to-create-excel-table-with-pandas-to-excel
    excel.ExcelFormatter.header_style = None

    output_filepath = os.path.join(output_dir, filename)
    output_dataframe.to_excel(
        output_filepath,
        index=False,
    )

    return output_filepath


def get_countries_from_api_response(api_response: list[dict]) -> list[dict]:
    countries = []
    _, iso_country_field = pick_date_and_iso_country_fields(api_response[0])

    for row in api_response:
        if row[iso_country_field] != "":
            countries.append(row[iso_country_field].lower())

    # Possibly we want to run a counter here to work out the significant countries in the dataset
    countries = list(set(countries))
    return countries


def get_dates_from_api_response(api_response: list[dict]) -> tuple[str, str]:
    dates = []
    date_field, _ = pick_date_and_iso_country_fields(api_response[0])
    for row in api_response:
        dates.append(row[date_field])
    api_start_date = None
    api_end_date = None
    if len(dates) != 0:
        api_start_date = min(dates).replace("Z", "")[0:10]
        api_end_date = max(dates).replace("Z", "")[0:10]
    return api_start_date, api_end_date


def create_dataset(
    topic: str,
    dataset_template: dict,
    countries: list,
    file_paths: dict,
    start_date: str,
    end_date: str,
    dataset_cache: dict = None,
) -> Dataset:
    if dataset_cache is None:
        dataset, _ = create_or_fetch_base_dataset(
            dataset_name,
            country_filter=country_filter,
            use_legacy=use_legacy,
            hdx_site=hdx_site,
        )
    else:
        if country_filter is not None and country_filter != "":
            dataset_name = dataset_name.replace("country", country_filter.lower())
        dataset = dataset_cache[dataset_name]

    if country_filter is not None and country_filter != "":
        dataset_name = dataset_name.replace("country", country_filter.lower())
    # This is where we get title, description and potentially name
    # from New-HDX-APIs-1-HDX-Home-Page.csv
    ii_metadata = read_insecurity_insight_attributes_pages(dataset_name)
    dataset["title"] = ii_metadata["Page"]
    dataset["description"] = ii_metadata["Page description"]
    dataset["name"] = ii_metadata["legacy_name"]

    # We should fetch resoure names from insecurity insight metadata here
    # resource_names = dataset_attributes["resource"]
    ii_resource_attributes = read_insecurity_insight_resource_attributes(dataset_name)
    resource_names = [x["ih_name"] for x in ii_resource_attributes]
    resource_descriptions = {
        x["ih_name"]: x["Description"] for x in ii_resource_attributes
    }
    # This is a bit nasty since it reads the API for every resource in a dataset
    # but we only do it if the dataset_date is not set
    if dataset_date is None or countries_group is None:
        dataset_date, countries_group = get_date_and_country_ranges_from_resources(
            resource_names, country_filter=country_filter
        )

    dataset["dataset_date"] = dataset_date
    dataset["groups"] = countries_group
    # Set organisation and maintainer in code because it is easier to update later.
    dataset.set_maintainer(
        "878dc76d-d357-4dce-8562-59f6421714e1"
    )  # From Insecurity Insight 878dc76d-d357-4dce-8562-59f6421714e1
    dataset.set_organization(
        "648d346e-3995-44cc-a559-29f8192a3010"
    )  # Insecurity Insight 648d346e-3995-44cc-a559-29f8192a3010

    resource_list = []

    n_missing_resources = 0
    logger.info("Resources:")
    for resource_name in resource_names:
        attributes = read_attributes(resource_name)
        # This skips the current year spreadsheet resources which have no dataset_name (yet)
        if not attributes:
            continue
        resource_filepath = find_resource_filepath(
            resource_name, attributes, country_filter=country_filter
        )

        if resource_filepath is None:
            n_missing_resources += 1
            continue

        # Update resource_description
        resource_description = resource_descriptions[resource_name]
        if "[current year]" in resource_description:
            current_year = datetime.datetime.now().isoformat()[0:4]
            date_regex = re.findall(r"\d{4}-\d{2}-\d{2}", dataset_date)
            if len(date_regex) != 2:
                logger.warning(
                    f"Dataset_date '{dataset_date}' is an unexpected format, "
                    "using current date for most recent"
                )
            resource_description = resource_description.replace(
                "[current year]", current_year
            )

        # This is where we would get start and end dates for an actual dataset
        if "[to date]" in resource_description:
            _, end_date = get_date_range_from_resource_file(resource_filepath)
            end_date_dt = datetime.datetime.fromisoformat(end_date)
            end_date_human = end_date_dt.strftime("%d %B %Y")

            resource_description = resource_description.replace(
                "[to date]", end_date_human
            )
            print(resource_description, flush=True)

        resource = Resource(
            {
                "name": os.path.basename(resource_filepath),
                "description": resource_description,
                "format": attributes.get("file_format", "XLSX"),
            }
        )
        resource.set_file_to_upload(resource_filepath)
        resource_list.append(resource)

    resource_list_names = [x["name"] for x in resource_list]

    dataset.add_update_resources(resource_list)

    dataset_name = dataset["name"]
    dataset.create_in_hdx(hxl_update=False)
    # Reorder resources so that the datasets from the API come first - code from hdx-cli-toolkit
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

    return dataset
