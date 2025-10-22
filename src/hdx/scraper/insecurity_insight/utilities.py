#!/usr/bin/python
"""insecurity insight utilities"""

import logging
from os.path import basename, join
from typing import Optional

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import dict_of_dicts_add
from pandas import DataFrame
from pandas.io.formats import excel

logger = logging.getLogger(__name__)


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

    # get columns with correct type
    output_dataframe = DataFrame.from_dict(filtered_rows, dtype="str")
    field_types = {}
    for column in output_dataframe.columns:
        if column.lower() in ["latitude", "longitude"]:
            field_type = "float64"
        elif column.lower().startswith("date"):
            field_type = "datetime64[ns, UTC]"
        elif column.lower() == "sind event id":
            field_type = "str"
        else:
            values = output_dataframe[column]
            is_numeric = values.str.isnumeric()
            if is_numeric.all():
                field_type = "Int64"
            else:
                field_type = "str"
        field_types[column] = field_type
    for key, value in field_types.items():
        if value == "str":
            output_dataframe[key] = output_dataframe[key].replace("", None)
    output_dataframe = output_dataframe.astype(field_types, errors="ignore")
    for key, value in field_types.items():
        if value == "datetime64[ns, UTC]":
            output_dataframe[key] = output_dataframe[key].dt.date

    # Generate filename
    date_field, _ = pick_date_and_iso_country_fields(filtered_rows[0])
    start_year = min([x[date_field] for x in filtered_rows])[0:4]
    end_year = max([x[date_field] for x in filtered_rows])[0:4]
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

    output_filepath = join(output_dir, filename)
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
    countries = sorted(list(set(countries)))
    return countries


def get_dates_from_api_response(
    api_response: list[dict], countries: Optional[list] = None
) -> tuple[str, str]:
    dates = []
    date_field, iso_country_field = pick_date_and_iso_country_fields(api_response[0])
    for row in api_response:
        if countries is None:
            dates.append(row[date_field])
        elif row[iso_country_field] in countries:
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
    api_cache: dict,
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

    topic_dates = {}
    if topic == "all":
        start_dates = []
        end_dates = []
        for top in dataset_template["topics"]:
            start_date, end_date = get_dates_from_api_response(
                api_cache[f"{top}-incidents"], countries
            )
            dict_of_dicts_add(topic_dates, top, "start_date", start_date)
            dict_of_dicts_add(topic_dates, top, "end_date", end_date)
            start_dates.append(start_date)
            end_dates.append(end_date)
        dataset.set_time_period(min(start_dates), max(end_dates))
    else:
        start_date, end_date = get_dates_from_api_response(
            api_cache[f"{topic}-incidents"]
        )
        dict_of_dicts_add(topic_dates, topic, "start_date", start_date)
        dict_of_dicts_add(topic_dates, topic, "end_date", end_date)
        dataset.set_time_period(start_date, end_date)

    resource_list = []
    for file_type, file_path in file_paths.items():
        if topic == "all":
            top = file_type.split("-")[1]
            resource_description = dataset_template["resource_descriptions"][top]
            if top not in dataset_template["topics"]:
                continue
        else:
            top = topic
            topic_type = "-".join(file_type.split("-")[1:])
            resource_description = dataset_template["resource_descriptions"][topic_type]

        topic_start_date = topic_dates[top]["start_date"]
        topic_end_date = topic_dates[top]["end_date"]
        start_date_str = parse_date(topic_start_date).strftime("%d %B %Y")
        end_date_str = parse_date(topic_end_date).strftime("%d %B %Y")
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

    dataset.add_update_resources(resource_list)
    return dataset
