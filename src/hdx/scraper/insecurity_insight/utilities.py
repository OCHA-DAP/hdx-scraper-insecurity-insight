#!/usr/bin/python
"""insecurity insight utilities"""

import logging

logger = logging.getLogger(__name__)


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


def get_countries_from_api_response(api_response: list[dict]) -> set:
    countries = set()
    _, iso_country_field = pick_date_and_iso_country_fields(api_response[0])
    for row in api_response:
        if row[iso_country_field] != "":
            countries.add(row[iso_country_field].lower())
    return countries


def get_dates_from_api_response(
    api_response: list[dict], countries: list | None = None
) -> tuple[str, str]:
    dates = []
    date_field, iso_country_field = pick_date_and_iso_country_fields(api_response[0])
    for row in api_response:
        if countries is None:
            dates.append(row[date_field])
        elif row[iso_country_field].lower() in countries:
            dates.append(row[date_field])
    api_start_date = None
    api_end_date = None
    if len(dates) != 0:
        api_start_date = min(dates).replace("Z", "")[0:10]
        api_end_date = max(dates).replace("Z", "")[0:10]
    return api_start_date, api_end_date
