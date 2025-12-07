#!/usr/bin/python
"""insecurity insight utilities"""

import logging
from datetime import date
from os.path import join

from pandas import DataFrame
from pandas.io.formats import excel

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


def create_spreadsheet(
    filtered_rows: list[dict],
    topic_type: str,
    proper_name: str,
    output_dir: str,
    country_filter: str = None,
) -> None | str:
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
    min_date = output_dataframe[date_field].min()
    max_date = output_dataframe[date_field].max()
    if isinstance(min_date, date):
        start_year = min_date.year
        end_year = max_date.year
    else:
        start_year = int(min_date)
        end_year = int(max_date)

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
