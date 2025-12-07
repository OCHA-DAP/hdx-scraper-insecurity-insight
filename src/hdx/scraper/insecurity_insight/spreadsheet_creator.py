#!/usr/bin/python
"""insecurity insight scraper"""

import logging
from datetime import date
from os.path import join

from hdx.api.configuration import Configuration
from hdx.utilities.retriever import Retrieve
from pandas import DataFrame
from pandas.io.formats import excel

from hdx.scraper.insecurity_insight.utilities import pick_date_and_iso_country_fields

logger = logging.getLogger(__name__)


class SpreadsheetCreator:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
        api_cache: dict,
    ):
        self._configuration = configuration
        self._temp_folder = retriever.temp_dir
        self._api_cache = api_cache
        self._file_paths = {}

    def filter_json_rows(
        self, topic: str, topic_type: str, country_filter: str, year_filter: str
    ) -> list[dict]:
        filtered_rows = []

        api_response = self._api_cache[f"{topic}-{topic_type}"]
        date_field, iso_country_field = pick_date_and_iso_country_fields(
            api_response[0]
        )

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

    def create_spreadsheet(
        self,
        topic: str,
        topic_type: str,
        proper_name: str,
        year_filter: str = "",
        country_filter: str | None = None,
    ):
        filtered_rows = self.filter_json_rows(
            topic, topic_type, country_filter, year_filter
        )
        if len(filtered_rows) == 0:
            logger.info(
                f"API response for `{topic}-{topic_type}` with country_filter {country_filter} contained no data"
            )
            file_path = None
        else:
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
                filename = f"{start_year}-{end_year}{country_iso} {proper_name} Incident Data.xlsx"
            elif topic_type == "incidents-current-year":
                filename = f"{start_year} {proper_name} Incident Data.xlsx"
            elif topic_type == "overview":
                filename = f"{start_year}-{end_year}{country_iso} {proper_name} Overview Data.xlsx"
            else:
                raise (ValueError(f"Unknown topic type {topic_type}!"))
            if start_year == end_year:
                filename = filename.replace(f"-{end_year}", "")

            # Despite the warning, this is the accepted way to remove the default bold header
            excel.ExcelFormatter.header_style = None

            # We can make the output an Excel table:
            # https://stackoverflow.com/questions/58326392/how-to-create-excel-table-with-pandas-to-excel
            file_path = join(self._temp_folder, filename)
            output_dataframe.to_excel(
                file_path,
                index=False,
            )

        if country_filter:
            self._file_paths[f"{country_filter}-{topic}-{topic_type}"] = file_path
        else:
            self._file_paths[f"{topic}-{topic_type}"] = file_path

    def refresh_spreadsheets_with_fresh_data(
        self,
        current_year: int,
        countries: list | None = None,
    ) -> dict:
        topics_to_update = self._configuration["topics"]
        logger.info("Refreshing topic spreadsheets")
        for topic_type in self._configuration["topic_types"]:
            year_filter = ""
            if topic_type == "incidents-current-year":
                year_filter = str(current_year)
            for maintopic, value in topics_to_update.items():
                if isinstance(value, str):
                    self.create_spreadsheet(
                        maintopic, topic_type, value, year_filter=year_filter
                    )
                    continue

                for topic, proper_name in value.items():
                    if topic == "overview":
                        if topic_type != "overview":
                            continue
                        self.create_spreadsheet(
                            maintopic, topic_type, proper_name, year_filter=year_filter
                        )
                        continue
                    if topic_type == "overview":
                        continue
                    self.create_spreadsheet(
                        topic, topic_type, proper_name, year_filter=year_filter
                    )

        logger.info("Refreshing all country spreadsheets")
        if countries is None:
            countries = self._configuration["country_datasets"]
        for country in countries:
            if country == "all":
                continue
            for maintopic, value in topics_to_update.items():
                if isinstance(value, str):
                    self.create_spreadsheet(
                        maintopic, "incidents", value, country_filter=country
                    )
                    continue

                for topic, proper_name in value.items():
                    if topic == "overview":
                        continue
                    self.create_spreadsheet(
                        topic, "incidents", proper_name, country_filter=country
                    )
        return self._file_paths
