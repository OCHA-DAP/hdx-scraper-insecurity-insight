#!/usr/bin/python
"""insecurity insight scraper"""

import logging
from os.path import join
from typing import Tuple

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

    @staticmethod
    def filter_country(
        df: DataFrame, iso_country_field: str, country_filter: str = ""
    ) -> DataFrame:
        if not country_filter:
            return df
        return df[df[iso_country_field] == country_filter]

    @staticmethod
    def filter_process_dates(
        df: DataFrame,
        field_types: dict,
        date_field: str,
        year_filter: int | None = None,
    ) -> Tuple[DataFrame, int, int]:
        if year_filter:
            if field_types[date_field] == "datetime64[ns, UTC]":
                df = df[df[date_field].dt.year == year_filter]
            else:
                df = df[df[date_field] == year_filter]
        if field_types[date_field] == "datetime64[ns, UTC]":
            start_year = df[date_field].dt.year.min()
            end_year = df[date_field].dt.year.max()
        else:
            start_year = df[date_field].min()
            end_year = df[date_field].max()
        df = df.copy()
        for key, value in field_types.items():
            if value == "datetime64[ns, UTC]":
                df[key] = df[key].dt.date
        return df, start_year, end_year

    def create_spreadsheet(
        self,
        topic: str,
        topic_type: str,
        proper_name: str,
        year_filter: int | None = None,
        country_filter: str | None = None,
    ):
        resource = f"{topic}-{topic_type}"
        if topic == "countryYear":
            resource = topic
        api_response = self._api_cache[resource]
        file_paths_key = resource
        if country_filter:
            file_paths_key = f"{country_filter}-{resource}"

        # get columns with correct type
        df = DataFrame.from_dict(api_response, dtype="str")
        field_types = {}
        for column in df.columns:
            if column.lower() in ["latitude", "longitude"]:
                field_type = "float64"
            elif column.lower().startswith("date"):
                field_type = "datetime64[ns, UTC]"
            elif column.lower() == "sind event id":
                field_type = "str"
            else:
                values = df[column].dropna()
                is_numeric = values.str.isnumeric()
                if is_numeric.all():
                    field_type = "Int64"
                else:
                    is_numeric = values.str.replace(
                        "\\.", "", regex=True
                    ).str.isnumeric()
                    if is_numeric.all():
                        is_int = values.str.replace(
                            "\\.0", "", regex=True
                        ).str.isnumeric()
                        if is_int.all():
                            field_type = "Int64"
                        else:
                            field_type = "float"
                    else:
                        field_type = "str"
            field_types[column] = field_type
        for key, value in field_types.items():
            if value == "str":
                df[key] = df[key].replace("", None)
        df = df.astype(field_types, errors="ignore")

        date_field, iso_country_field = pick_date_and_iso_country_fields(
            api_response[0]
        )
        if country_filter:
            country_iso = country_filter
            df = self.filter_country(df, iso_country_field, country_filter)
        else:
            country_iso = ""
        if len(df) == 0:
            logger.info(
                f"API response for `{resource}` with country_filter '{country_filter}' contained no data"
            )
            self._file_paths[file_paths_key] = None
            return
        filtered_df, start_year, end_year = self.filter_process_dates(
            df, field_types, date_field, year_filter
        )
        if len(filtered_df) == 0:
            if year_filter:
                year_filter = year_filter - 1
                filtered_df, start_year, end_year = self.filter_process_dates(
                    df, field_types, date_field, year_filter
                )
            if len(filtered_df) == 0:
                logger.warning(
                    f"API response for `{resource}` with year_filter {year_filter} contained no data (country_filter was '{country_filter}')"
                )
                self._file_paths[file_paths_key] = None
                return
        df = filtered_df
        if topic_type == "incidents":
            filename = f"{start_year}-{end_year} {country_iso} {proper_name} Incident Data.xlsx"
        elif (
            topic_type == "incidents-current-year"
        ):  # Current year data is not generated for country datasets
            filename = f"{start_year} {proper_name} Incident Data.xlsx"
        elif topic_type == "overview":
            filename = f"{start_year}-{end_year} {country_iso} {proper_name} Overview Data.xlsx"
        else:
            raise (ValueError(f"Unknown topic type {topic_type}!"))
        if start_year == end_year:
            filename = filename.replace(f"-{end_year}", "")
        filename = filename.replace("  ", " ")

        # Despite the warning, this is the accepted way to remove the default bold header
        excel.ExcelFormatter.header_style = None

        # We can make the output an Excel table:
        # https://stackoverflow.com/questions/58326392/how-to-create-excel-table-with-pandas-to-excel
        file_path = join(self._temp_folder, filename)
        df.to_excel(
            file_path,
            index=False,
        )
        self._file_paths[file_paths_key] = file_path

    def refresh_spreadsheets_with_fresh_data(
        self,
        current_year: int,
        countries: list | None = None,
        topics_to_update: dict | None = None,
    ) -> dict:
        if not topics_to_update:
            topics_to_update = self._configuration["topics"]
        logger.info("Refreshing topic spreadsheets")
        for topic_type in self._configuration["topic_types"]:
            year_filter = None
            if topic_type == "incidents-current-year":
                year_filter = current_year
            for maintopic, value in topics_to_update.items():
                if isinstance(value, str):
                    if maintopic == "countryYear":
                        continue
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
                    if maintopic == "countryYear":
                        self.create_spreadsheet(
                            maintopic, "overview", value, country_filter=country
                        )
                    else:
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
