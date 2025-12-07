import logging

from hdx.api.configuration import Configuration
from hdx.location.country import Country
from hdx.utilities.base_downloader import DownloadError
from hdx.utilities.retriever import Retrieve

from hdx.scraper.insecurity_insight.utilities import pick_date_and_iso_country_fields

logger = logging.getLogger(__name__)


class APIReader:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._api_cache = {}

    @staticmethod
    def censor_location(countries: list[str], api_response: list[dict]) -> list[dict]:
        censored_rows = []

        _, iso_country_field = pick_date_and_iso_country_fields(api_response[0])

        # Geo fields are Latitude, Longitude and Geo Precision
        n_censored = 0
        n_records = 0
        for api_row in api_response:
            n_records += 1
            countryiso = api_row.get(iso_country_field)
            if countryiso is None:
                countryname = api_row["Country"]
                countryiso, _ = Country.get_iso3_country_code_fuzzy(countryname)
                if not countryiso:
                    logger.error(f"No country iso found for {countryname}!")
                    continue
            api_row[iso_country_field] = countryiso
            if countryiso in countries:
                if "Latitude" in api_row:
                    n_censored += 1
                    api_row["Latitude"] = None
                    api_row["Longitude"] = None
                    api_row["Geo Precision"] = "censored"
            censored_rows.append(api_row)

        logging.info(f"Censoring latitude/longitude fields if present for {countries}")
        logging.info(f"{n_censored} of {n_records} censored for {countries}")
        return censored_rows

    @staticmethod
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

    def fetch_api_responses(self) -> dict:
        def add_cache(topic, topic_type, api_url):
            resource = f"{topic}-{topic_type}"
            logger.info(f"Fetching data for {resource} from API")

            if topic_type == "overview":
                api_url = f"{api_url}Overview"
            try:
                json_response = self._retriever.download_json(api_url)
            except DownloadError:
                logger.error(f"Failed to download response for {resource}")
                return

            censored_location_response = self.censor_location(["PSE"], json_response)
            censored_response = self.censor_event_description(
                censored_location_response
            )
            self._api_cache[resource] = censored_response

        for topic_type in self._configuration["topic_types"]:
            for maintopic, value in self._configuration["topics"].items():
                if isinstance(value, str) or topic_type == "overview":
                    api_url = f"{self._configuration['base_url']}{maintopic}"
                    add_cache(maintopic, topic_type, api_url)
                    continue
                for topic in value:
                    if topic == "overview":
                        continue
                    api_url = f"{self._configuration['base_url']}{topic}"
                    add_cache(topic, topic_type, api_url)

        logger.info(
            f"Loaded {len(self._api_cache)} API responses to cache, expected 32"
        )
        return self._api_cache
