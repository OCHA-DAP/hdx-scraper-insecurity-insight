import logging

from hdx.scraper.insecurity_insight.utilities import pick_date_and_iso_country_fields

logger = logging.getLogger(__name__)


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
