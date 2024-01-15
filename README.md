# HDX Scraper - Insecurity Insight

## Introduction

This repository produces datasets for the [Insecurity Insights](https://data.humdata.org/organization/insecurity-insight) organisation on HDX.

Created by Ian Hopkinson November 2023-January 2024 under JIRA Epic [HDXDS-187](https://humanitarian.atlassian.net/browse/HDXDS-187)

## Installation

`hdx-scraper-insecurity-insight` is a Python application. To install the [GitHub repo](https://github.com/OCHA-DAP/hdx-scraper-insecurity-insight) should be cloned, and a virtual enviroment created:

```shell
python -m venv venv
source venv/Scripts/activate
```

The code is installed with

```shell
pip install -e .
```

`hdx-cli-toolkit` uses the `hdx-python-api` library, configuration for which is done in the usual way [described here](https://hdx-python-api.readthedocs.io/en/latest/). 

For local running the user agent (`insecurity_insight`) is specified in the `~/.useragents.yaml` file:
```
hdx-scraper-insecurity-insight:
    preprefix: HDXINTERNAL
    user_agent: insecurity_insight
```

## Scheduled dataset updates

The dataset update process is run using a GitHub Action specified in [this file](.github/workflows/run-python-script.yaml).
This can be run manually from the GitHUB UI or on a scheduled basis using a CRON entry in the Action.

Dataset updates use the `run` target in the [Makefile](Makefile) which simply executes `run.py` carrying out the following processes:

```python
1. fetch_and_cache_api_responses(use_sample=USE_SAMPLE)
2. DATASET_CACHE = fetch_and_cache_datasets(use_legacy=USE_LEGACY)
3. HAS_CHANGED, CHANGED_LIST = check_api_has_not_changed(API_CACHE)
4. ITEMS_TO_UPDATE = decide_which_resources_have_fresh_data(
        DATASET_CACHE, API_CACHE, refresh_all=REFRESH_ALL
    )
5. refresh_spreadsheets_with_fresh_data(ITEMS_TO_UPDATE, API_CACHE)
6. MISSING_REPORT = update_datasets_whose_resources_have_changed(
        ITEMS_TO_UPDATE, API_CACHE, DATASET_CACHE, dry_run=DRY_RUN, use_legacy=USE_LEGACY
    )
```
Which is controlled by a set of flags:

1. `USE_SAMPLE` - if `True` then samples of the API are used rather than the live API are used  
2. `DRY_RUN` - if `True` then there are no writes to HDX
3. `REFRESH_ALL` = if `True` then all resources and datasets are updated, rather than just those that have changed
4. `USE_LEGACY` = if `False` then new datasets are created/updated based on templates, rather than updating legacy datasets from HDX

## New dataset and resource (spreadsheet) process
 
The production of datasets and resources (Excel spreadsheets) from the Insecurity Insight API is driven by the [attributes.csv](src/hdx_scraper_insecurity_insight/metadata/attributes.csv) and [schema.csv](src/hdx_scraper_insecurity_insight/metadata/schema.csv) files.
The `attributes.csv` file contains information like paths and URLs to the resource samples and the API and is generated manually. The `schema.csv` can be generated automatically from a sample (using `generate_api_transformation_schema.py`) of the API response 
and a sample spreadsheet but then edited as any normal file, perhaps to add or update HXL tags. This is executed using:

```
./generate_api_transformation_schema.py {dataset_name|all}
```

Results are written to console and are only written to `schema.csv` if entries are not already present.

Where API fields are not readily associated with the existing Excel spreadsheets the file [field_mappings.csv](src/hdx_scraper_insecurity_insight/metadata/field_mappings.csv) provides a lookup.

Entries in both `attributes.csv` and `schema.csv` are keyed by a `dataset_name`

The countries datasets are specified in the [countries.csv](src/hdx_scraper_insecurity_insight/metadata/countries.csv) file

During development various parts of the process can be tested independently from the commandline:

```
./create_spreadsheets.py {dataset_name|all} {iso_country_code|}
```

Once spreadsheets have been created the `create_datasets.py` command is run to create the datasets in HDX. A commandline argument (the dataset name) can be supplied with the word "all" instructing the script to create all the datasets defined in the `attributes.csv` file, an (uppercase) ISO country code can be used as a second optional argument with the dataset name `insecurity-insight-country-dataset` to produce country pages.

```
./create_datasets.py {dataset_name|all} {iso_country_code|}
```

## Contributions

This project users a GitHub Action to run tests and linting, as well as the running the process. It requires the following environment variables to be set in the `test` environment:

```
EMAIL_FROM - obtain from the HDX Data Systems team
EMAIL_PORT - obtain from the HDX Data Systems team
EMAIL_USERNAME - obtain from the HDX Data Systems team
HDX_SITE - Value: stage
PREPREFIX - Value: HDXINTERNAL
USER_AGENT - Value: insecurity_insight 
```
And the following secrets:
```
EMAIL_LIST - obtain from the HDX Data Systems team
EMAIL_PASSWORD - obtain from the HDX Data Systems team
EMAIL_SERVER - obtain from the HDX Data Systems team
HDX_KEY - obtain from your account on HDX
```

