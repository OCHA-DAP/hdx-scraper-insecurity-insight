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

1. `USE_SAMPLE` - if `True` then samples of the API are used rather than the live API are used. This is handy for testing because it is fast.  
2. `DRY_RUN` - if `True` then there are no writes to HDX
3. `REFRESH_ALL` = if [`all`] then all resources and datasets are updated, rather than just those that have changed. REFRESH_ALL can contain a list of entries i.e. ["foodsecurity"], in which case just those datasets listed are updated or None in which case only those datasets with new data are updated.
4. `COUNTRIES` - if None then all countries are updated or a list of countries can be selected  ["PSE"] in which case just these countries are updated. Again this is mainly used for testing;
5. `USE_LEGACY` - if `False` then new datasets are created/updated based on templates, rather than updating legacy datasets from HDX
6. `HDX_SITE` - sets the target HDX instance either "prod" or "stage"


## New dataset and resource (spreadsheet) process
 
The production of datasets and resources (Excel spreadsheets) from the Insecurity Insight API is driven by a number of files in the the [metadata](src/hdx_scraper_insecurity_insight/metadata/) folder. 

Metadata files are as follows:
1. `attributes.csv` - 
2. `schema.csv`
3. `schema-overview.csv`
4. `New-HDX-APIs-1-HDX-Home-Page.csv`
5. `New-HDX-APIs-2-Topics.csv`
6. `New-HDX-APIs-3-Country.csv`

The `attributes.csv` file configures the datasets to be created, the `dataset_name` is used as a key to fetch mainly description text from the `New-HDX-APIs-*.csv` files. These are derived from a Google Sheet which Insecurity Insight created. It is here that corrections to dataset and resource text are made. Dates are injected into descriptions in a number of places using a crude templating system.

The `schema` files contain HXL tags which were originally included in the spreadsheets but were
removed because Insecurity Insight preferred that the Excel spreadsheet columns had appropriate
data types. The HXL tags forced Excel to treat all columns as string type. They are only used in the `create_spreadsheet` function.


When the original datasets were created from the API, the schema files were created using a script:
```
./generate_api_transformation_schema.py {dataset_name|all}
```

Results are written to console and are only written to `schema.csv` if entries are not already present.

Where API fields are not readily associated with the existing Excel spreadsheets the file [field_mappings.csv](src/hdx_scraper_insecurity_insight/metadata/field_mappings.csv) provides a lookup.

Entries in both `attributes.csv` and `schema.csv` are keyed by a `dataset_name`

The countries datasets are specified in the [countries.csv](src/hdx_scraper_insecurity_insight/metadata/countries.csv) file

Test coverage is good, and typically when new work is done further tests are added.

The `make run` command is used during development with appropriate parameters set by editing the code. Possibly a `click` or similar commandline interface could be added here.


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

