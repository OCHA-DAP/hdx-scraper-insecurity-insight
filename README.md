# Collector for insecurity_insight Datasets
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-insecurity-insight/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-insecurity-insight/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-insecurity-insight/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-insecurity-insight?branch=main)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

This repository produces datasets for the [Insecurity Insights](https://data.humdata.org/organization/insecurity-insight) organisation on HDX.

Created by Ian Hopkinson November 2023-January 2024 under JIRA Epic [HDXDS-187](https://humanitarian.atlassian.net/browse/HDXDS-187)

Dataset updates are controlled by a set of flags:

1. `USE_SAMPLE` - if `True` then samples of the API are used rather than the live API are used. This is handy for testing because it is fast.
2. `DRY_RUN` - if `True` then there are no writes to HDX
3. `REFRESH_ALL` = if [`all`] then all resources and datasets are updated, rather than just those that have changed. REFRESH_ALL can contain a list of entries i.e. ["foodsecurity"], in which case just those datasets listed are updated or None in which case only those datasets with new data are updated.
4. `COUNTRIES` - if None then all countries are updated or a list of countries can be selected  ["PSE"] in which case just these countries are updated. Again this is mainly used for testing;
5. `USE_LEGACY` - if `False` then new datasets are created/updated based on templates, rather than updating legacy datasets from HDX
6. `HDX_SITE` - sets the target HDX instance either "prod" or "stage"

The production of datasets and resources (Excel spreadsheets) from the Insecurity Insight API is driven by a number of files in the the metadata folder.

Metadata files are as follows:
1. `attributes.csv`
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

## Development

### Environment

Development is currently done using Python 3.12. We recommend using a virtual
environment such as ``venv``:

```shell
    python -m venv venv
    source venv/bin/activate
```

In your virtual environment, install all packages for development by running:

```shell
    pip install -r requirements.txt
```

### Installing and running


For the script to run, you will need to have a file called
.hdx_configuration.yaml in your home directory containing your HDX key, e.g.:

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod

 You will also need to supply the universal .useragents.yaml file in your home
 directory as specified in the parameter *user_agent_config_yaml* passed to
 facade in run.py. The collector reads the key
 **hdx-scraper-insecurity_insight** as specified in the parameter
 *user_agent_lookup*.

 Alternatively, you can set up environment variables: `USER_AGENT`, `HDX_KEY`,
`HDX_SITE`, `EXTRA_PARAMS`, `TEMP_DIR`, and `LOG_FILE_ONLY`.

To install and run, execute:

```shell
    pip install .
    python -m hdx.scraper.insecurity_insight
```

### Pre-commit

Be sure to install `pre-commit`, which is run every time you make a git commit:

```shell
    pip install pre-commit
    pre-commit install
```

With pre-commit, all code is formatted according to
[ruff](https://docs.astral.sh/ruff/) guidelines.

To check if your changes pass pre-commit without committing, run:

```shell
    pre-commit run --all-files
```

### Testing

Ensure you have the required packages to run the tests:

```shell
    pip install -r requirements-test.txt
```

To run the tests and view coverage, execute:

```shell
    pytest -c --cov hdx
```

## Packages

[uv](https://github.com/astral-sh/uv) is used for package management.  If
you’ve introduced a new package to the source code (i.e. anywhere in `src/`),
please add it to the `project.dependencies` section of `pyproject.toml` with
any known version constraints.

To add packages required only for testing, add them to the `test` section under
`[project.optional-dependencies]`.

Any changes to the dependencies will be automatically reflected in
`requirements.txt` and `requirements-test.txt` with `pre-commit`, but you can
re-generate the files without committing by executing:

```shell
    pre-commit run pip-compile --all-files
```

## Project

[Hatch](https://hatch.pypa.io/) is used for project management. The project can be built using:

```shell
    hatch build
```

Linting and syntax checking can be run with:

```shell
    hatch fmt --check
```

Tests can be executed using:

```shell
    hatch test
```
