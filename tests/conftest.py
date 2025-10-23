from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.dataset import Dataset
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.utilities.useragent import UserAgent


@pytest.fixture(scope="session")
def fixtures_dir():
    return join("tests", "fixtures")


@pytest.fixture(scope="session")
def input_dir(fixtures_dir):
    return join(fixtures_dir, "input")


@pytest.fixture(scope="session")
def config_dir(fixtures_dir):
    return join("src", "hdx", "scraper", "insecurity_insight", "config")


@pytest.fixture(scope="function")
def read_dataset(monkeypatch):
    def read_from_hdx(dataset_name):
        return Dataset.load_from_json(
            join(
                "tests",
                "fixtures",
                "input",
                f"dataset-{dataset_name}.json",
            )
        )

    monkeypatch.setattr(Dataset, "read_from_hdx", staticmethod(read_from_hdx))


@pytest.fixture(scope="session")
def configuration(config_dir):
    UserAgent.set_global("test")
    Configuration._create(
        hdx_read_only=True,
        hdx_site="prod",
        project_config_yaml=join(config_dir, "project_configuration.yaml"),
    )
    # Change locations below to match those needed in tests
    Locations.set_validlocations(
        [
            {"name": "afg"},
            {"name": "ago"},
            {"name": "aze"},
            {"name": "bdi"},
            {"name": "ben"},
            {"name": "bfa"},
            {"name": "bgd"},
            {"name": "bih"},
            {"name": "bol"},
            {"name": "bra"},
            {"name": "caf"},
            {"name": "chl"},
            {"name": "civ"},
            {"name": "cmr"},
            {"name": "cod"},
            {"name": "col"},
            {"name": "dom"},
            {"name": "ecu"},
            {"name": "egy"},
            {"name": "esp"},
            {"name": "eth"},
            {"name": "fra"},
            {"name": "gnb"},
            {"name": "gnq"},
            {"name": "grc"},
            {"name": "gtm"},
            {"name": "hnd"},
            {"name": "hti"},
            {"name": "ind"},
            {"name": "irn"},
            {"name": "irq"},
            {"name": "isr"},
            {"name": "ita"},
            {"name": "jor"},
            {"name": "ken"},
            {"name": "kgz"},
            {"name": "khm"},
            {"name": "lao"},
            {"name": "lbn"},
            {"name": "lby"},
            {"name": "lso"},
            {"name": "mdg"},
            {"name": "mex"},
            {"name": "mli"},
            {"name": "mmr"},
            {"name": "moz"},
            {"name": "ner"},
            {"name": "nga"},
            {"name": "nic"},
            {"name": "pak"},
            {"name": "per"},
            {"name": "phl"},
            {"name": "png"},
            {"name": "pse"},
            {"name": "rwa"},
            {"name": "sdn"},
            {"name": "sen"},
            {"name": "slb"},
            {"name": "sle"},
            {"name": "slv"},
            {"name": "som"},
            {"name": "ssd"},
            {"name": "sur"},
            {"name": "syr"},
            {"name": "tcd"},
            {"name": "tls"},
            {"name": "tun"},
            {"name": "tur"},
            {"name": "tza"},
            {"name": "uga"},
            {"name": "ukr"},
            {"name": "usa"},
            {"name": "ven"},
            {"name": "yem"},
            {"name": "zaf"},
            {"name": "zwe"},
        ]
    )
    Country.countriesdata(False)
    Vocabulary._approved_vocabulary = {
        "tags": [
            {"name": tag}
            # Change tags below to match those needed in tests
            for tag in (
                "aid worker security",
                "aid workers",
                "complex emergency-conflict-security",
                "conflict-violence",
                "damage assessment",
                "disease",
                "education",
                "education facilities-schools",
                "facilities-infrastructure",
                "fatalities",
                "food security",
                "health",
                "health facilities",
                "indicators",
                "internally displaced persons-idp",
                "OPT-Israel-hostilities",
                "populated places-settlements",
                "refugee crisis",
                "refugees",
            )
        ],
        "id": "b891512e-9516-4bf5-962a-7a289772a2a1",
        "name": "approved",
    }
    return Configuration.read()
