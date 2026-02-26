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
            {"name": "alb"},
            {"name": "are"},
            {"name": "arg"},
            {"name": "arm"},
            {"name": "aus"},
            {"name": "aut"},
            {"name": "aze"},
            {"name": "bdi"},
            {"name": "bel"},
            {"name": "ben"},
            {"name": "bfa"},
            {"name": "bgd"},
            {"name": "bgr"},
            {"name": "bhs"},
            {"name": "bih"},
            {"name": "blr"},
            {"name": "blz"},
            {"name": "bol"},
            {"name": "bra"},
            {"name": "caf"},
            {"name": "can"},
            {"name": "che"},
            {"name": "chl"},
            {"name": "chn"},
            {"name": "civ"},
            {"name": "cmr"},
            {"name": "cod"},
            {"name": "cog"},
            {"name": "col"},
            {"name": "com"},
            {"name": "cri"},
            {"name": "cub"},
            {"name": "cym"},
            {"name": "cyp"},
            {"name": "cze"},
            {"name": "deu"},
            {"name": "dji"},
            {"name": "dom"},
            {"name": "dza"},
            {"name": "ecu"},
            {"name": "egy"},
            {"name": "esp"},
            {"name": "eth"},
            {"name": "fin"},
            {"name": "fra"},
            {"name": "gbr"},
            {"name": "geo"},
            {"name": "gha"},
            {"name": "gin"},
            {"name": "gmb"},
            {"name": "gnb"},
            {"name": "gnq"},
            {"name": "grc"},
            {"name": "gtm"},
            {"name": "guy"},
            {"name": "hnd"},
            {"name": "hrv"},
            {"name": "hti"},
            {"name": "idn"},
            {"name": "ind"},
            {"name": "irn"},
            {"name": "irq"},
            {"name": "isr"},
            {"name": "ita"},
            {"name": "jam"},
            {"name": "jor"},
            {"name": "jpn"},
            {"name": "kaz"},
            {"name": "ken"},
            {"name": "kgz"},
            {"name": "khm"},
            {"name": "kwt"},
            {"name": "lao"},
            {"name": "lbn"},
            {"name": "lbr"},
            {"name": "lby"},
            {"name": "lka"},
            {"name": "lso"},
            {"name": "lux"},
            {"name": "mar"},
            {"name": "mdg"},
            {"name": "mdv"},
            {"name": "mex"},
            {"name": "mkd"},
            {"name": "mli"},
            {"name": "mlt"},
            {"name": "mmr"},
            {"name": "mng"},
            {"name": "moz"},
            {"name": "mus"},
            {"name": "mwi"},
            {"name": "mys"},
            {"name": "ner"},
            {"name": "nga"},
            {"name": "nic"},
            {"name": "nld"},
            {"name": "nor"},
            {"name": "npl"},
            {"name": "nru"},
            {"name": "nzl"},
            {"name": "pak"},
            {"name": "pan"},
            {"name": "per"},
            {"name": "phl"},
            {"name": "png"},
            {"name": "pol"},
            {"name": "prt"},
            {"name": "pry"},
            {"name": "pse"},
            {"name": "rou"},
            {"name": "rus"},
            {"name": "rwa"},
            {"name": "sau"},
            {"name": "sdn"},
            {"name": "sen"},
            {"name": "sgp"},
            {"name": "slb"},
            {"name": "sle"},
            {"name": "slv"},
            {"name": "som"},
            {"name": "srb"},
            {"name": "ssd"},
            {"name": "sur"},
            {"name": "svk"},
            {"name": "swe"},
            {"name": "swz"},
            {"name": "syr"},
            {"name": "tcd"},
            {"name": "tgo"},
            {"name": "tha"},
            {"name": "tjk"},
            {"name": "tls"},
            {"name": "tto"},
            {"name": "tun"},
            {"name": "tur"},
            {"name": "tza"},
            {"name": "uga"},
            {"name": "ukr"},
            {"name": "usa"},
            {"name": "uzb"},
            {"name": "ven"},
            {"name": "vnm"},
            {"name": "xkx"},
            {"name": "yem"},
            {"name": "zaf"},
            {"name": "zmb"},
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
                "children",
                "complex emergency-conflict-security",
                "conflict-violence",
                "damage assessment",
                "disease",
                "education",
                "education facilities-schools",
                "facilities-infrastructure",
                "fatalities",
                "food security",
                "gender-based violence-gbv",
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
