from os.path import basename, join

from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from pandas import read_excel

from hdx.scraper.insecurity_insight.api_reader import APIReader
from hdx.scraper.insecurity_insight.dataset_generator import DatasetGenerator
from hdx.scraper.insecurity_insight.spreadsheet_creator import SpreadsheetCreator


class TestInsecurityInsight:
    def test_insecurity_insight(
        self, configuration, read_dataset, fixtures_dir, input_dir, config_dir
    ):
        with temp_dir(
            "TestInsecurityInsight",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                api_reader = APIReader(configuration, retriever)
                api_cache = api_reader.fetch_api_responses()
                assert len(api_cache) == 32
                spreadsheet_creator = SpreadsheetCreator(
                    configuration, retriever, api_cache
                )
                topics = {
                    "children": "Children in Armed Conflict",
                    "sv": {
                        "CRSV": "Conflict-Related Sexual Violence (CRSV)",
                        "SVPoliticalViolence": "Political-Related Sexual Violence",
                        "overview": "Conflict-Related (CRSV) or Political-Related Sexual Violence",
                    },
                }

                file_paths = spreadsheet_creator.refresh_spreadsheets_with_fresh_data(
                    2025,
                    countries=["SDN"],
                    topics_to_update=topics,
                )
                assert len(file_paths) == 11

                for _, file_name in file_paths.items():
                    new_data = read_excel(file_name)
                    old_data = read_excel(join(fixtures_dir, basename(file_name)))
                    assert new_data.equals(old_data)

                dataset_generator = DatasetGenerator(
                    configuration, api_cache, file_paths
                )
                datasets = dataset_generator.get_datasets()

                assert len(datasets) == 40

                dataset = datasets[3]
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                assert dataset == {
                    "caveats": "Not representative or a comprehensive compilation of all events "
                    "affecting the provision of healthcare. Data collection is ongoing "
                    "and data may change as more information is made available.",
                    "data_update_frequency": "-2",
                    "dataset_date": "[2016-01-01T00:00:00 TO 2025-12-31T23:59:59]",
                    "dataset_source": "Insecurity Insight",
                    "groups": [
                        {"name": "afg"},
                        {"name": "ago"},
                        {"name": "aze"},
                        {"name": "bdi"},
                        {"name": "bfa"},
                        {"name": "bgd"},
                        {"name": "bhs"},
                        {"name": "bra"},
                        {"name": "caf"},
                        {"name": "cmr"},
                        {"name": "cod"},
                        {"name": "col"},
                        {"name": "dom"},
                        {"name": "dza"},
                        {"name": "ecu"},
                        {"name": "egy"},
                        {"name": "eth"},
                        {"name": "gbr"},
                        {"name": "gha"},
                        {"name": "hnd"},
                        {"name": "hti"},
                        {"name": "idn"},
                        {"name": "ind"},
                        {"name": "irn"},
                        {"name": "irq"},
                        {"name": "isr"},
                        {"name": "kaz"},
                        {"name": "ken"},
                        {"name": "kgz"},
                        {"name": "kwt"},
                        {"name": "lbn"},
                        {"name": "lby"},
                        {"name": "lka"},
                        {"name": "mdg"},
                        {"name": "mex"},
                        {"name": "mli"},
                        {"name": "mlt"},
                        {"name": "mmr"},
                        {"name": "moz"},
                        {"name": "nga"},
                        {"name": "npl"},
                        {"name": "pak"},
                        {"name": "per"},
                        {"name": "pse"},
                        {"name": "rus"},
                        {"name": "rwa"},
                        {"name": "sdn"},
                        {"name": "sle"},
                        {"name": "som"},
                        {"name": "ssd"},
                        {"name": "syr"},
                        {"name": "tcd"},
                        {"name": "tza"},
                        {"name": "uga"},
                        {"name": "ukr"},
                        {"name": "usa"},
                        {"name": "uzb"},
                        {"name": "yem"},
                        {"name": "zaf"},
                        {"name": "zmb"},
                    ],
                    "license_id": "cc-by-sa",
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "methodology": "Other",
                    "methodology_other": "Systematically collected from open source using the "
                    "SiND.",
                    "name": "children-in-armed-conflict-data",
                    "notes": "This page contains agency- and publicly-reported data for events "
                    "affecting children and children-related services in conflict zones. "
                    "Based on agency- and open source events. Categorized by country.",
                    "owner_org": "648d346e-3995-44cc-a559-29f8192a3010",
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "children",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "conflict-violence",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Conflict Incidents Affecting Children and Children-Related Services "
                    "Incident Data",
                }

                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "2016-2025 Children in Armed Conflict Incident Data.xlsx",
                        "description": "Resource covering 01 January 2024 to 31 December 2024 on incidents affecting children and children-related services in conflict zones. Based on agency- and open source events. Categorized by country.",
                        "format": "xlsx",
                    },
                    {
                        "name": "2025 Children in Armed Conflict Incident Data.xlsx",
                        "description": "Resource covering 01 January to 31 December 2024 on incidents affecting children and children-related services in conflict zones. Based on agency- and open source events. Categorized by country.",
                        "format": "xlsx",
                    },
                    {
                        "name": "2016-2025 Children in Armed Conflict Overview Data.xlsx",
                        "description": "Resource covering 01 January 2024 to 31 December 2025 on incidents affecting children and children-related services in conflict zones. Based on agency- and open source events. Categorized by country and year.",
                        "format": "xlsx",
                    },
                ]

                dataset = datasets[5]
                assert dataset == {
                    "caveats": "The incidents reported are not a complete nor a representative list of all incidents and have not been independently verified.",
                    "dataset_date": "[2020-01-01T00:00:00 TO 2025-12-31T23:59:59]",
                    "groups": [
                        {"name": "afg"},
                        {"name": "ago"},
                        {"name": "bdi"},
                        {"name": "ben"},
                        {"name": "bfa"},
                        {"name": "bgd"},
                        {"name": "bgr"},
                        {"name": "bhs"},
                        {"name": "blz"},
                        {"name": "bra"},
                        {"name": "caf"},
                        {"name": "can"},
                        {"name": "chl"},
                        {"name": "chn"},
                        {"name": "civ"},
                        {"name": "cmr"},
                        {"name": "cod"},
                        {"name": "col"},
                        {"name": "com"},
                        {"name": "cri"},
                        {"name": "cub"},
                        {"name": "dom"},
                        {"name": "egy"},
                        {"name": "eth"},
                        {"name": "fra"},
                        {"name": "gha"},
                        {"name": "gin"},
                        {"name": "gnq"},
                        {"name": "guy"},
                        {"name": "hnd"},
                        {"name": "hti"},
                        {"name": "idn"},
                        {"name": "ind"},
                        {"name": "irn"},
                        {"name": "irq"},
                        {"name": "isr"},
                        {"name": "jam"},
                        {"name": "kaz"},
                        {"name": "ken"},
                        {"name": "kgz"},
                        {"name": "lbn"},
                        {"name": "lby"},
                        {"name": "lka"},
                        {"name": "lso"},
                        {"name": "mar"},
                        {"name": "mdg"},
                        {"name": "mex"},
                        {"name": "mkd"},
                        {"name": "mli"},
                        {"name": "mmr"},
                        {"name": "moz"},
                        {"name": "mwi"},
                        {"name": "mys"},
                        {"name": "ner"},
                        {"name": "nga"},
                        {"name": "nic"},
                        {"name": "npl"},
                        {"name": "nzl"},
                        {"name": "pak"},
                        {"name": "pan"},
                        {"name": "per"},
                        {"name": "phl"},
                        {"name": "png"},
                        {"name": "pry"},
                        {"name": "pse"},
                        {"name": "rus"},
                        {"name": "rwa"},
                        {"name": "sau"},
                        {"name": "sdn"},
                        {"name": "slv"},
                        {"name": "som"},
                        {"name": "ssd"},
                        {"name": "swz"},
                        {"name": "syr"},
                        {"name": "tcd"},
                        {"name": "tha"},
                        {"name": "tun"},
                        {"name": "tur"},
                        {"name": "tza"},
                        {"name": "uga"},
                        {"name": "ukr"},
                        {"name": "uzb"},
                        {"name": "ven"},
                        {"name": "yem"},
                        {"name": "zaf"},
                        {"name": "zmb"},
                        {"name": "zwe"},
                    ],
                    "license_id": "cc-by",
                    "methodology": "Other",
                    "methodology_other": "Systematically collected from open source, public reports as well as verified submissions from our partner agencies.",
                    "name": "conflict-related-sexual-violence",
                    "notes": "This page contains publicly-reported cases of conflict-related and political [sexual violence](https://insecurityinsight.org/projects/reporting-sexual-violence-and-abuse-in-conflict-settings) by conflict actors, security personnel, and sexual violence violence that targets aid workers, educators, health workers and IDPS/refugees. Please get in touch if you are interested in curated Resources: info@insecurityinsight.org",
                    "tags": [
                        {
                            "name": "conflict-violence",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "gender-based violence-gbv",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Conflict-Related (CRSV) or Political-Related Sexual Violence Data",
                }

                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "2020-2025 Conflict-Related Sexual Violence (CRSV) Incident Data.xlsx",
                        "description": "Resource covering 01 January 2020 to 30 October 2025 on incidents of [sexual violence](https://insecurityinsight.org/projects/reporting-sexual-violence-and-abuse-in-conflict-settings) perpetrated by conflict actors. Categorised by country.",
                        "format": "xlsx",
                    },
                    {
                        "name": "2025 Conflict-Related Sexual Violence (CRSV) Incident Data.xlsx",
                        "description": "Resource covering 01 January to 30 October 2025 on incidents of [sexual violence](https://insecurityinsight.org/projects/reporting-sexual-violence-and-abuse-in-conflict-settings) perpetrated by conflict actors. Categorised by country.",
                        "format": "xlsx",
                    },
                    {
                        "name": "2020-2025 Political-Related Sexual Violence Incident Data.xlsx",
                        "description": "Resource covering 01 January 2020 to 14 September 2025 on incidents of [sexual violence](https://insecurityinsight.org/projects/reporting-sexual-violence-and-abuse-in-conflict-settings) perpetrated by police or state security personnel. Categorised by country.",
                        "format": "xlsx",
                    },
                    {
                        "name": "2025 Political-Related Sexual Violence Incident Data.xlsx",
                        "description": "Resource covering 01 January to 14 September 2025 on incidents of [sexual violence](https://insecurityinsight.org/projects/reporting-sexual-violence-and-abuse-in-conflict-settings) perpetrated by police or state security personnel. Categorised by country.",
                        "format": "xlsx",
                    },
                    {
                        "name": "2020-2025 Conflict-Related (CRSV) or Political-Related Sexual Violence Overview Data.xlsx",
                        "description": "Resource covering 01 January 2020 to 31 December 2025 on incidents of [sexual violence](https://insecurityinsight.org/projects/reporting-sexual-violence-and-abuse-in-conflict-settings) perpetrated by conflict actors, security personnel, and sexual violence that targets aid workers, educators, health workers and IDPs/refugees. Categorised by country and year.",
                        "format": "xlsx",
                    },
                ]
