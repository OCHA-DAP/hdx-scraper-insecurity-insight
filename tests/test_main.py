from os.path import basename, join

from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from pandas import read_excel

from hdx.scraper.insecurity_insight.insecurity_insight import InsecurityInsight

_TOPICS = None
_FORCE_REFRESH = False


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
                insecurity_insight = InsecurityInsight(configuration, retriever)

                api_cache = insecurity_insight.fetch_api_responses()
                assert len(api_cache) == 23

                topics_to_update = insecurity_insight.check_for_updates(
                    api_cache, _TOPICS
                )
                assert topics_to_update == [
                    "aidworkerKIKA",
                    "education",
                    "explosiveWeapons",
                    "foodSecurity",
                    "healthcare",
                    "protection",
                    "sv",
                ]

                file_paths = insecurity_insight.refresh_spreadsheets(
                    api_cache,
                    2025,
                    topics_to_update=["aidworkerKIKA"],
                    countries=["AFG"],
                )
                assert len(file_paths) == 9

                for _, file_name in file_paths.items():
                    new_data = read_excel(file_name)
                    old_data = read_excel(join(fixtures_dir, basename(file_name)))
                    assert new_data.equals(old_data)

                datasets = insecurity_insight.update_datasets(
                    api_cache,
                    file_paths,
                    topics_to_update=["aidworkerKIKA"],
                    countries_to_update=["AFG"],
                )
                assert len(datasets) == 2

                dataset = datasets[0]
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                assert dataset == {
                    "name": "sind-aid-worker-kka-dataset",
                    "title": "Aid Worker KIKA (Killed, Injured, Kidnapped or Arrested) Data",
                    "caveats": "Not representative or a comprehensive compilation of all events in which an aid worker was killed, kidnapped, or arrested.  \nKey definitions  \nAid worker: An individual employed by or attached to a humanitarian, UN, international, national, or government aid agency.  \nKilled: Refers to a staff member being killed. Aid worker(s) killed while in captivity are coded as ‘kidnapped’.  \nKidnapped: Refers to a staff member being kidnapped, missing or taken hostage.  \nArrested: Refers to a staff member being arrested, charged, detained, fined or imprisoned.  \nData collection is ongoing and data may change as more information is made available.",
                    "notes": "This dataset contains agency- and publicly-reported data for events in which an [aid worker was killed, injured, kidnapped, or arrested (KIKA)](https://insecurityinsight.org/projects/aid-in-danger/aid-security-digests). Categorized by country.  \n  \nPlease get in touch if you are interested in curated datasets: info@insecurityinsight.org",
                    "license_id": "cc-by-sa",
                    "methodology": "Other",
                    "methodology_other": "Systematically collected from open source, public reports as well as verified submissions from our partner agencies.",
                    "tags": [
                        {
                            "name": "aid worker security",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "aid workers",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "conflict-violence",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "indicators",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "groups": [
                        {"name": "afg"},
                        {"name": "ago"},
                        {"name": "aze"},
                        {"name": "bdi"},
                        {"name": "ben"},
                        {"name": "bfa"},
                        {"name": "bgd"},
                        {"name": "bol"},
                        {"name": "caf"},
                        {"name": "cmr"},
                        {"name": "cod"},
                        {"name": "col"},
                        {"name": "dom"},
                        {"name": "esp"},
                        {"name": "eth"},
                        {"name": "gtm"},
                        {"name": "hti"},
                        {"name": "ind"},
                        {"name": "irn"},
                        {"name": "irq"},
                        {"name": "isr"},
                        {"name": "ken"},
                        {"name": "kgz"},
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
                        {"name": "pak"},
                        {"name": "per"},
                        {"name": "png"},
                        {"name": "pse"},
                        {"name": "rwa"},
                        {"name": "sdn"},
                        {"name": "sen"},
                        {"name": "sle"},
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
                        {"name": "ven"},
                        {"name": "yem"},
                    ],
                    "dataset_date": "[2020-01-02T00:00:00 TO 2025-09-26T23:59:59]",
                    "dataset_source": "Insecurity Insight",
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "972627a5-4f23-4922-8892-371ece6531b6",
                    "owner_org": "insecurity-insight",
                    "subnational": "1",
                    "data_update_frequency": "-2",
                }

                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "2020-2025 Aid Worker KIKA Incident Data.xlsx",
                        "description": "Dataset covering 01 January 2020 to 26 September 2025 on aid workers [killed, injured, kidnapped, or arrested (KIKA)](https://insecurityinsight.org/projects/aid-in-danger/aid-security-digests) based on agency- and open source events. Categorized by country.",
                        "format": "xlsx",
                    },
                    {
                        "name": "2025 Aid Worker KIKA Incident Data.xlsx",
                        "description": "Dataset covering 01 January to 26 September 2025 on aid workers [killed, injured, kidnapped, or arrested (KIKA)](https://insecurityinsight.org/projects/aid-in-danger/aid-security-digests) based on agency- and open source events. Categorized by country.",
                        "format": "xlsx",
                    },
                    {
                        "name": "2016-2025 Aid Worker KIKA Overview Data.xlsx",
                        "description": "Dataset covering 01 January 2020 to 26 September 2025 on aid workers [killed, injured, kidnapped, or arrested (KIKA)](https://insecurityinsight.org/projects/aid-in-danger/aid-security-digests) based on agency- and open source events. Categorized by country and year.",
                        "format": "xlsx",
                    },
                ]

                dataset = datasets[1]
                assert dataset == {
                    "name": "afghanistan-violence-against-civilians-and-vital-civilian-facilities",
                    "title": "Afghanistan (AFG): Attacks on Aid Operations, Education and Health Care, and Explosive Weapons Incident Data",
                    "caveats": "The incidents reported are not a complete nor a representative list of all incidents and have not been independently verified.",
                    "notes": "These datasets contain information on reported incidents of violence and threats affecting aid operations and workers, education and health care services in [Afghanistan](https://insecurityinsight.org/country-pages/afghanistan). They also provide information on incidents of explosive weapons use affecting aid access, education and health care services. Also included are datasets cited in the [Safeguarding Health in Conflict Coalition (SHCC)'s](https://www.safeguardinghealth.org/) annual reports.  \n  \nPlease get in touch if you are interested in curated datasets: info@insecurityinsight.org",
                    "license_id": "cc-by-sa",
                    "methodology": "Other",
                    "methodology_other": "Systematically collected from open source, public reports as well as verified submissions from our partner agencies.",
                    "tags": [
                        {
                            "name": "aid worker security",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "aid workers",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "complex emergency-conflict-security",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "conflict-violence",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "damage assessment",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "disease",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "education",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "education facilities-schools",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "fatalities",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "health",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "health facilities",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "indicators",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "groups": [{"name": "afg"}],
                    "dataset_date": "[2016-06-27T00:00:00 TO 2025-05-07T23:59:59]",
                }

                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "2016-2025-AFG Aid Worker KIKA Incident Data.xlsx",
                        "description": "Dataset covering 22 March 2020 to 30 November 2024 on aid workers [killed, injured, kidnapped, or arrested (KIKA)](https://insecurityinsight.org/projects/aid-in-danger/aid-security-digests) based on agency- and open source events.",
                        "format": "xlsx",
                    },
                    {
                        "name": "2020-2025-AFG Education in Danger Incident Data.xlsx",
                        "description": "Dataset covering 07 February 2021 to 07 May 2025 on [attacks on education](https://insecurityinsight.org/projects/education-in-danger/education-in-danger-monthly-news-brief) based on agency- and open source events.",
                        "format": "xlsx",
                    },
                    {
                        "name": "2018-2024-AFG Explosive Weapons Incident Data.xlsx",
                        "description": "Dataset covering 16 March 2021 to 29 March 2023 on incidents in which aid access, education and healthcare services were impacted by [explosive weapons](https://insecurityinsight.org/projects/explosive-weapons) based on agency- and open source events.",
                        "format": "xlsx",
                    },
                    {
                        "name": "2016-2025-AFG Attacks on Health Care Incident Data.xlsx",
                        "description": "Dataset covering 27 June 2016 to 11 October 2024 on [attacks on health care](https://insecurityinsight.org/projects/healthcare/monthlynewsbrief) based on agency- and open source events.",
                        "format": "xlsx",
                    },
                ]
