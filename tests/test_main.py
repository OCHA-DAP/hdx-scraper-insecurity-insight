from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.insecurity_insight.insecurity_insight import (
    InsecurityInsight,
    parse_dates_from_string,
)


class TestInsecurityInsight:
    def test_insecurity_insight(
        self, configuration, fixtures_dir, input_dir, config_dir
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

                api_cache = insecurity_insight.fetch_and_cache_api_responses()
                assert len(api_cache) == 21

                dataset_cache = insecurity_insight.fetch_and_cache_datasets()
                assert len(dataset_cache) == 32

                # compare_api_to_samples is tested, effectively in
                # test_generate_api_transformation_schema.test_compare_api_to_samples_changed
                # assert True

                dataset_cache = {
                    "insecurity-insight-healthcare-dataset": {
                        "dataset_date": "[2020-01-01T00:00:00 TO 2023-10-17T23:59:59]"
                    }
                }
                api_cache = {
                    "insecurity-insight-healthcare-incidents": [
                        {
                            "Date": "2024-03-06T00:00:00.000Z",
                            "Country ISO": "UKR",
                        },
                        {
                            "Date": "2024-03-03T00:00:00.000Z",
                            "Country ISO": "UKR",
                        },
                    ]
                }
                items_to_update = (
                    insecurity_insight.decide_which_resources_have_fresh_data(
                        dataset_cache,
                        api_cache,
                        refresh=[],
                        dataset_list=["insecurity-insight-healthcare-dataset"],
                        resource_list=["insecurity-insight-healthcare-incidents"],
                        topic_list=["healthcare"],
                    )
                )
                assert items_to_update == [("healthcare", "2024-03-03", "2024-03-06")]

                api_cache = {
                    "insecurity-insight-healthcare-incidents": [
                        {
                            "Date": "2023-10-17T00:00:00.000Z",
                            "Country ISO": "UKR",
                        },
                        {
                            "Date": "2019-01-01T00:00:00.000Z",
                            "Country ISO": "UKR",
                        },
                    ]
                }
                items_to_update = (
                    insecurity_insight.decide_which_resources_have_fresh_data(
                        dataset_cache,
                        api_cache,
                        refresh=[],
                        dataset_list=["insecurity-insight-healthcare-dataset"],
                        resource_list=["insecurity-insight-healthcare-incidents"],
                        topic_list=["healthcare"],
                    )
                )
                print(items_to_update, flush=True)
                assert items_to_update == [("healthcare", "2019-01-01", "2023-10-17")]

                dataset_date = "[2020-01-01T00:00:00 TO 2023-10-17T23:59:59]"
                start_date, end_date = parse_dates_from_string(dataset_date)
                assert start_date == "2020-01-01"
                assert end_date == "2023-10-17"
