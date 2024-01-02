#!/usr/bin/env python
# encoding: utf-8

from hdx_scraper_insecurity_insight.run import update_date_from_string


def test_update_date_from_string():
    dataset_date = "[2020-01-01T00:00:00 TO 2023-10-17T23:59:59]"

    update_date = update_date_from_string(dataset_date)

    assert update_date == "2023-10-17"
