#!/usr/bin/env python
# encoding: utf-8

from hdx_scraper_insecurity_insight.read_insecurity_insight_hdx import (
    create_insecurity_insight_resource_list,
)


def test_create_insecurity_insight_resource_list():
    n_datasets, n_resources = create_insecurity_insight_resource_list()

    assert n_datasets == 38
