# pybana

[![Build Status](https://travis-ci.org/optimdata/pybana.svg?branch=master)](https://travis-ci.org/optimdata/pybana)
[![codecov](https://codecov.io/gh/optimdata/pybana/branch/master/graph/badge.svg)](https://codecov.io/gh/optimdata/pybana)
![](https://img.shields.io/badge/python-3.6-brightgreen.svg)

- [Github](https://github.com/optimdata/pybana)
- [Documentation](https://pybana.readthedocs.io/en/latest/index.html)

# 🚧 CAREFUL! WORK IN PROGRESS 🚧

## What is this?

This is a kibana client written in python. It provides two kind of utilities
- **An ORM layer**. The goal is to ease the manipulation of kibana objects such as `index-pattern`, `visualization`, `dashboard`. This ORM provides:
  - Modeling using [elasticsearch_dsl](https://elasticsearch-dsl.readthedocs.io/).
  - helpers to extract useful information from kibana objects (ex: the index pattern associated to a visualization).
  - reverse relationships between index-pattern & visualizations, visualizations & dashboards.
- **A translation layer**. The goal is to mimic kibana behaviour in terms of data fetching and visualization rendering. Thus, there are two types of translators:
  - **elastic**. It transforms a kibana `visualization` definition into an elasticsearch query.
  - **vega**. It transforms a kibana `visualization` and data fetched into a [vega](https://vega.github.io/) spec.

## Why?

The ORM was implemented to ease the automatic creation/update of kibana objects. For instance:
- If you've added an access-control layer on top of kibana to handle multi-tenancy, you may want to automate the creation of kibana indexes and the default index-pattern.
- If an `index-pattern` correspond to a table defined somewhere else (like a sql table), you may want to automate the creation of `index-pattern`.
- If a `dashboard` is defined in another database (like a sql db), you may want to delete the kibana object if the sql object is deleted.

The translation layer was implemented to progressively get rid of kibana. Even if kibana is a fantastic tool, it's more meant for internal use than for an integration in another application.

The elastic translator aims to generate almost identical queries to elasticsearch as kibana.

The vega translator tries to provide an equivalent in vega of kibana visualisation. Currently, it supports a limited set of options. Vega was chosen as it provide a complex but almost exhaustive visualization grammar. Vega'sapi allows the rendering of visualizations both on the backend and frontend and has bridges with the main js frameworks (react, vue…).

## Roadmap

- ORM
  - Automatic creation of index pattern
- Elastic translator:
  - Handle more bucket type: ipv4, significatn terms etc
  - Handle more metrics: top hit, sibling etc
- Vega translator:
  - Handle more visualization types (gauge, metric, map etc)
- Versions
  - For now, only elk stack 6.7.1 is handled.

## License

Licensed under MIT license.
