# Using the ORM

`pybana` provides a simple [ORM](https://en.wikipedia.org/wiki/Object-relational_mapping) for manipulating kibana saved objects.

The ORM was implemented to ease the automatic creation/update of kibana objects. For instance:
- If you've added an access-control layer on top of kibana to handle multi-tenancy, you may want to automate the creation of kibana indexes and the default index-pattern.
- If an `index-pattern` correspond to a table defined somewhere else (like a sql table), you may want to automate the creation of `index-pattern`.
- If a `dashboard` is defined in another database (like a sql db), you may want to delete the kibana object if the sql object is deleted.


## Initializing kibana

A kibana server instance performs several checks when it starts:

1. Create if it does not exists a `.kibana` index on elasticsearch. `pybana` does not mimic this behaviour.
2. Create a `Config` document.
    - This document has the following id: `config:${ELASTICSEARCH_VERSION}` (example: `config:6.7.1`)
    - It contains a `config` field which stores:
        - `defaultIndex`. The identifier of the default index
        - All the settings you can configure in the "Advanced settings" menu. The [official documentation](https://www.elastic.co/guide/en/kibana/current/advanced-options.html) provide a full list of available options.
    - You may create programmatically this document using the `Kibana.init_config` api.
3. Configure the default `index-pattern`. To do it programmatically, you can use the `Kibana.update_or_create_default_index_pattern` api.

## Models

For now, four models have been implemented:
- `IndexPattern`
- `Search`
- `Visualization`
- `Dashboard`


## Usage

```python
from elasticsearch import Elasticsearch
from elasticsearch_dsl import connections
from pybana import Kibana, Visualization, Dashboard

# Instantiate a connection to a Elasticsearch cluster
elastic = Elasticsearch()

# Add this connection as the default connection for elasticsearch_dsl
connections.add_connection("default", elastic)

# Instantiate a kibana client to the default index
kibana = Kibana()

# Instantiate a kibana client to a custom index
kibana = Kibana(".kibana_tenant1")

# Init config (does nothing if the config is already here)
kibana.init_config()

# Init config (does nothing if the config is already here)
kibana.init_config()

# Search for dashboards
dashboards = kibana.dashboards()
# Here dashboards is only have a `elasticsearch_dsl.Search`

# Iterate over the first 10 dashboards
for dashboard in dashboards:
    pass

# Iterate over all the dashboards
for dashboard in dashboards.scan():
    pass

# Get one dashboard
dashboard = kibana.dashboard("7b12e580-dae6-11e9-94be-2b2f7d5f3e45")

# Fetch all the associated visualization to this dashboard
visualizations = dashboard.visualizations()

# Get one visualization
visualization = visualizations[0]

# Deserialize the visState
visualization.state()

# Get the search associated to the visualization (raise an error if ther's not)
search = visualization.related_search()

# Get the index pattern associated to a visualization (go through the search if there's one)
index_pattern = visualization.index()

# Get the index pattern associated to a search
index_pattern = search.index()

```
