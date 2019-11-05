# Translate visualization to elastic queries

This package tries to reimplement the translation of a `Visualization` given a context.

## Usage

```python
import datetime
import pytz
from pybana import Kibana, ElasticTranslator, Context

# Create a Kibana instance
kibana = Kibana()
visualization = kibana.visualization(id="7b12e580-dae6-11e9-94be-2b2f7d5f3e45")

end = datetime.datetime.now()
beg = end - datetime.timedelta(days=7)

# Create a context
context = Context(beg, end, pytz.UTC)

# Create the search
search = ElasticTranslator().translate(visualization, context)

# Search is an elasticsearch_dsl.Search object. Then, you can do execute the query
response = search.execute()
```

## Known limits

Several buckets or metrics have not yet been implemented.
- Buckets:
    - Ipv4 range.
    - Significant terms.
    - Terms: Group other values in separate bucket.
    - Order by custom metric.
- Metrics:
    - Top hit.
    - Sibling pipeline aggregations.
    - Parent pipeline aggregations.
