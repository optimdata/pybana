# -*- coding: utf-8 -*-

import json
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search


def dumpindex(elastic, index, fn):
    """
    Helper which dump a whole db and returns in a format handled by bulk api
    """
    search = Search(using=elastic, index=index).filter(
        "terms", type=["index-pattern", "visualization", "dashboard", "search"]
    )
    with open(fn, "w+") as fd:
        for doc in search.scan():
            fd.write(
                json.dumps(
                    {
                        "_index": doc.meta.index,
                        "_type": doc.meta.doc_type,
                        "_id": doc.meta.id,
                        "_source": doc.to_dict(),
                    }
                )
            )
            fd.write("\n")


if __name__ == "__main__":
    import os

    dumpindex(
        Elasticsearch(),
        ".kibana_pybana",
        os.path.join(os.path.dirname(__file__), "index.json"),
    )
