import inspect

from elasticsearch_dsl import Search


class SearchListProxy(list):
    """
    Container for a list of elasticsearc_dsl.Search objects. The class has all methods of Search, calling them on an instance will call the method on each Search object of the list and return a list of all results.
    Example:
        >>> searches = SearchListProxy([Search() for _ in range(10)])
        >>> searches = searches.filter("term", tag="example")
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if len(args) > 0:
            assert all(isinstance(o, Search) for o in args[0])

    def append(self, obj, **kwargs):
        assert isinstance(obj, Search)
        return super().append(obj, **kwargs)


def get_proxy_method(method_name):
    def proxy_method(self, *args, **kwargs):
        ret = [getattr(o, method_name)(*args, **kwargs) for o in self]
        if all(isinstance(o, Search) for o in ret):
            return SearchListProxy(ret)
        return ret

    return proxy_method


for name, method in inspect.getmembers(
    Search,
    predicate=lambda v: inspect.isfunction(v)
    or inspect.ismethod(v)
    or inspect.isdatadescriptor(v),
):
    if not name.startswith("__"):
        setattr(SearchListProxy, name, get_proxy_method(name))


def get_field_arg(agg, field):
    if not field:
        return {"field": agg["params"]["field"]}
    return (
        {"field": field["name"]}
        if not field.get("scripted")
        else {"script": {"source": field["script"], "lang": field["lang"]}}
    )
