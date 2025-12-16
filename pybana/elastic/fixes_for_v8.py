import copy
import logging
import json
from typing import Iterator, Dict, List, Any, Optional, Tuple, Union
from elasticsearch.client import Transport
from elasticsearch.exceptions import TransportError

logger = logging.getLogger("elasticsearch")


def get_sub_dict(origin_dict: Dict, sub_steps: List[str]) -> Dict:
    current_dict = origin_dict
    for step in sub_steps:
        new_dict = current_dict.get(step)
        if new_dict is None:
            current_dict[step] = {}
        else:
            assert isinstance(new_dict, dict)
            pass
        current_dict = current_dict[step]
    return current_dict


ALLOWED_MAPPINGS_KEYS = set(
    (
        "date_detection|dynamic|dynamic_date_formats|dynamic_templates|_field_names|_meta"
        "numeric_detection|properties|_routing|_source|runtime"
    ).split("|")
)


def fusion_list(old_list: List, new_list: List, override: bool) -> List:
    old_keys = {v["key"]: v for v in old_list if isinstance(v, dict) and "key" in v}
    new_keys = {v["key"]: v for v in new_list if isinstance(v, dict) and "key" in v}
    if len(new_keys) != len(new_list) or len(old_keys) != len(old_list):
        return new_list if override else old_list
    final_list = []
    for v in old_list:
        k = v["key"]
        final_list.append(new_keys[k] if override and k in new_keys else v)
    for v in new_list:
        k = v["key"]
        if k not in old_keys:
            final_list.append(v)
    return final_list


def fusion_mappings(mappings: Dict[str, Any], key: str, override=True):
    tmp = mappings.get(key)
    if not tmp:
        return
    if not isinstance(tmp, dict):
        raise RuntimeError(
            f"supplementary keys must be dict, not this mappings[{key}]={tmp.__class__.__name__}/{tmp}"
        )
    for k, v in tmp.items():
        if k in ALLOWED_MAPPINGS_KEYS:
            prev = mappings.get(k)
            if prev is None:
                mappings[k] = v
            elif isinstance(v, dict) and isinstance(prev, dict):
                if not override:
                    v.update(prev)
                prev.update(v)
            elif isinstance(v, list) and isinstance(prev, list):
                mappings[k] = fusion_list(prev, v, override=override)
            elif override:
                mappings[k] = v
    del mappings[key]


HISTOGRAMS_CALENDARS = set(
    [
        "minute",
        "1m",
        "hour",
        "1h",
        "day",
        "1d",
        "week",
        "1w",
        "month",
        "1M",
        "quarter",
        "1q",
        "year",
        "1y",
    ]
)


class V8ToV6:
    def correct_mappings(
        self, original_mappings: Optional[Dict], doc_type
    ) -> Optional[Dict]:
        if not original_mappings:
            return original_mappings
        for v in original_mappings.values():
            if isinstance(v, dict):
                self.correct_mapping(v, doc_type=doc_type)
        return original_mappings

    def correct_mapping(
        self, original_mappings: Optional[Dict], doc_type
    ) -> Optional[Dict]:
        if not original_mappings:
            return original_mappings
        mappings = original_mappings.get("mappings")
        if mappings and isinstance(mappings, dict):
            if "dynamic_templates" in mappings:
                mappings["_default_"] = {
                    "dynamic_templates": mappings.pop("dynamic_templates")
                }
            if doc_type and isinstance(doc_type, str) and "properties" in mappings:
                mappings[doc_type] = {"properties": mappings.pop("properties")}
        return original_mappings

    @staticmethod
    def correct_search_result(results: Optional[Dict], doc_type) -> Dict:
        if not results:
            return {}
        hits = results.get("hits")
        if hits and isinstance(hits, dict):
            if isinstance(hits.get("total", -1), dict):
                total = hits.get("total")
                if isinstance(total, dict) and isinstance(total.get("value"), int):
                    hits["total_v8"] = total
                    hits["total"] = total.get("value")
            rows = hits.get("hits", -1)
            if isinstance(rows, list):
                for hit in rows:
                    if doc_type and isinstance(hit, dict) and "_type" not in hit:
                        hit["_type"] = doc_type
        return results


def _is_calendar_interval(interval: str) -> bool:
    if not isinstance(interval, str):
        return False
    return interval in HISTOGRAMS_CALENDARS


class V6ToV8:
    def fix_transport_instance(self, transport: Transport):
        if hasattr(transport, "_perform_request_v8"):
            print("Transport already fixed for v8")
            return

        def new_perform_request(method, url, headers=None, params=None, body=None):
            try:
                try:
                    #print(f"new_perform_request: method={method}, url={url}, params:{params}, body:{body}")
                    return transport._perform_request_v8(  # type: ignore
                        method=method, url=url, headers=headers, params=params, body=body
                    )
                except TransportError as e:
                    if len(e.args) > 2:
                        e.args = v6_to_v8.fix_transport_error_args(e.args)
                    raise
            except Exception as e:
                print(f"ERROR in new_perform_request: method={method}, url={url}, params:{params}, body:{body}: {e}, hosts: {transport.hosts}")
                raise

        _perform_request_v8 = transport.perform_request
        transport._perform_request_v8 = _perform_request_v8  # type: ignore
        transport.perform_request = new_perform_request

    def fix_dynamic_template(self, dynamic: Dict) -> bool:
        changed = False
        if not dynamic:
            return False
        for v in dynamic.values():
            if not isinstance(v, dict):
                continue
            if v.get("match_mapping_type", "") != "string":
                continue
            mapping = v.get("mapping")
            if not mapping:
                continue
            if not isinstance(mapping, dict):
                continue
            if (
                str(mapping.get("index")) == "not_analyzed"
                and str(mapping.get("type")) == "string"
                and str(mapping.get("omit_norms")) == str(True)
            ):
                del mapping["omit_norms"]
                mapping["norms"] = False
                mapping["index"] = False
                mapping["type"] = "keyword"
                changed = True
        return changed

    def fix_dynamic_templates(self, dynamics: Optional[List[Dict]]):
        if not dynamics:
            return False
        if not isinstance(dynamics, list):
            return False
        changed = False
        for dynamic in dynamics:
            if self.fix_dynamic_template(dynamic):
                changed = True
        return changed

    def fix_mappings(self, original_mappings: Optional[Dict]) -> Dict:
        if not original_mappings:
            return {}
        keys = [k for k in original_mappings if k not in ALLOWED_MAPPINGS_KEYS]
        if not keys:
            return original_mappings

        mappings: Dict = json.loads(json.dumps(original_mappings))
        for key in keys:
            fusion_mappings(mappings=mappings, key=key, override=(key != "_default_"))
        self.fix_dynamic_templates(mappings.get("dynamic_templates"))
        return mappings

    def fix_template(self, template: Optional[Dict]) -> Dict:
        if not template:
            return {}
        mappings_n = "mappings"
        if mappings_n in template:
            template[mappings_n] = self.fix_mappings(template[mappings_n])
        get_sub_dict(template, ["settings", "index"])["codec"] = "best_compression"
        return template

    def _remove_type(self, action):
        if isinstance(action, list):
            for v in action:
                self._remove_type(v)
        elif isinstance(action, dict):
            if "_type" in action:
                del action["_type"]
            for v in action.values():
                self._remove_type(v)

    def fix_actions(
        self, origin_actions: Union[Iterator[Dict], List[Dict], None]
    ) -> Iterator[Dict]:
        if not origin_actions:
            return iter([])
        for action in origin_actions:
            self._remove_type(action)
            yield action

    def fix_histogram(self, params: Union[List, Dict]):
        if isinstance(params, list):
            for v in params:
                if isinstance(v, list) or isinstance(v, dict):
                    self.fix_histogram(v)
        elif isinstance(params, dict):
            for k, v in params.items():
                if k == "date_histogram" and isinstance(v, dict) and "interval" in v:
                    # cf https://www.elastic.co/docs/reference/aggregations/search-aggregations-bucket-datehistogram-aggregation
                    interval = v["interval"]
                    v[
                        "calendar_interval"
                        if _is_calendar_interval(interval)
                        else "fixed_interval"
                    ] = v["interval"]
                    del v["interval"]
                elif isinstance(v, dict) or isinstance(v, list):
                    self.fix_histogram(v)

    def fix_search_body(
        self, params: Optional[Union[Dict, str]]
    ) -> Optional[Union[Dict, str]]:
        if not params:
            return params
        try:
            to_change = json.loads(params) if isinstance(params, str) else params
        except (ValueError, TypeError):
            return params
        if not isinstance(to_change, dict):
            return params
        changed = copy.deepcopy(to_change)
        if "doc_type" in changed:
            del changed["doc_type"]
        self.fix_histogram(changed)
        if changed == to_change:
            return params
        return (
            changed
            if not isinstance(params, str)
            else json.dumps(changed, sort_keys=True)
        )

    def fix_search_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        # for the moment, only those params found, will probably add more rules later
        # e.g same than for actions partss...
        for origin, dest in [
            ("_source_exclude", "_source_excludes"),
            ("_source_include", "_source_includes"),
        ]:
            if origin in params and dest not in params:
                params[dest] = params.pop(origin)
        return params

    @staticmethod
    def fix_transport_error_args(args: Tuple) -> Tuple:
        if len(args) <= 2:
            return args
        arg2 = args[2]
        reason = arg2
        if isinstance(arg2, dict) and "error" in arg2:
            reason = arg2["error"]
            if isinstance(reason, dict):
                root_causes = reason.get("root_cause")
                if (
                    root_causes
                    and isinstance(root_causes, list)
                    and isinstance(root_causes[0], dict)
                    and "reason" in root_causes[0]
                ):
                    return args

        arg2 = {"error": {"root_cause": [{"reason": reason}]}}
        return (args[0], args[1], arg2)


v6_to_v8 = V6ToV8()
v8_to_v6 = V8ToV6()
