# -*- coding: utf-8 -*-

import ast
import math
import re

__all__ = ("DatasweetTransformer",)


def is_variable(name):
    return bool(re.match("^agg\\d+$", name))


def ds_avg(*args):
    return sum(args) / len(args)


def ds_count(*args):
    return len(args)


def ds_cusum(*args):
    ret = []
    s = 0
    for a in args:
        s += a
        ret.append(s)
    return ret


def ds_derivative(*args):
    prev = float("nan")
    ret = []
    for arg in args:
        ret.append(arg - prev)
        prev = arg
    return ret


def ds_min(*args):
    return min(args)


def ds_max(*args):
    return max(args)


def ds_next(*args):
    return ds_prev(*args[::-1])[::-1]


def ds_prev(*args):
    prev = float("nan")
    ret = []
    for arg in args:
        ret.append(prev)
        prev = arg
    return ret


def ds_sum(*args):
    return sum(args)


def ds_if(cond, yes, no):
    if isinstance(cond, list):
        out = []
        for i, cond_item in enumerate(cond):
            out.append(
                ds_if(
                    cond_item,
                    yes
                    if not isinstance(yes, list)
                    else yes[i]
                    if len(yes) > i
                    else None,
                    no if not isinstance(no, list) else no[i] if len(no) > i else None,
                )
            )
        return out
    try:
        _yes = float(yes)
        return ds_ifnan(_yes if cond else no, no)
    except (ValueError, TypeError):
        return yes if cond else no


def ds_ifnan(arg, default_value):
    if isinstance(arg, list):
        return [ds_ifnan(arg_item, default_value) for arg_item in arg]
    try:
        return default_value if math.isnan(float(arg)) else arg
    except (ValueError, TypeError):
        return default_value


FUNCS = {
    "avg": ds_avg,
    "ceil": math.ceil,
    "count": ds_count,
    "cusum": ds_cusum,
    "derivative": ds_derivative,
    # "filter": TODO,
    "floor": math.floor,
    "_if": ds_if,
    "ifnan": ds_ifnan,
    "min": ds_min,
    "max": ds_max,
    # "mvavg": TODO,
    "next": ds_next,
    "prev": ds_prev,
    "round": round,
    "sum": ds_sum,
    "trunc": math.trunc,
}


class DatasweetTransformer(ast.NodeTransformer):
    """
    Compile a datasweet formula

    - Make sure a only known names are used (for variables & funcs)
    - Rename `aggX` to `bucket["X"]["value"]`
    """

    def visit_Name(self, node):
        self.generic_visit(node)
        if node.id not in FUNCS and not is_variable(node.id):
            raise ValueError(f"{node.id} is not authorized")
        return node


def datasweet_eval(expr, bucket):
    fixed_expr = re.sub(r"([^\w_]*)if\(", r"\1_if(", expr)
    tree = ast.parse(fixed_expr, mode="eval")
    tree = DatasweetTransformer().visit(tree)
    scope = {}
    for key, value in bucket.items():
        if key.isdigit():
            if "hits" in value:
                hits = value["hits"].get("hits", [])
                if len(hits) > 0 and "_source" in value["hits"]["hits"][0]:
                    val = value["hits"]["hits"][0]["_source"].values()[0]
                else:
                    val = None
            else:
                # TODO. Ugly. fix this.
                # count agg are not supported here
                val = (
                    value["std_deviation"]
                    if "std_deviation" in value
                    else value["values"]["50.0"]
                    if "values" in value
                    else value["value"]
                )
            scope[f"agg{key}"] = float("nan") if val is None else val
    try:
        return eval(compile(tree, "a", mode="eval"), FUNCS, scope)
    except ZeroDivisionError:
        return None
