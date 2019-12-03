# -*- coding: utf-8 -*-

import ast
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


FUNCS = {
    "avg": ds_avg,
    "count": ds_count,
    "cusum": ds_cusum,
    "derivative": ds_derivative,
    # "filter": TODO,
    # "if": TODO,
    # "ifnan": TODO,
    "min": ds_min,
    "max": ds_max,
    # "mvavg": TODO,
    "next": ds_next,
    "prev": ds_prev,
    "sum": ds_sum,
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
    tree = ast.parse(expr, mode="eval")
    tree = DatasweetTransformer().visit(tree)
    scope = {}
    for key, value in bucket.items():
        # TODO. Ugly. fix this.
        # count agg are not supported here
        if key.isdigit():
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
