# -*- coding: utf-8 -*-

import datetime
import json
import math
import os
import sys
import unittest
from unittest.mock import Mock, patch

BASE_DIRECTORY = os.path.join(os.path.dirname(__file__), "..")  # NOQA
sys.path.insert(0, BASE_DIRECTORY)  # NOQA

import pendulum  # noqa: E402
import pytest  # noqa: E402

import pybana.helpers as helpers  # noqa: E402
from pybana.helpers.datasweet import (  # noqa: E402
    DatasweetTransformer,
    datasweet_eval,
    ds_avg,
    ds_count,
    ds_cusum,
    ds_derivative,
    ds_if,
    ds_ifnan,
    ds_max,
    ds_min,
    ds_next,
    ds_prev,
    ds_sum,
    is_variable,
)
from pybana.helpers.datetime import (  # noqa: E402
    UnknownMomentTokenError,
    convert,
    format_timestamp,
    get_scaled_date_format,
)
from pybana.helpers.math import percentage  # noqa: E402
from pybana.helpers.vega import (  # noqa: E402
    RO_FORMAT_LOCALE,
    FallbackVegaRenderer,
    InvalidVegaSpecException,
    VegaRenderer,
)

TS = 1546300800000


class Config:
    def __init__(self, data):
        self.config = self
        self._data = data

    def to_dict(self):
        return self._data


class TestMath(unittest.TestCase):
    def test_percentage(self):
        self.assertEqual(percentage(1, 4), 25)
        self.assertEqual(percentage(1, 0), 0)
        self.assertEqual(percentage(1, None), 0)
        self.assertIs(helpers.percentage, percentage)


class TestDatetime(unittest.TestCase):
    def test_convert_moment_tokens(self):
        self.assertEqual(convert(""), "")
        self.assertEqual(convert("YYYY-MM-DD"), "YYYY-MM-DD")
        self.assertEqual(convert("llll"), "LLLL")
        self.assertEqual(convert("[kept]"), "[kept]")
        self.assertEqual(convert("Y [kept]"), "Y [kept]")
        self.assertEqual(convert("[kept] Y"), "[kept] Y")
        self.assertEqual(convert("Y [kept] MM"), "Y [kept] MM")
        self.assertEqual(convert("[a [nested]]"), "[a [nested]]")
        self.assertIsNone(convert("w"))
        with pytest.raises(UnknownMomentTokenError):
            convert("w", ignore=False)

    def test_format_timestamp(self):
        self.assertTrue(format_timestamp(TS).startswith("2019-01-01"))
        self.assertEqual(format_timestamp(TS, "YYYY-MM-DD", "en"), "2019-01-01")
        self.assertEqual(
            format_timestamp(TS, "YYYY-MM-DD", "not-a-locale"), "2019-01-01"
        )

    def test_scaled_date_format(self):
        matched = get_scaled_date_format(
            Config(
                {
                    "dateFormat": "default",
                    "dateFormat:scaled": json.dumps([["P1D", "day"], ["P1Y", "year"]]),
                }
            ),
            pendulum.parse("P2D"),
        )
        self.assertEqual(matched, "day")
        fallback = get_scaled_date_format(
            Config(
                {
                    "dateFormat": "default",
                    "dateFormat:scaled": json.dumps([["P1Y", "year"]]),
                }
            ),
            pendulum.parse("PT1S"),
        )
        self.assertEqual(fallback, "default")
        empty_duration = get_scaled_date_format(
            Config(
                {
                    "dateFormat": "default",
                    "dateFormat:scaled": json.dumps(
                        [["", "fallback"], ["P1Y", "year"]]
                    ),
                }
            ),
            pendulum.parse("PT1S"),
        )
        self.assertEqual(empty_duration, "fallback")
        self.assertEqual(
            get_scaled_date_format(Config({}), datetime.timedelta(0)), None
        )
        self.assertIs(helpers.format_timestamp, format_timestamp)


class TestDatasweet(unittest.TestCase):
    def test_scalar_functions(self):
        self.assertEqual(ds_avg(0, 1), 0.5)
        self.assertEqual(ds_count(0, 1), 2)
        self.assertEqual(ds_cusum(0, 1), [0, 1])
        derivative = ds_derivative(0, 1)
        self.assertTrue(math.isnan(derivative[0]))
        self.assertEqual(derivative[1], 1)
        self.assertEqual(ds_max(0, 1), 1)
        self.assertEqual(ds_min(0, 1), 0)
        nxt = ds_next(0, 1)
        self.assertEqual(nxt[0], 1)
        self.assertTrue(math.isnan(nxt[1]))
        prev = ds_prev(0, 1)
        self.assertTrue(math.isnan(prev[0]))
        self.assertEqual(prev[1], 0)
        self.assertEqual(ds_sum(0, 1), 1)
        self.assertTrue(is_variable("agg12"))
        self.assertFalse(is_variable("agg"))
        self.assertFalse(is_variable("xagg1"))

    def test_conditionals(self):
        self.assertEqual(ds_if(1, 2, 0), 2)
        self.assertEqual(ds_if(0, 3, 4), 4)
        self.assertEqual(ds_if(1, float("nan"), 7), 7)
        self.assertTrue(math.isnan(ds_if(0, 2.0, float("nan"))))
        self.assertEqual(ds_if(1, "yes", "no"), "yes")
        self.assertEqual(ds_if(0, "yes", "no"), "no")
        self.assertEqual(ds_if([1, 0, 1], "a", "b"), ["a", "b", "a"])
        self.assertEqual(
            ds_if([1, 0, 1], ["a", "a", "a"], ["b", "b", "b"]), ["a", "b", "a"]
        )
        self.assertEqual(ds_if([1, 0], ["yes"], "no"), ["yes", "no"])
        self.assertEqual(ds_if([1, 0], "yes", ["no"]), ["yes", None])
        self.assertEqual(ds_ifnan(1, 0), 1)
        self.assertEqual(ds_ifnan("1", 0), "1")
        self.assertEqual(ds_ifnan(float("nan"), 0), 0)
        self.assertEqual(ds_ifnan("bad", 0), 0)
        self.assertEqual(ds_ifnan([[float("nan")], 1], 0), [[0], 1])

    def test_eval_bucket_shapes(self):
        self.assertEqual(
            datasweet_eval(
                "avg(agg1, agg2) + 1", {"1": {"value": 0}, "2": {"value": 1}}
            ),
            1.5,
        )
        self.assertEqual(datasweet_eval("sum(1, 2)", {}), 3)
        self.assertEqual(datasweet_eval("if(agg1, 2, 3)", {"1": {"value": 1}}), 2)
        self.assertEqual(datasweet_eval("1+if(agg1, 1, 2)", {"1": {"value": 0}}), 3)
        self.assertIsNone(
            datasweet_eval("agg1 / agg2", {"1": {"value": 1}, "2": {"value": 0}})
        )
        self.assertEqual(
            datasweet_eval("agg1", {"1": {"std_deviation": 1.5}, "name": {"value": 9}}),
            1.5,
        )
        self.assertEqual(datasweet_eval("agg1", {"1": {"values": {"50.0": 2.5}}}), 2.5)
        self.assertEqual(datasweet_eval("ifnan(agg1, 4)", {"1": {"value": None}}), 4)
        self.assertEqual(
            datasweet_eval(
                "agg1",
                {"1": {"hits": {"hits": [{"_source": {"name": "ada"}}]}}},
            ),
            "ada",
        )
        self.assertEqual(
            datasweet_eval("ifnan(agg1, 4)", {"1": {"hits": {"hits": []}}}), 4
        )
        self.assertEqual(
            datasweet_eval("ifnan(agg1, 4)", {"1": {"hits": {"hits": [{"_id": "1"}]}}}),
            4,
        )
        self.assertEqual(datasweet_eval("ifnan(agg1, 4)", {"1": {"hits": {}}}), 4)
        with pytest.raises(ValueError):
            datasweet_eval("foo + 1", {})
        with pytest.raises(ValueError):
            DatasweetTransformer().visit(__import__("ast").parse("x + 1", mode="eval"))


class TestVegaRenderer(unittest.TestCase):
    def test_locales_and_timezone_injection(self):
        renderer = VegaRenderer("fr", "Europe/Paris")
        spec = {"$schema": "https://vega.github.io/schema/vega-lite/v5.json"}
        with patch(
            "pybana.helpers.vega.vlc.vegalite_to_svg", return_value="<lite>"
        ) as lite:
            self.assertEqual(renderer.to_svg(spec), "<div><lite></div>")
        self.assertEqual(lite.call_args.kwargs["format_locale"], "fr-FR")
        self.assertEqual(lite.call_args.kwargs["time_format_locale"], "fr-FR")
        self.assertEqual(spec["config"]["timezone"], "Europe/Paris")
        self.assertEqual(spec["config"]["locale"], {})
        self.assertEqual(spec["config"]["timeFormat"], {})

        plain = {}
        with patch("pybana.helpers.vega.vlc.vega_to_svg", return_value="<vg>") as vega:
            self.assertEqual(VegaRenderer("ro", None).to_svg(plain), "<div><vg></div>")
        self.assertIs(vega.call_args.kwargs["format_locale"], RO_FORMAT_LOCALE)
        self.assertIsNone(vega.call_args.kwargs["time_format_locale"])
        self.assertNotIn("config", plain)

        with patch("pybana.helpers.vega.vlc.vega_to_svg", return_value="<vg>") as vega:
            VegaRenderer(None, "").to_svg({})
        self.assertIsNone(vega.call_args.kwargs["format_locale"])
        self.assertIsNone(vega.call_args.kwargs["time_format_locale"])

        unknown = {}
        with patch("pybana.helpers.vega.vlc.vega_to_svg", return_value="<vg>"):
            VegaRenderer("xx", "UTC").to_svg(unknown)
        self.assertEqual(unknown["config"]["timezone"], "UTC")
        self.assertFalse(renderer._is_vegalite({}))
        self.assertFalse(renderer._is_vegalite({"$schema": "vega"}))
        kept = {"config": {"locale": {"x": 1}, "timeFormat": {"a": "b"}}}
        self.assertIs(renderer._inject_timezone(kept, "UTC"), kept)
        self.assertEqual(kept["config"]["locale"], {"x": 1})
        self.assertEqual(kept["config"]["timeFormat"], {"a": "b"})
        self.assertEqual(kept["config"]["timezone"], "UTC")

    def test_fallback_renderer(self):
        proc = Mock()
        proc.communicate.return_value = (b"<svg/>", b"")
        original = {"spec": {"a": 1}}
        with patch("pybana.helpers.vega.subprocess.Popen", return_value=proc) as popen:
            rendered = FallbackVegaRenderer("/bin/vega").to_svg(
                original, auth_headers={"Authorization": "token"}
            )
        self.assertEqual(rendered, "<svg/>")
        self.assertNotIn("authHeaders", original)
        self.assertEqual(popen.call_args.args[0], ["/bin/vega"])
        sent = json.loads(proc.communicate.call_args.kwargs["input"])
        self.assertEqual(sent["authHeaders"]["Authorization"], "token")

        proc.communicate.return_value = (b"<svg/>", b"")
        with patch("pybana.helpers.vega.subprocess.Popen", return_value=proc):
            wrapped = FallbackVegaRenderer("/bin/vega").to_svg(["raw"])
        self.assertEqual(
            json.loads(proc.communicate.call_args.kwargs["input"])["spec"], ["raw"]
        )
        self.assertEqual(wrapped, "<svg/>")

        proc.communicate.return_value = (b"", b"cli failed")
        with patch("pybana.helpers.vega.subprocess.Popen", return_value=proc):
            with pytest.raises(InvalidVegaSpecException) as raised:
                FallbackVegaRenderer("/bin/vega").to_svg({"spec": {}})
        self.assertEqual(raised.value.vega_cli_traceback, "cli failed")
        self.assertIn("Error when rendering", str(raised.value.args))

        failing = Mock()
        failing.communicate.return_value = (b"<svg fb>", b"")
        with patch(
            "pybana.helpers.vega.vlc.vega_to_svg", side_effect=RuntimeError("bad")
        ):
            with patch("pybana.helpers.vega.capture_exception") as captured:
                with patch(
                    "pybana.helpers.vega.subprocess.Popen", return_value=failing
                ):
                    svg = VegaRenderer("en", "UTC").to_svg({"mark": "rect"})
        self.assertEqual(svg, "<div><svg fb></div>")
        captured.assert_called_once()
        self.assertIs(helpers.VegaRenderer, VegaRenderer)
