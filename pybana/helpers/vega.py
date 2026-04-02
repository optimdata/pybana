# -*- coding: utf-8 -*-
import json
import os
import subprocess

import vl_convert as vlc

from typing import Any, Dict, Optional, Union
from sentry_sdk import capture_exception

__all__ = ("InvalidVegaSpecException", "VegaRenderer")


class InvalidVegaSpecException(Exception):
    def __init__(self, message, vega_cli_traceback, *args, **kwargs):
        super().__init__(self, message, *args, **kwargs)
        self.vega_cli_traceback = vega_cli_traceback

VEGA_BIN = os.path.join(os.path.dirname(__file__), "./bin/vega-cli")

LANGUAGE_TO_FORMAT_LOCALE: Dict[str, str] = {
    "fr": "fr-FR",
    "de": "de-DE",
    "es": "es-ES",
    "it": "it-IT",
    "ja": "ja-JP",
    "cs": "cs-CZ",
    "ro": "ro",  # not in d3 built-ins — handled via custom dict below
    "en": "en-US",
}

LANGUAGE_TO_TIME_FORMAT_LOCALE: Dict[str, str] = {
    "fr": "fr-FR",
    "de": "de-DE",
    "es": "es-ES",
    "it": "it-IT",
    "ja": "ja-JP",
    "cs": "cs-CZ",
    "en": "en-US",
}

RO_FORMAT_LOCALE: Dict[str, Any] = {
    "decimal": ",",
    "thousands": ".",
    "grouping": [3],
    "currency": ["", " RON"],
    "numerals": ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"],
    "percent": "%",
    "minus": "\u2212",
    "nan": "NaN",
}


class VegaRenderer:
    def __init__(self, language, timezone):
        self.fallback_renderer = FallbackVegaRenderer()
        self.language = language
        self.timezone = timezone

    def _is_vegalite(self, spec: Dict[str, Any]) -> bool:
        return "vega-lite" in spec.get("$schema", "")

    def _inject_timezone(self, spec: Dict[str, Any], timezone: str) -> Dict[str, Any]:
        """Inject a default timezone into the Vega spec config."""
        config = spec.setdefault("config", {})
        if "locale" not in config:
            config["locale"] = {}
        config.setdefault("timeFormat", {})
        # Vega 5.25+ supports config.timezone
        config["timezone"] = timezone
        return spec

    def _resolve_format_locale(
        self, language: Optional[str]
    ) -> Union[Optional[str], Dict[str, Any]]:
        if language is None:
            return None
        locale_name = LANGUAGE_TO_FORMAT_LOCALE.get(language)
        if locale_name == "ro":
            return RO_FORMAT_LOCALE
        return locale_name

    def _resolve_time_format_locale(self, language: Optional[str]) -> Optional[str]:
        if language is None:
            return None
        return LANGUAGE_TO_TIME_FORMAT_LOCALE.get(language)

    def _to_svg(self, spec):
        """
        Python equivalent of the Node.js vega-to-svg script. Reads a JSON object
        from stdin with the shape ``{"spec": <vega-spec>, "language?": "fr", "timezone?": "Europe/Paris"}``
        and writes the rendered SVG to stdout.
        """
        format_locale = self._resolve_format_locale(self.language)
        time_format_locale = self._resolve_time_format_locale(self.language)

        if self.timezone:
            spec = self._inject_timezone(spec, self.timezone)

        try:
            if self._is_vegalite(spec):
                return vlc.vegalite_to_svg(
                    vl_spec=spec,
                    format_locale=format_locale,
                    time_format_locale=time_format_locale,
                )

            return vlc.vega_to_svg(
                vg_spec=spec,
                format_locale=format_locale,
                time_format_locale=time_format_locale,
            )
        except Exception as exc:
            # TODO : Update vl-convert-python when release is greater than > 1.9.0.post1
            capture_exception(exc)
            return self.fallback_renderer.to_svg(spec)

    def to_svg(self, spec):
        svg_str = self._to_svg(spec)
        return f"<div>{svg_str}</div>"


class FallbackVegaRenderer:
    """
    Renderer which takes in input a vega spec and returns the svg code
    """

    def __init__(self, vega_bin=VEGA_BIN):
        self.vega_bin = vega_bin

    def to_svg(self, spec, auth_headers=None):
        if isinstance(spec, dict):
            spec = dict(spec)
        else:
            spec = {"spec": spec}
        if auth_headers is not None:
            spec["authHeaders"] = auth_headers
        p = subprocess.Popen(
            [self.vega_bin],
            stdout=subprocess.PIPE,
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        result = p.communicate(input=json.dumps(spec).encode())
        if result[0]:
            return result[0].decode()
        raise InvalidVegaSpecException(
            "Error when rendering vega visualization", result[1].decode()
        )