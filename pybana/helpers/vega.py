# -*- coding: utf-8 -*-
import vl_convert as vlc

from typing import Any, Dict, Optional, Union

__all__ = ("InvalidVegaSpecException", "VegaRenderer")


class InvalidVegaSpecException(Exception):
    def __init__(self, message, vega_cli_traceback, *args, **kwargs):
        super().__init__(self, message, *args, **kwargs)
        self.vega_cli_traceback = vega_cli_traceback


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

    def to_svg(self, spec):
        svg_str = self._to_svg(spec)
        return f"<div>{svg_str}</div>"
