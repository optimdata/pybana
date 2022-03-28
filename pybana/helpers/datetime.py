import json
import re

import pendulum

__all__ = (
    "convert",
    "format_timestamp",
    "get_scaled_date_format",
    "TOKEN_MAPPINGS",
    "UnknownMomentTokenError",
)

TOKEN_MAPPINGS = {
    "M": "M",
    "Mo": "Mo",
    "MM": "MM",
    "MMM": "MMM",
    "MMMM": "MMMM",
    "Q": "Q",
    "Qo": "Qo",
    "D": "D",
    "Do": "Do",
    "DD": "DD",
    "DDD": "DDD",
    "DDDo": "DDD",
    "DDDD": "DDDD",
    "d": "d",
    "do": "d",
    "dd": "dd",
    "ddd": "ddd",
    "dddd": "dddd",
    "e": "d",
    "E": "E",
    "YY": "YYYY",
    "YYYY": "YYYY",
    "YYYYYY": "YYYY",
    "Y": "Y",
    "A": "A",
    "a": "A",
    "H": "H",
    "HH": "HH",
    "h": "h",
    "hh": "hh",
    "m": "m",
    "mm": "mm",
    "s": "s",
    "ss": "ss",
    "S": "S",
    "SS": "SS",
    "SSS": "SSS",
    "SSSS": "SSSS",
    "SSSSS": "SSSSS",
    "SSSSSS": "SSSSSS",
    "z": "z",
    "zz": "zz",
    "Z": "Z",
    "ZZ": "ZZ",
    "X": "X",
    "x": "x",
    "LT": "LT",
    "LTS": "LTS",
    "L": "L",
    "l": "L",
    "LL": "LL",
    "ll": "ll",
    "LLL": "LLL",
    "lll": "LLL",
    "LLLL": "LLLL",
    "llll": "LLLL",
}


def _escape_tokenize(fmt):
    cur = 0
    brackets = 0
    for it in range(len(fmt)):
        if fmt[it] == "[":
            if brackets == 0 and it > cur:
                yield fmt[cur:it], False
                cur = it
            brackets += 1
        if it > 0 and fmt[it - 1] == "]":
            brackets -= 1
            if brackets == 0 and it > cur:
                yield fmt[cur:it], True
                cur = it
    if cur < len(fmt):
        yield fmt[cur:], brackets > 0


class UnknownMomentTokenError(ValueError):
    pass


def _convert_token(match):
    token = match.group(0)
    try:
        return TOKEN_MAPPINGS[token]
    except KeyError:
        raise UnknownMomentTokenError(f"This token is not handled: {token}")


def convert(fmt, ignore=True):
    """
    Convert moment formating to pendulum formating.

    Careful, moment tokens may be handled in several ways:
    - not handled: In this case, a `UnknownMomentTokenError` will be raised or None will be returned depending on the `ignore` argument.
    - approximated: Some moment tokens are mapped to pendulum tokens which behave almost identically. Example: `llll` is mapped to `LLLL`.
    - exact: For many tokens, pendulum provide the same behaviour.

    :param string fmt: The format string to convert.
    :param bool ignore: If true and if a token is not recognized, None will be returned. Otherwrise a `UnknownMomentTokenError` will be raised.
    """
    try:

        return "".join(
            [
                token if escaped else re.sub("[a-zA-Z]+", _convert_token, token)
                for token, escaped in _escape_tokenize(fmt)
            ]
        )
    except UnknownMomentTokenError:
        if ignore:
            return None
        raise


def format_timestamp(timestamp, fmt=None, locale=None):
    value = pendulum.from_timestamp(timestamp * 1e-3)
    return (
        value.isoformat() if fmt is None else value.format(convert(fmt), locale=locale)
    )


def get_scaled_date_format(config, interval):
    config = config.config.to_dict()
    scaled_date_formats = json.loads(config.get("dateFormat:scaled", "[]"))
    scaled_date_formats.reverse()
    default_date_format = config.get("dateFormat")
    for (duration, date_format) in scaled_date_formats:
        if not duration or interval >= pendulum.parse(duration):
            return date_format
    return default_date_format
