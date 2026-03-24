# -*- coding: utf-8 -*-

import json
import os
import subprocess

__all__ = ("InvalidVegaSpecException", "VegaRenderer")
VEGA_BIN = os.path.join(os.path.dirname(__file__), "../../bin/vega-cli")


class InvalidVegaSpecException(Exception):
    def __init__(self, message, vega_cli_traceback, *args, **kwargs):
        super().__init__(self, message, *args, **kwargs)
        self.vega_cli_traceback = vega_cli_traceback


class VegaRenderer:
    """
    Renderer which takes in input a vega spec and returns the svg code
    """

    def __init__(self, vega_bin=VEGA_BIN):
        self.vega_bin = vega_bin

