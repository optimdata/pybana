# -*- coding: utf-8 -*-

import json
import os
import subprocess

__all__ = ("VegaRenderer",)
VEGA_BIN = os.path.join(os.path.dirname(__file__), "../../bin/vega-cli")


class VegaRenderer:
    """
    Renderer which takes in input a vega spec and returns the svg code
    """

    def __init__(self, vega_bin=VEGA_BIN):
        self.vega_bin = vega_bin

    def to_svg(self, spec):
        p = subprocess.Popen(
            [self.vega_bin],
            stdout=subprocess.PIPE,
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return p.communicate(input=json.dumps(spec).encode())[0].decode()
