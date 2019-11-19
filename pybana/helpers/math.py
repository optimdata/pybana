# -*- coding: utf-8 -*-

__all__ = ("percentage",)


def percentage(num, den):
    return 100 * num / den if den else 0
