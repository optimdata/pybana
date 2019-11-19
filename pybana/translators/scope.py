# -*- coding: utf-8 -*-

__all__ = ("Scope",)


class Scope:
    """
    Scope associated to the visualization.

    :param beg datetime: Begin date of the period on which data should be fetched.
    :param end datetime: End date of the period on which data should be fetched.
    :param tzinfo (str, pytz.Timezone): Timezone of the request.
    :param config pybana.Config: Config of the kibana instance.
    """

    def __init__(self, beg, end, tzinfo, config):
        self.beg = beg
        self.end = end
        self.tzinfo = tzinfo
        self.config = config
