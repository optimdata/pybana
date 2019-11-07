# -*- coding: utf-8 -*-

from elasticsearch_dsl import Q


class BaseFilterTranslator:
    def estype(self, filtr):
        return self.type

    def params(self, filtr):
        return filtr["meta"]["params"]

    def translate(self, filtr):
        return Q(self.estype(filtr), **{filtr["meta"]["key"]: self.params(filtr)})


class ExistsFilterTranslator(BaseFilterTranslator):
    type = "exists"

    def translate(self, filtr):
        return Q("exists", field=filtr["meta"]["key"])


class PhraseFilterTranslator(BaseFilterTranslator):
    type = "phrase"

    def estype(self, filtr):
        return "match_phrase"

    def params(self, filtr):
        return {"query": filtr["meta"]["value"]}


class PhrasesFilterTranslator(PhraseFilterTranslator):
    type = "phrases"

    def translate(self, filtr):
        q = None
        for param in filtr["meta"]["params"]:
            q1 = Q("match_phrase", **{filtr["meta"]["key"]: param})
            q = q1 if q is None else q | q1
        return q


class RangeFilterTranslator(BaseFilterTranslator):
    type = "range"


FILTER_TRANSLATORS = {
    filtr.type: filtr
    for filtr in (
        ExistsFilterTranslator,
        PhraseFilterTranslator,
        PhrasesFilterTranslator,
        RangeFilterTranslator,
    )
}


class FilterTranslator:
    def translate(self, filters):
        q = Q()
        for filtr in filters:
            if filtr["meta"]["disabled"]:
                continue
            translator = FILTER_TRANSLATORS[filtr["meta"]["type"]]()
            q1 = translator.translate(filtr)
            q &= ~q1 if filtr["meta"]["negate"] else q1
        return q
