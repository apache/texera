package edu.uci.ics.amber.operator.filter

import edu.uci.ics.amber.core.tuple.Tuple

class SpecializedFilterOpExec(predicates: List[FilterPredicate]) extends FilterOpExec {

  setFilterFunc((tuple: Tuple) => predicates.exists(_.evaluate(tuple)))
}
