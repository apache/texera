package Engine.Common

import Engine.Common.tuple.Tuple


trait TupleSink extends TupleProcessor {

  def getResultTuples(): Array[Tuple]

}
