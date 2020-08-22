package Engine.Operators.Filter

import Engine.Common.AmberTuple.Tuple
import Engine.Operators.Filter.FilterType.AmberDateTime
import com.github.nscala_time.time.Imports._

class FilterSpecializedTupleProcessor(
    targetField: Int,
    filterType: FilterType.Val[DateTime],
    threshold: DateTime
) extends FilterTupleProcessor[DateTime](targetField, filterType, threshold) {

  override def accept(tuple: Tuple): Unit = {
    val str = tuple.getString(targetField)
    if (str != null && filterType.validate(DateTime.parse(str), threshold)) {
      nextFlag = true
      _tuple = tuple
    }
  }
}
