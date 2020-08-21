package texera.common

class TexeraContext {
  var workflowID: String = null
  var customFieldIndexMapping: Map[String, Integer] = _
  var isOneK = false

  def fieldIndexMapping(field: String): Integer = {
    if (customFieldIndexMapping != null) {
      val index = customFieldIndexMapping(field.toLowerCase.trim)
      if (index == null) {
        return -1;
      } else {
        return index;
      }
    } else {
      try {
        Integer.parseInt(field.trim())
      } catch {
        case n: NumberFormatException => null
        case e: Throwable => throw(e)
      }
    }
  }
}