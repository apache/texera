package edu.uci.ics.texera.workflow.operators.udf.pythonV2

object PythonCodeBuilder {
  def build(newAttributeUnits: List[NewAttributeUnit]): String = {
    // build the python udf code
    var code: String = "from pytexera import *\n" +
      "from decimal import Decimal\n" +
      "import re\n" +
      "class ProcessTupleOperator(UDFOperatorV2):\n" +
      "    @overrides\n" +
      "    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:\n";
    // if newColumns is not null, add the new column into the tuple
    if (newAttributeUnits != null) {
      for (unit <- newAttributeUnits) {
        code += s"        self.add_column(tuple_, '${unit.attributeName}')\n"
        code += new LambdaExpression(unit.expression, unit.attributeName, unit.attributeType).eval()
      }
    }
    code += "        yield tuple_\n" +
      "    def add_column(self, tuple_: Tuple, new_column_name: str):\n" +
      "        columns = tuple_.get_field_names()\n" +
      "        if new_column_name in columns:\n" +
      "            raise \"Column name \" + new_column_name + \" already exists!\"\n" +
      "        tuple_[new_column_name] = None\n" +
      "class Utils:\n" +
      "    @staticmethod\n" +
      "    def evaluate(tuple_: Tuple, expression: str):\n" +
      "        columns = tuple_.get_field_names()\n" +
      "        tokens = Utils.find_tokens(expression)\n" +
      "        print(tuple_.as_dict())\n" +
      "        for token in tokens:\n" +
      "            if token not in columns:\n" +
      "                raise \"Column name \" + token + \" doesn't exist!\"\n" +
      "            if not Utils.is_numeric(tuple_[token]):\n" +
      "                raise \"Column name \" + token + \" isn't a numerical data type\"\n" +
      "            expression = Utils.replace_token(expression, token, tuple_[token])\n" +
      "        return eval(expression)\n" +
      "    @staticmethod\n" +
      "    def find_tokens(s: str):\n" +
      "        pattern = r\"`([^`]+)`\"\n" +
      "        return re.findall(pattern, s)\n" +
      "    @staticmethod\n" +
      "    def replace_token(s: str, token: str, value) -> str:\n" +
      "        pattern = rf\"`{token}`\"\n" +
      "        return re.sub(pattern, str(value), s)\n" +
      "    @staticmethod\n" +
      "    def is_numeric(x):\n" +
      "        return isinstance(x, (int, float, Decimal))\n"
    code
  }
}
