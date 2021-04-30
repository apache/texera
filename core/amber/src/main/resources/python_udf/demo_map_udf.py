def map_function(row, *args):
    """
    This is a demo map UDF. Takes exactly 1 tuple as input and outputs exactly 1 tuple as output. This function is
    automatically recognized as the input of a `TexeraMapOperator`. Specifically, This function simply copies the input
    field to the output field, both specified in `args`.
    :param row: Input tuple.
    :param args: Argument(s), including the name that specify the input field and additional output field.
    :return: The output tuple.
    """
    input_col, output_col, *_ = args
    return {output_col: row[input_col]}
