import ast
import json
import sys
import base64

class TypeAnnotationVisitor(ast.NodeVisitor):
    def __init__(self, start_line_offset=0):
        self.untyped_args = []
        # To calculate the correct start line
        self.start_line_offset = start_line_offset

    def visit_FunctionDef(self, node):
        for arg in node.args.args:
            # Self is not an argument
            if arg.arg == 'self':
                continue
            if not arg.annotation:
                # +1 and -1 is to change ast line/col to monaco editor line/col for the range
                start_line = arg.lineno + self.start_line_offset - 1
                start_col = arg.col_offset + 1
                end_line = start_line
                end_col = start_col + len(arg.arg)
                self.untyped_args.append([arg.arg, start_line, start_col, end_line, end_col])
        self.generic_visit(node)

def find_untyped_variables(source_code, start_line):
    tree = ast.parse(source_code)
    visitor = TypeAnnotationVisitor(start_line_offset=start_line)
    visitor.visit(tree)
    return visitor.untyped_args

if __name__ == "__main__":
    # First argument is the code
    encoded_code = sys.argv[1]
    # Second argument is the start line
    start_line = int(sys.argv[2])
    source_code = base64.b64decode(encoded_code).decode('utf-8')
    untyped_variables = find_untyped_variables(source_code, start_line)
    print(json.dumps(untyped_variables))