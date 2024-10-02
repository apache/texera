import ast
import json
import sys
import base64

class ParentNodeVisitor(ast.NodeVisitor):
    def __init__(self):
        self.parent = None

    def generic_visit(self, node):
        node.parent = self.parent
        previous_parent = self.parent
        self.parent = node
        super().generic_visit(node)
        self.parent = previous_parent

class TypeAnnotationVisitor(ast.NodeVisitor):
    def __init__(self, start_line_offset=0):
        self.untyped_args = []
        self.start_line_offset = start_line_offset

    def visit(self, node):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            self.process_function(node)
        elif isinstance(node, ast.Lambda):
            raise ValueError("Lambda functions do not support type annotation")
        self.generic_visit(node)

    def process_function(self, node):
        # Boolean to determine if it's a global function or a method
        is_method = isinstance(node.parent, ast.ClassDef)
        # Boolean to determine if it's a static method
        is_staticmethod = False
        if is_method and hasattr(node, 'decorator_list'):
            for decorator in node.decorator_list:
                if isinstance(decorator, ast.Name) and decorator.id == 'staticmethod':
                    is_staticmethod = True
                elif isinstance(decorator, ast.Attribute) and decorator.attr == 'staticmethod':
                    is_staticmethod = True
        args = node.args

        all_args = []
        all_args.extend(args.args)
        # Positional-only
        all_args.extend(args.posonlyargs)
        # Keyword-only
        all_args.extend(args.kwonlyargs)
        # *args
        if args.vararg:
            all_args.append(args.vararg)
        # **kwargs
        if args.kwarg:
            all_args.append(args.kwarg)

        start_index = 0
        # Skip the "self" or "cls"
        if is_method and not is_staticmethod:
            start_index = 1
        for i, arg in enumerate(all_args[start_index:]):
            if not arg.annotation:
                self.add_untyped_arg(arg)


    def add_untyped_arg(self, arg):
        start_line = arg.lineno + self.start_line_offset - 1
        start_col = arg.col_offset + 1
        end_line = start_line
        end_col = start_col + len(arg.arg)
        self.untyped_args.append([arg.arg, start_line, start_col, end_line, end_col])

def find_untyped_variables(source_code, start_line):
    tree = ast.parse(source_code)
    ParentNodeVisitor().visit(tree)
    visitor = TypeAnnotationVisitor(start_line_offset=start_line)
    visitor.visit(tree)
    return visitor.untyped_args

if __name__ == "__main__":
    encoded_code = sys.argv[1]
    start_line = int(sys.argv[2])
    # Encoding the code to transmit multi-line code as a single command-line argument before, so we need to decode it here
    source_code = base64.b64decode(encoded_code).decode('utf-8')
    untyped_variables = find_untyped_variables(source_code, start_line)
    print(json.dumps(untyped_variables))
