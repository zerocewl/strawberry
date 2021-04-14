import ast
import inspect
from typing import TYPE_CHECKING, List

import pytest

import astor

if TYPE_CHECKING:
    from _pytest.config import Config


class Transformer(ast.NodeTransformer):
    def __init__(self) -> None:
        super().__init__()

        self.in_test = False
        self.types: List[str] = []

    def visit_FunctionDef(self, node: ast.FunctionDef):
        if node.name.startswith("test_"):
            self.in_test = True
            self.types.append("MyType")

        node.body.insert(0, ast.Global(names=self.types))

        self.generic_visit(node)


def pytest_collection_modifyitems(config: "Config", items: List[pytest.Function]):
    t = Transformer()

    for item in items:
        function_code = inspect.getsource(item.obj)
        function_ast = ast.parse(function_code)

        lol = t.visit(function_ast)

        print(astor.to_source(lol))

        code = compile(lol, filename="<no_file>", mode="exec")
        _local_ctx = {}
        _global_ctx = {}
        exec(code, _global_ctx, _local_ctx)

        breakpoint()

        item.obj = lambda: 1 / 2


def pytest_emoji_xfailed(config):
    return "🤷‍♂️ ", "XFAIL 🤷‍♂️ "
