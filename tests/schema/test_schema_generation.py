from textwrap import dedent
from typing import Any, Dict, Optional, cast

import pytest

from graphql import (
    ExecutionContext as GraphQLExecutionContext,
    ExecutionResult,
    GraphQLField,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLSchema,
    GraphQLString,
    print_schema as graphql_core_print_schema,
)
from graphql.pyutils import AwaitableOrValue

import strawberry
from strawberry.schema.schema_converter import GraphQLCoreConverter


def test_generates_schema():
    @strawberry.type
    class Query:
        example: str

    schema = strawberry.Schema(query=Query)

    target_schema = GraphQLSchema(
        query=GraphQLObjectType(
            name="Query",
            fields={
                "example": GraphQLField(
                    GraphQLNonNull(GraphQLString), resolve=lambda obj, info: "world"
                )
            },
        )
    )

    assert schema.as_str().strip() == graphql_core_print_schema(target_schema).strip()


def test_schema_introspect_returns_the_introspection_query_result():
    @strawberry.type
    class Query:
        example: str

    schema = strawberry.Schema(query=Query)
    introspection = schema.introspect()
    assert {"__schema"} == introspection.keys()
    assert {
        "queryType",
        "mutationType",
        "subscriptionType",
        "types",
        "directives",
    } == introspection["__schema"].keys()


def test_schema_fails_on_an_invalid_schema():
    @strawberry.type
    class Query:
        ...  # Type must have at least one field

    with pytest.raises(ValueError, match="Invalid Schema. Errors.*"):
        strawberry.Schema(query=Query)


def test_custom_execution_context():
    class CustomExecutionContext(GraphQLExecutionContext):
        def build_response(
            self, data: AwaitableOrValue[Optional[Dict[str, Any]]]
        ) -> AwaitableOrValue[ExecutionResult]:
            result = cast(ExecutionResult, super().build_response(data))

            if not result.data:
                return result

            # Add some extra data to the response
            result.data.update(
                {
                    "extra": "data",
                }
            )
            return result

    @strawberry.type
    class Query:
        hello: str = "World"

    schema = strawberry.Schema(
        query=Query, execution_context_class=CustomExecutionContext
    )

    result = schema.execute_sync("{ hello }", root_value=Query())

    assert result.data == {
        "hello": "World",
        "extra": "data",
    }


def test_custom_schema_converter():
    class CustomGraphQLConverter(GraphQLCoreConverter):
        def from_object_type(self, *args, **kwargs):
            graphql_object_type = super().from_object_type(*args, **kwargs)
            new_name = f"Custom{graphql_object_type.name}"

            # update typemap
            self.type_map[new_name] = self.type_map[graphql_object_type.name]
            graphql_object_type.name = new_name

            return graphql_object_type

    class CustomSchema(strawberry.Schema):
        def get_schema_converter(self):
            return CustomGraphQLConverter()

    @strawberry.type
    class Query:
        example: str

    schema = CustomSchema(Query)
    expected = dedent(
        """\
        schema {
          query: CustomQuery
        }

        type CustomQuery {
          example: String!
        }
        """
    ).strip()

    assert str(schema) == expected
