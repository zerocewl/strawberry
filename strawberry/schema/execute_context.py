from functools import partial
from typing import Any, Dict, Hashable, List, Optional, TypeVar, Union

# from promise import Promise, is_thenable
from vine import promise, barrier, wrap
from strawberry.sync_dataloader import dispatch, get_all_loaders

from graphql import (
    ExecutionContext,
    GraphQLObjectType,
    GraphQLOutputType,
    GraphQLResolveInfo,
)
from graphql.language import FieldNode, OperationDefinitionNode
from graphql.pyutils import (
    AwaitableOrValue,
    Path,
    Undefined,
    is_awaitable as default_is_awaitable,
)


def is_thenable(p):
    return isinstance(p, promise)


def get_promise_value(p):
    if not p.ready:
        raise Exception("Promise is not ready yet")

    return p.value[0][0]


def is_awaitable(value):
    """
    Create custom is_awaitable function to make sure that Promises' aren't
    considered awaitable
    """
    if is_thenable(value):
        return False
    return default_is_awaitable(value)


class PromiseList:
    def __init__(self, values):
        self.promise = promise()
        self._length = len(values)
        self._total_resolved = 0
        self._original_values = list(values)
        self._values = list(values)

        for i, val in enumerate(values):
            if is_thenable(val):
                if val.ready:
                    self._promise_fulfilled(i, get_promise_value(val))
                elif val.failed:
                    # TODO
                    pass
                else:
                    val = val.then(
                        wrap(
                            promise(
                                partial(self._promise_fulfilled, i),
                                on_error=self._promise_rejected,
                            )
                        )
                    )
                    self._values[i] = val
                # elif maybe_promise.is_fulfilled:
                #     is_resolved = self._promise_fulfilled(maybe_promise._value(), i)
                # elif maybe_promise.is_rejected:
                #     is_resolved = self._promise_rejected(maybe_promise._reason(), promise=maybe_promise)

            else:
                self._promise_fulfilled(i, val)

    def _promise_fulfilled(self, i, value):
        if is_thenable(value):
            value = get_promise_value(value)
        self._values[i] = value
        self._total_resolved += 1
        if self._total_resolved >= self._length:
            self._resolve(self._values)
            return True
        return False

    def _promise_rejected(self, error):
        self._total_resolved += 1
        self._reject(error)
        return True

    def _resolve(self, values):
        self.promise(values)

    def _reject(self, error):
        self.promise.throw(error)


S = TypeVar("S")


def promise_for_dict(
    value: Any,  # Dict[Hashable, Union[S, Promise[S]]]
):  # -> Promise[Dict[Hashable, S]]:
    """
    A special function that takes a dictionary of promises
    and turns them into a promise for a dictionary of values.
    """

    def handle_success(resolved_values: List[S]) -> Dict[Hashable, S]:
        return_value = zip(value.keys(), resolved_values)
        return dict(return_value)

    return PromiseList(value.values()).promise.then(handle_success)


class ExecutionContextWithPromise(ExecutionContext):
    is_awaitable = staticmethod(is_awaitable)

    def execute_operation(
        self, operation: OperationDefinitionNode, root_value: Any
    ) -> Optional[AwaitableOrValue[Any]]:
        result = super().execute_operation(operation, root_value)

        # Run all data loaders
        while True:
            loaders = get_all_loaders()
            for loader in loaders:
                dispatch(loader)

            if all([loader.is_ready for loader in loaders]):
                break

        return result

    def build_response(self, data):
        if is_thenable(data):
            original_build_response = super().build_response

            def on_rejected(error):
                self.errors.append(error)
                return None

            def on_resolve(data):
                return original_build_response(data)

            p = data.then(on_resolve, on_error=on_rejected)
            return get_promise_value(p)
        return super().build_response(data)

    def complete_value_catching_error(
        self,
        return_type: GraphQLOutputType,
        field_nodes: List[FieldNode],
        info: GraphQLResolveInfo,
        path: Path,
        result: Any,
    ) -> AwaitableOrValue[Any]:
        """Complete a value while catching an error.
        This is a small wrapper around completeValue which detects and logs errors in
        the execution context.
        """
        completed: AwaitableOrValue[Any]
        try:
            if is_thenable(result):

                def handle_error(error):
                    self.handle_field_error(error, field_nodes, path, return_type)

                completed = result.then(
                    lambda resolved: self.complete_value(
                        return_type, field_nodes, info, path, resolved
                    ),
                    on_error=handle_error,
                )
            else:
                completed = self.complete_value(
                    return_type, field_nodes, info, path, result
                )
            return completed
        except Exception as error:
            self.handle_field_error(error, field_nodes, path, return_type)
            return None

    def execute_fields(
        self,
        parent_type: GraphQLObjectType,
        source_value: Any,
        path: Optional[Path],
        fields: Dict[str, List[FieldNode]],
    ):
        """Execute the given fields concurrently.

        Implements the "Evaluating selection sets" section of the spec for "read" mode.
        """
        contains_promise = False
        results = {}
        for response_name, field_nodes in fields.items():
            field_path = Path(path, response_name)
            result = self.resolve_field(
                parent_type, source_value, field_nodes, field_path
            )
            if result is not Undefined:
                results[response_name] = result
                if is_thenable(result):
                    contains_promise = True

        if contains_promise:
            return promise_for_dict(results)

        return results
