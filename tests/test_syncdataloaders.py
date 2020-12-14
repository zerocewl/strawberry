import pytest

from strawberry.exceptions import WrongNumberOfResultsReturned
from strawberry.sync_dataloader import SyncDataLoader, dispatch


def get_promise_value(p):
    if not p.ready:
        raise Exception("Promise is not ready yet")

    return p.value[0][0]


def test_loading(mocker):
    def idx(keys):
        return keys

    mock_loader = mocker.Mock(side_effect=idx)

    loader = SyncDataLoader(load_fn=mock_loader)

    value_a = loader.load(1)
    value_b = loader.load(2)
    value_c = loader.load(3)
    dispatch(loader)

    mock_loader.assert_called_once_with([1, 2, 3])

    assert get_promise_value(value_a) == 1
    assert get_promise_value(value_b) == 2
    assert get_promise_value(value_c) == 3


def test_gathering(mocker):
    def idx(keys):
        return keys

    mock_loader = mocker.Mock(side_effect=idx)

    loader = SyncDataLoader(load_fn=mock_loader)

    loader.load(1)
    loader.load(2)
    loader.load(3)

    dispatch(loader)

    mock_loader.assert_called_once_with([1, 2, 3])

    values = []
    for batch in loader.batches:
        for task in batch.tasks:
            values.append(get_promise_value(task.future))

    assert values == [1, 2, 3]


def test_max_batch_size(mocker):
    def idx(keys):
        return keys

    mock_loader = mocker.Mock(side_effect=idx)

    loader = SyncDataLoader(load_fn=mock_loader, max_batch_size=2)

    loader.load(1)
    loader.load(2)
    loader.load(3)

    dispatch(loader)

    mock_loader.assert_has_calls([mocker.call([1, 2]), mocker.call([3])])


def test_error():
    def idx(keys):
        return [ValueError()]

    loader = SyncDataLoader(load_fn=idx)
    loader.load(1)

    with pytest.raises(ValueError):
        dispatch(loader)


def test_error_and_values():
    def idx(keys):
        if keys == [2]:
            return [2]

        return [ValueError()]

    loader = SyncDataLoader(load_fn=idx)
    loader.load(1)

    with pytest.raises(ValueError):
        dispatch(loader)

    p = loader.load(2)
    dispatch(loader)
    assert p.ready
    assert p.value[0][0]


def test_when_raising_error_in_loader():
    def idx(keys):
        raise ValueError()

    loader = SyncDataLoader(load_fn=idx)

    loader.load(1)
    loader.load(2)
    loader.load(3)

    with pytest.raises(ValueError):
        dispatch(loader)


def test_returning_wrong_number_of_results():
    def idx(keys):
        return [1, 2]

    loader = SyncDataLoader(load_fn=idx)
    loader.load(1)

    with pytest.raises(
        WrongNumberOfResultsReturned,
        match=(
            "Received wrong number of results in dataloader, "
            "expected: 1, received: 2"
        ),
    ):
        dispatch(loader)


def test_caches_by_id(mocker):
    def idx(keys):
        return keys

    mock_loader = mocker.Mock(side_effect=idx)

    loader = SyncDataLoader(load_fn=mock_loader, cache=True)

    a = loader.load(1)
    b = loader.load(1)

    assert a == b

    dispatch(loader)
    assert get_promise_value(a) == 1
    assert get_promise_value(b) == 1

    mock_loader.assert_called_once_with([1])


def test_cache_disabled(mocker):
    def idx(keys):
        return keys

    mock_loader = mocker.Mock(side_effect=idx)

    loader = SyncDataLoader(load_fn=mock_loader, cache=False)

    a = loader.load(1)
    b = loader.load(1)

    assert a != b

    dispatch(loader)

    assert get_promise_value(a) == 1
    assert get_promise_value(b) == 1

    mock_loader.assert_has_calls([mocker.call([1, 1])])
