import strawberry
from strawberry.schema.execute_context import ExecutionContextWithPromise
from strawberry.sync_dataloader import SyncDataLoader


def test_batches_correct(mocker):
    def idx(keys):
        return keys

    mock_loader = mocker.Mock(side_effect=idx)

    @strawberry.type
    class Query:
        @strawberry.field
        def get_id(self, info, id: str) -> str:
            return info.context["dataloaders"]["test"].load(id)

    schema = strawberry.Schema(query=Query)
    result = schema.execute_sync(
        """
        query {
            id1: getId(id: "1")
            id2: getId(id: "2")
        }
    """,
        context_value={"dataloaders": {"test": SyncDataLoader(load_fn=mock_loader)}},
        execution_context_class=ExecutionContextWithPromise,
    )
    assert not result.errors
    mock_loader.assert_called_once_with(["1", "2"])
    assert result.data == {"id1": "1", "id2": "2"}


def test_handles_promise_and_plain(mocker):
    def idx(keys):
        return keys

    mock_loader = mocker.Mock(side_effect=idx)

    @strawberry.type
    class Query:
        @strawberry.field
        def get_id(self, info, id: str) -> str:
            return info.context["dataloaders"]["test"].load(id)

        @strawberry.field
        def hello(self) -> str:
            return "world"

    schema = strawberry.Schema(query=Query)
    result = schema.execute_sync(
        """
        query {
            hello
            id1: getId(id: "1")
            id2: getId(id: "2")
        }
    """,
        context_value={"dataloaders": {"test": SyncDataLoader(load_fn=mock_loader)}},
        execution_context_class=ExecutionContextWithPromise,
    )
    assert not result.errors
    assert result.data == {"hello": "world", "id1": "1", "id2": "2"}
    mock_loader.assert_called_once_with(["1", "2"])


def test_batches_multiple_loaders(mocker):
    def idx(keys):
        return keys

    location_mock_loader = mocker.Mock(side_effect=idx)
    company_mock_loader = mocker.Mock(side_effect=idx)

    @strawberry.type
    class Location:
        id: str

    @strawberry.type
    class Company:
        id: str

        @strawberry.field
        def location(self, info) -> Location:
            return (
                info.context["dataloaders"]["location_loader"]
                .load(f"location-{self.id}")
                .then(lambda id: Location(id=id))
            )

    @strawberry.type
    class Query:
        @strawberry.field
        def get_company(self, info, id: str) -> Company:
            return (
                info.context["dataloaders"]["company_loader"]
                .load(id)
                .then(lambda id: Company(id=id))
            )

    schema = strawberry.Schema(query=Query)
    result = schema.execute_sync(
        """
        query {
            company1: getCompany(id: "1") {
                id
                location {
                    id
                }
            }
            company2: getCompany(id: "2") {
                id
                location {
                    id
                }
            }
        }
    """,
        context_value={
            "dataloaders": {
                "location_loader": SyncDataLoader(load_fn=location_mock_loader),
                "company_loader": SyncDataLoader(load_fn=company_mock_loader),
            },
        },
        execution_context_class=ExecutionContextWithPromise,
    )
    assert not result.errors
    assert result.data == {
        "company1": {"id": "1", "location": {"id": "location-1"}},
        "company2": {"id": "2", "location": {"id": "location-2"}},
    }
    company_mock_loader.assert_called_once_with(["1", "2"])
    location_mock_loader.assert_called_once_with(["location-1", "location-2"])


def test_multiple_levels(mocker):
    global Company

    def idx(keys):
        return keys

    location_mock_loader = mocker.Mock(side_effect=idx)
    company_mock_loader = mocker.Mock(side_effect=idx)

    @strawberry.type
    class Location:
        id: str
        company_key: strawberry.Private(str)

        @strawberry.field
        def company(self, info) -> "Company":
            return (
                info.context["dataloaders"]["company_loader"]
                .load(self.company_key)
                .then(lambda id: Company(id=id))
            )

    @strawberry.type
    class Company:
        id: str

        @strawberry.field
        def location(self, info) -> Location:
            return (
                info.context["dataloaders"]["location_loader"]
                .load(f"location-{self.id}")
                .then(lambda id: Location(id=id, company_key=self.id))
            )

    @strawberry.type
    class Query:
        @strawberry.field
        def get_company(self, info, id: str) -> Company:
            return (
                info.context["dataloaders"]["company_loader"]
                .load(id)
                .then(lambda id: Company(id=id))
            )

    schema = strawberry.Schema(query=Query)
    result = schema.execute_sync(
        """
        query {
            company1: getCompany(id: "1") {
                id
                location {
                    id
                    company {
                        id
                        location {
                            id
                        }
                    }
                }
            }
            company2: getCompany(id: "2") {
                id
                location {
                    id
                    company {
                        id
                        location {
                            id
                        }
                    }
                }
            }
        }
    """,
        context_value={
            "dataloaders": {
                "company_loader": SyncDataLoader(load_fn=company_mock_loader),
                "location_loader": SyncDataLoader(load_fn=location_mock_loader),
            },
        },
        execution_context_class=ExecutionContextWithPromise,
    )
    assert not result.errors
    assert result.data == {
        "company1": {
            "id": "1",
            "location": {
                "id": "location-1",
                "company": {"id": "1", "location": {"id": "location-1"}},
            },
        },
        "company2": {
            "id": "2",
            "location": {
                "id": "location-2",
                "company": {"id": "2", "location": {"id": "location-2"}},
            },
        },
    }
    company_mock_loader.assert_called_once_with(["1", "2"])
    location_mock_loader.assert_called_once_with(["location-1", "location-2"])
