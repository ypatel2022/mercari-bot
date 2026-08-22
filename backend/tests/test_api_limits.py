"""Per-user keyword-cap enforcement tests for watchlist adding paths."""

from __future__ import annotations

import asyncio

import pytest
from api_resource_helpers import ApiResourceDatabase, client_for, create_destination, signup
from httpx import AsyncClient

from src import database
from src.api.app import create_app
from src.config import settings
from src.presets import PresetKeywordRecord

pytestmark = pytest.mark.asyncio

_KEYWORD_LIMIT = {"detail": "Keyword limit reached", "code": "keyword_limit_exceeded"}
_VALIDATION_ERROR = {"detail": "Invalid request", "code": "validation_error"}


@pytest.fixture
def api_database(monkeypatch: pytest.MonkeyPatch) -> ApiResourceDatabase:
    """Patch persistence to an isolated in-memory database."""
    fake = ApiResourceDatabase("keyword_limit_api_tests")
    monkeypatch.setattr(database, "db_client", fake)
    return fake


@pytest.fixture
def tiny_keyword_caps(monkeypatch: pytest.MonkeyPatch) -> None:
    """Override stored and per-request keyword bounds to a tiny test cap."""
    monkeypatch.setattr(settings, "max_keywords_per_user", 2)
    monkeypatch.setattr(settings, "max_keywords_per_request", 2)


async def _create_watchlist(
    client: AsyncClient,
    destination_id: object,
    *,
    name: str,
    keywords: list[str] | None = None,
    enabled: bool = True,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "name": name,
        "destination_id": destination_id,
        "enabled": enabled,
    }
    if keywords is not None:
        payload["keywords"] = keywords
    response = await client.post("/api/v1/watchlists", json=payload)
    assert response.status_code == 201, response.text
    return response.json()


async def test_all_four_adding_paths_reject_when_at_cap(
    api_database: ApiResourceDatabase,
    tiny_keyword_caps: None,
) -> None:
    """Create, replace, single add, and preset copy all return 409 at the cap."""
    preset = PresetKeywordRecord.new(name="Overflow", keywords=["gamma"])
    await database.upsert_preset_keyword(preset)
    application = create_app()
    async with client_for(application) as client:
        await signup(client, "at-cap@example.com")
        destination = await create_destination(client)
        filled = await _create_watchlist(
            client,
            destination["id"],
            name="Filled",
            keywords=["alpha", "beta"],
        )
        empty = await _create_watchlist(client, destination["id"], name="Empty")

        created = await client.post(
            "/api/v1/watchlists",
            json={"name": "Overflow", "keywords": ["gamma"], "destination_id": destination["id"]},
        )
        replaced = await client.patch(
            f"/api/v1/watchlists/{empty['id']}",
            json={"keywords": ["gamma"]},
        )
        added = await client.post(
            f"/api/v1/watchlists/{filled['id']}/keywords",
            json={"keyword": "gamma"},
        )
        copied = await client.post(
            f"/api/v1/watchlists/{filled['id']}/keywords/from-preset",
            json={"preset_id": preset._id},
        )
        listed = await client.get("/api/v1/watchlists")

    assert [response.status_code for response in (created, replaced, added, copied)] == [409] * 4
    assert all(response.json() == _KEYWORD_LIMIT for response in (created, replaced, added, copied))
    by_name = {item["name"]: item for item in listed.json()}
    assert by_name["Filled"]["keywords"] == ["alpha", "beta"]
    assert by_name["Empty"]["keywords"] == []
    assert "Overflow" not in by_name
    assert await database.get_registry_entry("mercari", "gamma") is None


async def test_tenant_can_fill_exactly_to_the_keyword_cap(
    api_database: ApiResourceDatabase,
    tiny_keyword_caps: None,
) -> None:
    """A tenant below the cap can add keywords up to exactly the configured total."""
    application = create_app()
    async with client_for(application) as client:
        await signup(client, "fill-cap@example.com")
        destination = await create_destination(client)
        first = await _create_watchlist(client, destination["id"], name="First", keywords=["alpha"])
        second = await client.post(
            f"/api/v1/watchlists/{first['id']}/keywords",
            json={"keyword": "beta"},
        )
        overflow = await client.post(
            f"/api/v1/watchlists/{first['id']}/keywords",
            json={"keyword": "gamma"},
        )

    assert second.status_code == 200
    assert second.json()["keywords"] == ["alpha", "beta"]
    assert overflow.status_code == 409
    assert overflow.json() == _KEYWORD_LIMIT


async def test_duplicate_keyword_and_preset_copy_are_noops_at_cap(
    api_database: ApiResourceDatabase,
    tiny_keyword_caps: None,
) -> None:
    """Re-adding an existing keyword or already-copied preset succeeds at the cap."""
    preset = PresetKeywordRecord.new(name="Existing", keywords=["alpha", "beta"])
    await database.upsert_preset_keyword(preset)
    application = create_app()
    async with client_for(application) as client:
        await signup(client, "noop-cap@example.com")
        destination = await create_destination(client)
        watchlist = await _create_watchlist(
            client,
            destination["id"],
            name="At Cap",
            keywords=["alpha", "beta"],
        )
        duplicate = await client.post(
            f"/api/v1/watchlists/{watchlist['id']}/keywords",
            json={"keyword": "alpha"},
        )
        copied = await client.post(
            f"/api/v1/watchlists/{watchlist['id']}/keywords/from-preset",
            json={"preset_id": preset._id},
        )

    assert duplicate.status_code == copied.status_code == 200
    assert duplicate.json()["keywords"] == copied.json()["keywords"] == ["alpha", "beta"]


async def test_removing_a_keyword_frees_cap_capacity(
    api_database: ApiResourceDatabase,
    tiny_keyword_caps: None,
) -> None:
    """Removing a stored keyword lets the next add succeed."""
    application = create_app()
    async with client_for(application) as client:
        await signup(client, "free-capacity@example.com")
        destination = await create_destination(client)
        watchlist = await _create_watchlist(
            client,
            destination["id"],
            name="Capped",
            keywords=["alpha", "beta"],
        )
        blocked = await client.post(
            f"/api/v1/watchlists/{watchlist['id']}/keywords",
            json={"keyword": "gamma"},
        )
        removed = await client.request(
            "DELETE",
            f"/api/v1/watchlists/{watchlist['id']}/keywords",
            json={"keyword": "beta"},
        )
        added = await client.post(
            f"/api/v1/watchlists/{watchlist['id']}/keywords",
            json={"keyword": "gamma"},
        )

    assert blocked.status_code == 409
    assert removed.status_code == added.status_code == 200
    assert added.json()["keywords"] == ["alpha", "gamma"]
    assert await database.get_registry_entry("mercari", "gamma") is not None
    assert await database.get_registry_entry("mercari", "beta") is None


async def test_keyword_cap_is_isolated_across_tenants(
    api_database: ApiResourceDatabase,
    tiny_keyword_caps: None,
) -> None:
    """One tenant at the cap does not consume another tenant's keyword budget."""
    application = create_app()
    async with client_for(application) as tenant_a, client_for(application) as tenant_b:
        await signup(tenant_a, "cap-a@example.com")
        await signup(tenant_b, "cap-b@example.com")
        destination_a = await create_destination(tenant_a, "A")
        destination_b = await create_destination(tenant_b, "B")
        watchlist_a = await _create_watchlist(
            tenant_a,
            destination_a["id"],
            name="A",
            keywords=["alpha", "beta"],
        )
        overflow_a = await tenant_a.post(
            f"/api/v1/watchlists/{watchlist_a['id']}/keywords",
            json={"keyword": "epsilon"},
        )
        created_b = await _create_watchlist(
            tenant_b,
            destination_b["id"],
            name="B",
            keywords=["gamma", "delta"],
        )
        overflow_b = await tenant_b.post(
            f"/api/v1/watchlists/{created_b['id']}/keywords",
            json={"keyword": "epsilon"},
        )
        still_b = await tenant_b.get(f"/api/v1/watchlists/{created_b['id']}")

    assert overflow_a.status_code == overflow_b.status_code == 409
    assert still_b.status_code == 200
    assert still_b.json()["keywords"] == ["gamma", "delta"]


async def test_concurrent_adds_cannot_both_exceed_the_keyword_cap(
    api_database: ApiResourceDatabase,
    tiny_keyword_caps: None,
) -> None:
    """Two concurrent adds that would together exceed the cap yield one success and one 409."""
    application = create_app()
    async with client_for(application) as client:
        await signup(client, "race-cap@example.com")
        destination = await create_destination(client)
        watchlist = await _create_watchlist(
            client,
            destination["id"],
            name="Race",
            keywords=["seed"],
        )
        alpha, beta = await asyncio.gather(
            client.post(f"/api/v1/watchlists/{watchlist['id']}/keywords", json={"keyword": "alpha"}),
            client.post(f"/api/v1/watchlists/{watchlist['id']}/keywords", json={"keyword": "beta"}),
        )
        fetched = await client.get(f"/api/v1/watchlists/{watchlist['id']}")

    statuses = sorted([alpha.status_code, beta.status_code])
    loser = alpha if alpha.status_code != 200 else beta
    assert statuses == [200, 409]
    assert loser.json() == _KEYWORD_LIMIT
    assert fetched.json()["keywords"] in (["seed", "alpha"], ["seed", "beta"])
    present = fetched.json()["keywords"][1]
    absent = "beta" if present == "alpha" else "alpha"
    assert await database.get_registry_entry("mercari", present) is not None
    assert await database.get_registry_entry("mercari", absent) is None


async def test_disabled_watchlists_count_toward_the_keyword_cap(
    api_database: ApiResourceDatabase,
    tiny_keyword_caps: None,
) -> None:
    """Keywords on a disabled watchlist still consume the cap, including later adds."""
    application = create_app()
    async with client_for(application) as client:
        await signup(client, "disabled-cap@example.com")
        destination = await create_destination(client)
        disabled = await _create_watchlist(
            client,
            destination["id"],
            name="Disabled",
            keywords=["alpha", "beta"],
            enabled=False,
        )
        overflow = await client.post(
            f"/api/v1/watchlists/{disabled['id']}/keywords",
            json={"keyword": "gamma"},
        )
        toggled = await client.patch(
            f"/api/v1/watchlists/{disabled['id']}/monitoring",
            json={"enabled": True},
        )
        fetched = await client.get(f"/api/v1/watchlists/{disabled['id']}")

    assert overflow.status_code == 409
    assert overflow.json() == _KEYWORD_LIMIT
    assert toggled.status_code == 200
    assert fetched.json()["keywords"] == ["alpha", "beta"]
    assert fetched.json()["enabled"] is True
    assert await database.get_registry_entry("mercari", "gamma") is None


async def test_rejected_mutations_leave_keyword_registry_unchanged(
    api_database: ApiResourceDatabase,
    tiny_keyword_caps: None,
) -> None:
    """A 409 keyword-cap abort does not create registry subscriptions."""
    application = create_app()
    async with client_for(application) as client:
        await signup(client, "registry-abort@example.com")
        destination = await create_destination(client)
        watchlist = await _create_watchlist(
            client,
            destination["id"],
            name="Capped",
            keywords=["alpha", "beta"],
        )
        before = await api_database.keyword_registry.find({}).to_list(length=None)
        overflow = await client.post(
            f"/api/v1/watchlists/{watchlist['id']}/keywords",
            json={"keyword": "gamma"},
        )
        after = await api_database.keyword_registry.find({}).to_list(length=None)

    assert overflow.status_code == 409
    assert after == before
    assert await database.get_registry_entry("mercari", "gamma") is None


async def test_overlength_keyword_list_returns_422_without_database_write(
    api_database: ApiResourceDatabase,
    tiny_keyword_caps: None,
) -> None:
    """An over-length keyword array is a generic 422 and creates no watchlist."""
    application = create_app()
    async with client_for(application) as client:
        await signup(client, "schema-bound@example.com")
        destination = await create_destination(client)
        created = await client.post(
            "/api/v1/watchlists",
            json={
                "name": "Too Many",
                "keywords": ["alpha", "beta", "gamma"],
                "destination_id": destination["id"],
            },
        )
        listed = await client.get("/api/v1/watchlists")
        patched = await client.post(
            "/api/v1/watchlists",
            json={"name": "Holder", "destination_id": destination["id"]},
        )
        oversized_patch = await client.patch(
            f"/api/v1/watchlists/{patched.json()['id']}",
            json={"keywords": ["alpha", "beta", "gamma"]},
        )

    assert created.status_code == oversized_patch.status_code == 422
    assert created.json() == oversized_patch.json() == _VALIDATION_ERROR
    assert listed.json() == []
    assert oversized_patch.json() == _VALIDATION_ERROR
    holder = await api_database.watchlists.find_one({"name": "Holder"})
    assert holder is not None
    assert holder["keywords"] == []
    leftover = await api_database.keyword_registry.find({}).to_list(length=None)
    assert leftover == []


async def test_boundary_length_keyword_list_is_accepted_then_subject_to_database_cap(
    api_database: ApiResourceDatabase,
    tiny_keyword_caps: None,
) -> None:
    """A list at the request bound passes schema validation and then the stored cap."""
    application = create_app()
    async with client_for(application) as client:
        await signup(client, "boundary-list@example.com")
        destination = await create_destination(client)
        at_bound = await client.post(
            "/api/v1/watchlists",
            json={
                "name": "Exact Bound",
                "keywords": ["alpha", "beta"],
                "destination_id": destination["id"],
            },
        )
        second = await client.post(
            "/api/v1/watchlists",
            json={
                "name": "Second Bound",
                "keywords": ["gamma", "delta"],
                "destination_id": destination["id"],
            },
        )

    assert at_bound.status_code == 201
    assert at_bound.json()["keywords"] == ["alpha", "beta"]
    assert second.status_code == 409
    assert second.json() == _KEYWORD_LIMIT
    leftover = await api_database.watchlists.find({"name": "Second Bound"}).to_list(length=None)
    assert leftover == []
