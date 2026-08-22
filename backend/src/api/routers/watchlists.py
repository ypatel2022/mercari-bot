"""Tenant-scoped watchlist, keyword, preset-copy, and monitoring routes."""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, Response

from ... import database
from ...destinations import DestinationNotFoundError
from ...keyword_registry import normalize_registry_keyword
from ...presets import PresetNotFoundError
from ...watchlists import WatchlistNotFoundError, WatchlistRecord
from ..auth.context import require_tenant_id
from ..schemas import (
    KeywordMutationRequest,
    MonitoringUpdateRequest,
    PresetMutationRequest,
    PublicWatchlist,
    WatchlistCreateRequest,
    WatchlistUpdateRequest,
)

router = APIRouter(prefix="/watchlists", tags=["watchlists"])
TenantId = Annotated[str, Depends(require_tenant_id)]


@router.post("", response_model=PublicWatchlist, status_code=201)
async def create_watchlist(payload: WatchlistCreateRequest, tenant_id: TenantId) -> PublicWatchlist:
    """Create a watchlist that points only to an owned destination."""
    await _require_owned_destination(payload.destination_id, tenant_id)
    watchlist = await database.create_watchlist(
        tenant_id,
        payload.name,
        payload.keywords,
        filters=payload.filters.model_dump(),
        destination_id=payload.destination_id,
        enabled=payload.enabled,
    )
    return PublicWatchlist.from_record(watchlist)


@router.get("", response_model=list[PublicWatchlist])
async def list_watchlists(tenant_id: TenantId) -> list[PublicWatchlist]:
    """List only the authenticated tenant's watchlists in deterministic order."""
    records = await database.list_watchlists_for_owner(tenant_id)
    return [PublicWatchlist.from_record(record) for record in records]


@router.get("/{watchlist_id}", response_model=PublicWatchlist)
async def get_watchlist(watchlist_id: str, tenant_id: TenantId) -> PublicWatchlist:
    """Read one watchlist through an owner-filtered selector."""
    return PublicWatchlist.from_record(await _require_owned_watchlist(watchlist_id, tenant_id))


@router.patch("/{watchlist_id}", response_model=PublicWatchlist)
async def update_watchlist(
    watchlist_id: str,
    payload: WatchlistUpdateRequest,
    tenant_id: TenantId,
) -> PublicWatchlist:
    """Update allowlisted fields on one owned watchlist."""
    changes: dict[str, Any] = payload.model_dump(exclude_unset=True)
    if "destination_id" in changes:
        await _require_owned_destination(changes["destination_id"], tenant_id)
    if "filters" in changes:
        changes["filters"] = payload.filters.model_dump() if payload.filters is not None else None
    record = await database.update_watchlist_for_owner(watchlist_id, tenant_id, **changes)
    return PublicWatchlist.from_record(record)


@router.delete("/{watchlist_id}", status_code=204)
async def delete_watchlist(watchlist_id: str, tenant_id: TenantId) -> Response:
    """Delete one owned watchlist and remove its registry subscriptions."""
    if not await database.delete_watchlist_for_owner(watchlist_id, tenant_id):
        raise WatchlistNotFoundError(watchlist_id)
    return Response(status_code=204)


@router.post("/{watchlist_id}/keywords", response_model=PublicWatchlist)
async def add_keyword(
    watchlist_id: str,
    payload: KeywordMutationRequest,
    tenant_id: TenantId,
) -> PublicWatchlist:
    """Add one normalized keyword, treating duplicates as a successful no-op."""
    keyword = normalize_registry_keyword(payload.keyword)
    updated = await database.add_watchlist_keywords_for_owner(
        watchlist_id,
        tenant_id,
        [keyword],
    )
    return PublicWatchlist.from_record(updated)


@router.delete("/{watchlist_id}/keywords", response_model=PublicWatchlist)
async def remove_keyword(
    watchlist_id: str,
    payload: KeywordMutationRequest,
    tenant_id: TenantId,
) -> PublicWatchlist:
    """Remove one normalized keyword, treating an absent keyword as a no-op."""
    keyword = normalize_registry_keyword(payload.keyword)
    updated = await database.remove_watchlist_keyword_for_owner(watchlist_id, tenant_id, keyword)
    return PublicWatchlist.from_record(updated)


@router.post("/{watchlist_id}/keywords/from-preset", response_model=PublicWatchlist)
async def add_keywords_from_preset(
    watchlist_id: str,
    payload: PresetMutationRequest,
    tenant_id: TenantId,
) -> PublicWatchlist:
    """Copy an enabled preset's normalized keywords into an owned watchlist.

    The copy is all-or-nothing: if the deduplicated post-image would exceed the
    tenant keyword cap, the whole request is rejected and the watchlist is unchanged.
    """
    await _require_owned_watchlist(watchlist_id, tenant_id)
    preset = await database.get_enabled_preset_keyword_by_id(payload.preset_id)
    if preset is None:
        raise PresetNotFoundError(payload.preset_id)

    updated = await database.add_watchlist_keywords_for_owner(watchlist_id, tenant_id, preset.keywords)
    return PublicWatchlist.from_record(updated)


@router.patch("/{watchlist_id}/monitoring", response_model=PublicWatchlist)
async def update_monitoring(
    watchlist_id: str,
    payload: MonitoringUpdateRequest,
    tenant_id: TenantId,
) -> PublicWatchlist:
    """Set one owned watchlist's monitoring state."""
    record = await database.set_watchlist_enabled_for_owner(watchlist_id, tenant_id, payload.enabled)
    return PublicWatchlist.from_record(record)


async def _require_owned_watchlist(watchlist_id: str, tenant_id: str) -> WatchlistRecord:
    watchlist = await database.get_watchlist_for_owner(watchlist_id, tenant_id)
    if watchlist is None:
        raise WatchlistNotFoundError(watchlist_id)
    return watchlist


async def _require_owned_destination(destination_id: str, tenant_id: str) -> None:
    if await database.get_destination_for_owner(destination_id, tenant_id) is None:
        raise DestinationNotFoundError(destination_id)
