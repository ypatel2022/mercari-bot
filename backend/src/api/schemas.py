"""Shared API response schemas."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, StrictBool, field_validator, model_validator

from ..alert_deliveries import AlertDeliveryRecord
from ..config import settings
from ..destinations import DestinationRecord, DestinationType
from ..presets import PresetKeywordRecord
from ..watchlists import WatchlistCondition, WatchlistRecord

BoundedName = Annotated[str, Field(min_length=1, max_length=120)]
BoundedKeyword = Annotated[str, Field(min_length=1, max_length=200)]
ResourceId = Annotated[str, Field(min_length=1, max_length=200)]
WebhookUrl = Annotated[str, Field(min_length=1, max_length=2048)]


def _bound_request_keyword_list(keywords: list[str]) -> list[str]:
    if len(keywords) > settings.max_keywords_per_request:
        raise ValueError("keyword list exceeds the per-request bound")
    return keywords


class StrictRequest(BaseModel):
    """Base request that rejects undeclared client-controlled fields."""

    model_config = ConfigDict(extra="forbid")


class HealthResponse(BaseModel):
    """Health status for the API and its database dependency."""

    status: Literal["ok", "degraded"]
    database: Literal["ok", "unavailable"]


class ErrorResponse(BaseModel):
    """Consistent public error envelope."""

    detail: str
    code: str


class WatchlistFiltersRequest(StrictRequest):
    """Validated watchlist filters accepted from a tenant client."""

    min_price: int | None = Field(default=None, ge=0)
    max_price: int | None = Field(default=None, ge=0)
    condition: WatchlistCondition = WatchlistCondition.ANY

    @model_validator(mode="after")
    def validate_bounds(self) -> Self:
        """Reject inverted inclusive price ranges."""
        if self.min_price is not None and self.max_price is not None and self.min_price > self.max_price:
            raise ValueError("min_price must be less than or equal to max_price")
        return self


class PublicWatchlistFilters(BaseModel):
    """Public normalized watchlist filters."""

    min_price: int | None
    max_price: int | None
    condition: WatchlistCondition


class WatchlistCreateRequest(StrictRequest):
    """Fields accepted when creating an owned watchlist."""

    name: BoundedName
    keywords: list[BoundedKeyword] = Field(default_factory=list)
    filters: WatchlistFiltersRequest = Field(default_factory=WatchlistFiltersRequest)
    destination_id: ResourceId
    enabled: StrictBool = True

    @field_validator("keywords")
    @classmethod
    def bound_keyword_list_length(cls, keywords: list[str]) -> list[str]:
        """Reject keyword arrays longer than ``max_keywords_per_request``."""
        return _bound_request_keyword_list(keywords)


class WatchlistUpdateRequest(StrictRequest):
    """Allowlisted partial watchlist mutation."""

    name: BoundedName | None = None
    keywords: list[BoundedKeyword] | None = None
    filters: WatchlistFiltersRequest | None = None
    destination_id: ResourceId | None = None
    enabled: StrictBool | None = None

    @field_validator("keywords")
    @classmethod
    def bound_keyword_list_length(cls, keywords: list[str] | None) -> list[str] | None:
        """Reject keyword arrays longer than ``max_keywords_per_request``."""
        if keywords is None:
            return None
        return _bound_request_keyword_list(keywords)

    @model_validator(mode="after")
    def validate_patch(self) -> Self:
        """Require at least one non-null field while preserving omission semantics."""
        if not self.model_fields_set:
            raise ValueError("at least one watchlist field is required")
        if any(getattr(self, field_name) is None for field_name in self.model_fields_set):
            raise ValueError("watchlist fields cannot be null")
        return self


class KeywordMutationRequest(StrictRequest):
    """One custom watchlist keyword mutation."""

    keyword: BoundedKeyword


class PresetMutationRequest(StrictRequest):
    """One enabled preset to copy into a watchlist."""

    preset_id: ResourceId


class MonitoringUpdateRequest(StrictRequest):
    """Explicit watchlist monitoring state."""

    enabled: StrictBool


class PublicWatchlist(BaseModel):
    """Public watchlist representation with ownership metadata omitted."""

    id: str
    name: str
    keywords: list[str]
    filters: PublicWatchlistFilters
    destination_id: str
    enabled: bool
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_record(cls, record: WatchlistRecord) -> Self:
        """Build the public allowlist from a stored watchlist record."""
        return cls(
            id=record._id,
            name=record.name,
            keywords=record.keywords,
            filters=PublicWatchlistFilters(
                min_price=record.filters.min_price,
                max_price=record.filters.max_price,
                condition=record.filters.condition,
            ),
            destination_id=record.destination_id,
            enabled=record.enabled,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )


class DestinationCreateRequest(StrictRequest):
    """Discord webhook destination creation fields."""

    label: BoundedName
    webhook_url: WebhookUrl
    type: Literal[DestinationType.DISCORD_WEBHOOK] = DestinationType.DISCORD_WEBHOOK


class DestinationUpdateRequest(StrictRequest):
    """Allowlisted partial destination mutation."""

    label: BoundedName | None = None
    webhook_url: WebhookUrl | None = None

    @model_validator(mode="after")
    def validate_patch(self) -> Self:
        """Require a non-null label or webhook replacement."""
        if not self.model_fields_set:
            raise ValueError("at least one destination field is required")
        if any(getattr(self, field_name) is None for field_name in self.model_fields_set):
            raise ValueError("destination fields cannot be null")
        return self


class PublicDestination(BaseModel):
    """Secret-free destination metadata."""

    id: str
    type: DestinationType
    label: str
    verified_at: datetime | None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_record(cls, record: DestinationRecord) -> Self:
        """Build public metadata without serializing webhook credentials."""
        return cls(
            id=record._id,
            type=record.type,
            label=record.label,
            verified_at=record.verified_at,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )


class PublicPreset(BaseModel):
    """Enabled read-only preset choice."""

    id: str
    marketplace: str
    name: str
    keywords: list[str]

    @classmethod
    def from_record(cls, record: PresetKeywordRecord) -> Self:
        """Build the public preset allowlist."""
        return cls(
            id=record._id,
            marketplace=record.marketplace,
            name=record.name,
            keywords=record.keywords,
        )


class PublicAlertDelivery(BaseModel):
    """Allowlisted recent alert delivery fields."""

    id: str
    listing_id: str
    destination_id: str
    marketplace: str
    title: str
    canonical_url: str
    matched_keywords: list[str]
    status: Literal["sent"]
    created_at: datetime
    delivered_at: datetime | None

    @classmethod
    def from_record(cls, record: AlertDeliveryRecord) -> Self:
        """Build a public recent-alert item."""
        return cls(
            id=record._id,
            listing_id=record.listing_id,
            destination_id=record.destination_id,
            marketplace=record.marketplace,
            title=record.title,
            canonical_url=record.canonical_url,
            matched_keywords=record.matched_keywords,
            status="sent",
            created_at=record.created_at,
            delivered_at=record.delivered_at,
        )


class RecentAlertsPage(BaseModel):
    """Cursor-paginated tenant recent-alert page."""

    items: list[PublicAlertDelivery]
    next_cursor: str | None
