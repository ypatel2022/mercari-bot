"""Domain-exception to HTTP-error mappings."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from ..database import KeywordLimitExceededError, KeywordMutationTransactionRequiredError
from ..destinations import (
    DestinationInUseError,
    DestinationLabelExistsError,
    DestinationNotFoundError,
    InvalidWebhookUrlError,
)
from ..logging_utils import get_logger
from ..presets import PresetNotFoundError
from ..users import EmailAlreadyExistsError
from ..watchlists import WatchlistNameExistsError, WatchlistNotFoundError
from ..webhook_errors import WebhookDeliveryError, WebhookPermanentError, WebhookTransientError
from .auth.exceptions import AuthenticationRequiredError, InvalidCredentialsError
from .schemas import ErrorResponse

_logger = get_logger("api")
_ErrorHandler = Callable[[Request, Exception], Awaitable[JSONResponse]]


def register_exception_handlers(app: FastAPI) -> None:
    """Register public, secret-safe responses for domain and unexpected errors."""
    app.add_exception_handler(
        EmailAlreadyExistsError,
        _build_error_handler(409, "An account with this email already exists", "email_exists"),
    )
    app.add_exception_handler(
        WatchlistNameExistsError,
        _build_error_handler(409, "A watchlist with this name already exists", "watchlist_name_exists"),
    )
    app.add_exception_handler(
        KeywordLimitExceededError,
        _build_error_handler(409, "Keyword limit reached", "keyword_limit_exceeded"),
    )
    app.add_exception_handler(
        KeywordMutationTransactionRequiredError,
        _build_error_handler(
            503,
            "Keyword updates require a transaction-capable MongoDB",
            "keyword_transaction_unavailable",
        ),
    )
    app.add_exception_handler(
        DestinationLabelExistsError,
        _build_error_handler(409, "A destination with this label already exists", "destination_label_exists"),
    )
    app.add_exception_handler(
        WatchlistNotFoundError,
        _build_error_handler(404, "Resource not found", "not_found"),
    )
    app.add_exception_handler(
        DestinationNotFoundError,
        _build_error_handler(404, "Resource not found", "not_found"),
    )
    app.add_exception_handler(
        PresetNotFoundError,
        _build_error_handler(404, "Resource not found", "not_found"),
    )
    app.add_exception_handler(
        InvalidWebhookUrlError,
        _build_error_handler(422, "Invalid webhook URL", "invalid_webhook_url"),
    )
    app.add_exception_handler(
        DestinationInUseError,
        _build_error_handler(409, "Destination is used by a watchlist", "destination_in_use"),
    )
    app.add_exception_handler(
        WebhookPermanentError,
        _build_error_handler(422, "Webhook was rejected", "webhook_rejected"),
    )
    app.add_exception_handler(
        WebhookTransientError,
        _build_error_handler(503, "Webhook service is temporarily unavailable", "webhook_unavailable"),
    )
    app.add_exception_handler(
        WebhookDeliveryError,
        _build_error_handler(502, "Webhook verification failed", "webhook_verification_failed"),
    )
    app.add_exception_handler(
        InvalidCredentialsError,
        _build_error_handler(401, "Invalid email or password", "invalid_credentials"),
    )
    app.add_exception_handler(
        AuthenticationRequiredError,
        _build_error_handler(401, "Authentication required", "authentication_required"),
    )
    app.add_exception_handler(
        RequestValidationError,
        _build_error_handler(422, "Invalid request", "validation_error"),
    )
    app.add_exception_handler(
        ValueError,
        _build_error_handler(422, "Invalid value", "validation_error"),
    )
    app.add_exception_handler(Exception, _unhandled_exception_handler)


def _build_error_handler(status_code: int, detail: str, code: str) -> _ErrorHandler:
    async def handler(_: Request, __: Exception) -> JSONResponse:
        return _error_response(status_code, detail, code)

    return handler


async def _unhandled_exception_handler(_: Request, exc: Exception) -> JSONResponse:
    safe_exception = RuntimeError(f"Unhandled {type(exc).__name__}; exception details redacted")
    _logger.error(
        "Unhandled API exception",
        exc_info=(type(safe_exception), safe_exception, exc.__traceback__),
    )
    return _error_response(500, "Internal server error", "internal_error")


def _error_response(status_code: int, detail: str, code: str) -> JSONResponse:
    body = ErrorResponse(detail=detail, code=code)
    return JSONResponse(status_code=status_code, content=body.model_dump())
