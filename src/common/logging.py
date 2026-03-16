from __future__ import annotations

import logging
import sys
from typing import Any

import orjson
import structlog


def setup_logging(level: str = "INFO") -> None:
    structlog.reset_defaults()
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True, key="timestamp"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(serializer=_json_dumps),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=False,
    )


def get_logger(**initial_bindings: Any):
    return structlog.get_logger(**initial_bindings)


def _json_dumps(obj: dict[str, Any], **_: Any) -> str:
    return orjson.dumps(obj).decode()
