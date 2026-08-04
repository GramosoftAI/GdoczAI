# -*- coding: utf-8 -*-
"""
royal_tech_schema_loader.py ? Dynamic schema retrieval from the database.

Replaces the previously hardcoded _HEADER_GEMINI_FIELDS constant.

Flow
----
1. Receive PdfClientName + user_id from the API layer.
2. Query document_types  ? get doc_type_id.
3. Query document_schemas ? get schema_json.
4. Validate schema structure.
5. Return schema dict to the processor / batch extractor.

All database I/O is synchronous (psycopg2) so it can be called safely from
the ThreadPoolExecutor workers used by the pipeline.

Usage
-----
    from royal_tech_schema_loader import SchemaLoader

    loader = SchemaLoader()
    schema = loader.load(user_id=42, pdf_client_name="RoyalInvoice")
    # schema is a dict matching the document_schemas.schema_json structure
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

import psycopg2  # pip install psycopg2-binary
import psycopg2.extras
from psycopg2.extensions import connection as PgConnection

from src.services.royal.royal_tech_config import cfg

logger = logging.getLogger(__name__)

# ============================================================================
# Exceptions
# ============================================================================


class SchemaLoaderError(Exception):
    """Base error for all schema-loader failures."""


class DocumentTypeNotFoundError(SchemaLoaderError):
    """Raised when the PdfClientName is not configured for the given user."""


class SchemaNotFoundError(SchemaLoaderError):
    """Raised when a doc_type_id exists but has no schema_json row."""


class SchemaValidationError(SchemaLoaderError):
    """Raised when the fetched schema_json fails structural validation."""


# ============================================================================
# SQL queries
# ============================================================================

_SQL_DOC_TYPE = """
    SELECT doc_type_id
    FROM   document_types
    WHERE  user_id       = %(user_id)s
    AND    document_type = %(document_type)s
    LIMIT  1;
"""

_SQL_SCHEMA = """
    SELECT schema_json
    FROM   document_schemas
    WHERE  doc_type_id = %(doc_type_id)s
    LIMIT  1;
"""

# ============================================================================
# Required top-level keys in a valid schema_json
# ============================================================================
_REQUIRED_SCHEMA_KEYS = {"header_fields", "item_fields"}


# ============================================================================
# DB connection helper
# ============================================================================


def _get_connection() -> PgConnection:
    """
    Open and return a new psycopg2 connection using DatabaseConfig from cfg.

    Callers are responsible for closing the connection (or use a context mgr).
    """
    db = cfg.database
    conn = psycopg2.connect(
        host=db.host,
        port=db.port,
        dbname=db.name,
        user=db.user,
        password=db.password,
        connect_timeout=db.connect_timeout,
    )
    conn.autocommit = True
    return conn


# ============================================================================
# SchemaLoader
# ============================================================================


class SchemaLoader:
    """
    Fetches and validates the Gemini extraction schema from the database.

    Thread safety
    -------------
    Each call to `load()` opens its own short-lived DB connection so the
    instance is safe to share across threads (ThreadPoolExecutor workers).
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self, user_id: int, pdf_client_name: str) -> dict[str, Any]:
        """
        Load the extraction schema for *pdf_client_name* owned by *user_id*.

        Parameters
        ----------
        user_id : int
            The authenticated user ID returned by validate_api_key_from_header.
        pdf_client_name : str
            The document-type name supplied in the request form field
            ``PdfClientName``.

        Returns
        -------
        dict
            The parsed schema_json dict.

        Raises
        ------
        DocumentTypeNotFoundError
            If no matching row exists in document_types.
        SchemaNotFoundError
            If a matching document_type exists but has no schema row.
        SchemaValidationError
            If the schema_json fails structural validation.
        SchemaLoaderError
            On any unexpected database error.
        """
        logger.info(
            "SchemaLoader.load: user_id=%s  pdf_client_name=%r",
            user_id,
            pdf_client_name,
        )

        conn: Optional[PgConnection] = None
        try:
            conn = _get_connection()

            doc_type_id = self._fetch_doc_type_id(conn, user_id, pdf_client_name)
            raw_schema = self._fetch_schema_json(conn, doc_type_id)
            schema = self._parse_schema(raw_schema, doc_type_id)
            self._validate_schema(schema, doc_type_id)

            logger.info(
                "SchemaLoader: schema loaded ? doc_type_id=%s  keys=%s",
                doc_type_id,
                list(schema.keys()),
            )
            return schema

        except (
            DocumentTypeNotFoundError,
            SchemaNotFoundError,
            SchemaValidationError,
        ):
            raise  # already typed ? let caller handle

        except psycopg2.Error as exc:
            logger.error("SchemaLoader DB error: %s", exc)
            raise SchemaLoaderError(f"Database error: {exc}") from exc

        except Exception as exc:
            logger.exception("SchemaLoader unexpected error: %s", exc)
            raise SchemaLoaderError(f"Unexpected error: {exc}") from exc

        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _fetch_doc_type_id(
        self,
        conn: PgConnection,
        user_id: int,
        pdf_client_name: str,
    ) -> int:
        """
        Query document_types and return the matching id.

        SELECT doc_type_id
        FROM   document_types
        WHERE  user_id       = <user_id>
        AND    document_type = <pdf_client_name>
        """
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                _SQL_DOC_TYPE,
                {"user_id": user_id, "document_type": pdf_client_name},
            )
            row = cur.fetchone()

        if row is None:
            logger.warning(
                "SchemaLoader: document_type '%s' not found for user_id=%s",
                pdf_client_name,
                user_id,
            )
            raise DocumentTypeNotFoundError(
                f"Document type '{pdf_client_name}' is not configured "
                f"for user_id={user_id}."
            )

        doc_type_id: int = int(row["doc_type_id"])
        logger.debug(
            "SchemaLoader: resolved '%s' ? doc_type_id=%s",
            pdf_client_name,
            doc_type_id,
        )
        return doc_type_id

    def _fetch_schema_json(
        self,
        conn: PgConnection,
        doc_type_id: int,
    ) -> Any:
        """
        Query document_schemas and return the raw schema_json value.

        SELECT schema_json
        FROM   document_schemas
        WHERE  doc_type_id = <doc_type_id>
        """
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(_SQL_SCHEMA, {"doc_type_id": doc_type_id})
            row = cur.fetchone()

        if row is None:
            logger.warning(
                "SchemaLoader: no schema found for doc_type_id=%s", doc_type_id
            )
            raise SchemaNotFoundError(
                f"No schema configured for doc_type_id={doc_type_id}."
            )

        raw = row["schema_json"]
        logger.debug(
            "SchemaLoader: raw schema fetched for doc_type_id=%s (type=%s)",
            doc_type_id,
            type(raw).__name__,
        )
        return raw

    def _parse_schema(self, raw: Any, doc_type_id: int) -> dict[str, Any]:
        """
        Ensure schema_json is a dict.

        psycopg2 returns JSONB columns as Python dicts automatically.
        If the column is TEXT, attempt JSON deserialization.
        """
        if isinstance(raw, dict):
            return raw

        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    return parsed
                raise SchemaValidationError(
                    f"schema_json for doc_type_id={doc_type_id} must be a JSON object, "
                    f"got {type(parsed).__name__}."
                )
            except json.JSONDecodeError as exc:
                raise SchemaValidationError(
                    f"schema_json for doc_type_id={doc_type_id} is not valid JSON: {exc}"
                ) from exc

        raise SchemaValidationError(
            f"Unexpected schema_json type for doc_type_id={doc_type_id}: "
            f"{type(raw).__name__}."
        )

    def _validate_schema(self, schema: dict[str, Any], doc_type_id: int) -> None:
        """
        Verify the schema dict contains the minimum required keys.

        Required keys: header_fields, item_fields
        If missing, a warning is logged but processing continues --
        resolve_schema_fields() in batch_extractor_helpers will fall back
        to DEFAULT_HEADER_GEMINI_FIELDS / DEFAULT_ITEM_GEMINI_FIELDS.
        """
        missing = _REQUIRED_SCHEMA_KEYS - schema.keys()
        if missing:
            logger.warning(
                "SchemaLoader: schema_json for doc_type_id=%s is missing "
                "keys %s -- batch extractor will use built-in defaults.",
                doc_type_id, sorted(missing),
            )
            return  # non-fatal: defaults will be used downstream

        for key in _REQUIRED_SCHEMA_KEYS:
            val = schema[key]
            if not val:
                logger.warning(
                    "SchemaLoader: schema_json['%s'] for doc_type_id=%s "
                    "is empty -- batch extractor will use built-in defaults.",
                    key, doc_type_id,
                )

        logger.debug(
            "SchemaLoader: validation passed for doc_type_id=%s", doc_type_id
        )


# ============================================================================
# Module-level convenience function
# ============================================================================


def load_schema(user_id: int, pdf_client_name: str) -> dict[str, Any]:
    """
    Module-level shortcut ? creates a SchemaLoader and calls load().

    Example
    -------
        from royal_tech_schema_loader import load_schema

        schema = load_schema(user_id=7, pdf_client_name="RoyalInvoice")
    """
    return SchemaLoader().load(user_id=user_id, pdf_client_name=pdf_client_name)