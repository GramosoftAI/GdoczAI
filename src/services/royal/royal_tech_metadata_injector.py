# -*- coding: utf-8 -*-
"""
royal_tech_metadata_injector.py - Inject request metadata into the final invoice JSON.

Responsibilities
----------------
1. Accept the merged invoice JSON (Header / ItemsDetails / ShipmentContainerDetails)
   and the metadata dict collected from the API request form fields.
2. Map each metadata field to its corresponding Header key.
3. Only fill fields that are currently null -- never overwrite a non-null value
   that Gemini successfully extracted.
4. Validate that the field being injected is in the permitted HEADER_NULL_FIELDS
   set before writing it.
5. Handle FileName as a special-case injection (mapped to no Header key directly;
   stored as-is for pipeline_meta only -- logged but not injected unless the
   target field is null).
6. Return the updated invoice JSON dict.

Usage
-----
    from royal_tech_metadata_injector import MetadataInjector

    injector = MetadataInjector()
    updated  = injector.inject(invoice_json, metadata)
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


# ============================================================================
# Fields permitted for null-fill injection (mirrors ExtractionSchemaConfig)
# ============================================================================

# Complete set of header fields that the pipeline leaves null by default.
# Metadata injection is ONLY allowed for fields in this set.
HEADER_NULL_FIELDS: frozenset[str] = frozenset({
    "Branch",
    "BranchCode",
    "BuyerOrderNo",
    "Commision",
    "CommonItem",
    "CompanyID",
    "Discount",
    "Freight",
    "IEC",
    "Insurance",
    "InvoiceResponseTime",
    "InvoiceStartTime",
    "JobNo",
    "JobStatus",
    "JobType",
    "KimballNo",
    "NotifyAddress1",
    "NotifyAddress2",
    "NotifyAddress3",
    "NotifyPartyName",
    "OrderNo",
    "OtherDed",
    "OtherReference",
    "Packingcharges",
    "PageCount",
    "PdfClientName",
    "PdfCount",
    "PortOfFinalDestination",
    "QtyCode",
    "SchemeCode",
    "TermsOfPayment",
    "TotalCBM",
    "UserID",
    "WorkingPeriod",
})


# ============================================================================
# Metadata field ? Header key mapping
# ============================================================================

# Maps each API request form field name to its target Header JSON key.
# Fields absent from this map are not injected into the Header.
_METADATA_TO_HEADER: dict[str, str] = {
    "CompanyID":       "CompanyID",
    "UserID":          "UserID",
    "BranchCode":      "BranchCode",
    "JobNo":           "JobNo",
    "JobType":         "JobType",
    "WorkingPeriod":   "WorkingPeriod",
    "PageCount":       "PageCount",
    "PdfCount":        "PdfCount",
    "JobStatus":       "JobStatus",
    "InvoiceStartTime": "InvoiceStartTime",
    "PdfClientName":   "PdfClientName",
    # FileName is a special case handled separately -- not mapped here
}

# FileName is injected only when the Header has no InvoiceNo yet.
# Override this key if the schema maps FileName differently.
_FILENAME_FALLBACK_KEY = "InvoiceNo"


# ============================================================================
# MetadataInjector
# ============================================================================

class MetadataInjector:
    """
    Injects API request metadata into the Header section of the invoice JSON.

    Rules
    -----
    * Only fields listed in HEADER_NULL_FIELDS may be injected.
    * A field is written only if its current value is None / absent.
    * Non-null Gemini-extracted values are NEVER overwritten.
    * FileName is injected as a fallback for InvoiceNo when that field is null.
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def inject(
        self,
        invoice_json: dict[str, Any],
        metadata: dict[str, str],
    ) -> dict[str, Any]:
        """
        Inject metadata fields into the Header of invoice_json.

        Parameters
        ----------
        invoice_json : dict
            The merged invoice JSON produced by the pipeline.
            Must contain a "Header" key whose value is a dict.
        metadata : dict
            Form-field values from the API request:
            {CompanyID, UserID, BranchCode, JobNo, JobType,
             WorkingPeriod, PageCount, PdfCount, JobStatus,
             FileName, InvoiceStartTime, PdfClientName}

        Returns
        -------
        dict
            The same invoice_json dict with Header fields populated.
            The dict is modified in-place AND returned for convenience.
        """
        if not isinstance(invoice_json, dict):
            logger.error(
                "MetadataInjector.inject: invoice_json must be a dict, got %s",
                type(invoice_json).__name__,
            )
            return invoice_json

        header = invoice_json.get("Header")
        if not isinstance(header, dict):
            logger.error(
                "MetadataInjector.inject: 'Header' key missing or not a dict"
            )
            return invoice_json

        injected: list[str] = []
        skipped_non_null: list[str] = []
        skipped_not_permitted: list[str] = []

        # -- Standard field injection -----------------------------------
        for meta_key, header_key in _METADATA_TO_HEADER.items():
            meta_value = metadata.get(meta_key)
            if meta_value is None:
                continue  # nothing to inject for this key

            result = self._inject_field(
                header=header,
                header_key=header_key,
                value=meta_value,
            )

            if result == "injected":
                injected.append(header_key)
            elif result == "non_null":
                skipped_non_null.append(header_key)
            elif result == "not_permitted":
                skipped_not_permitted.append(header_key)

        # -- FileName special-case injection ----------------------------
        filename = metadata.get("FileName")
        if filename:
            result = self._inject_filename(header, filename)
            if result == "injected":
                injected.append(f"{_FILENAME_FALLBACK_KEY}(from FileName)")

        # -- Logging summary --------------------------------------------
        logger.info(
            "MetadataInjector: injected=%s  skipped_non_null=%s  not_permitted=%s",
            injected,
            skipped_non_null,
            skipped_not_permitted,
        )

        if skipped_not_permitted:
            logger.warning(
                "MetadataInjector: fields not in HEADER_NULL_FIELDS -- skipped: %s",
                skipped_not_permitted,
            )

        return invoice_json

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _inject_field(
        self,
        header: dict[str, Any],
        header_key: str,
        value: Any,
    ) -> str:
        """
        Attempt to write *value* into header[header_key].

        Returns
        -------
        str
            "injected"      -- field was null/absent and value was written.
            "non_null"      -- field already has a non-null value; skipped.
            "not_permitted" -- header_key not in HEADER_NULL_FIELDS; skipped.
        """
        if header_key not in HEADER_NULL_FIELDS:
            logger.debug(
                "MetadataInjector: '%s' not in HEADER_NULL_FIELDS -- skip",
                header_key,
            )
            return "not_permitted"

        current = header.get(header_key)
        if current is not None:
            logger.debug(
                "MetadataInjector: '%s' already has value %r -- skip",
                header_key,
                current,
            )
            return "non_null"

        header[header_key] = value
        logger.debug(
            "MetadataInjector: '%s' ? %r",
            header_key,
            value,
        )
        return "injected"

    def _inject_filename(self, header: dict[str, Any], filename: str) -> str:
        """
        Inject FileName as a fallback value for InvoiceNo when that field
        is currently null.

        FileName is a special case -- it is not a direct header-field name in
        the Document 2 schema, so it is only used as a last-resort fallback
        to avoid a completely null InvoiceNo.

        Returns "injected" or "non_null" (same semantics as _inject_field).
        """
        current = header.get(_FILENAME_FALLBACK_KEY)
        if current is not None:
            logger.debug(
                "MetadataInjector: FileName fallback skipped -- "
                "'%s' already has value %r",
                _FILENAME_FALLBACK_KEY,
                current,
            )
            return "non_null"

        # InvoiceNo is NOT in HEADER_NULL_FIELDS (Gemini extracts it),
        # so we skip the permission check here -- this is an explicit fallback.
        header[_FILENAME_FALLBACK_KEY] = filename
        logger.info(
            "MetadataInjector: '%s' ? FileName fallback %r",
            _FILENAME_FALLBACK_KEY,
            filename,
        )
        return "injected"


# ============================================================================
# Module-level convenience function
# ============================================================================

def inject_metadata(
    invoice_json: dict[str, Any],
    metadata: dict[str, str],
) -> dict[str, Any]:
    """
    Module-level shortcut -- creates a MetadataInjector and calls inject().

    Example
    -------
        from royal_tech_metadata_injector import inject_metadata

        updated = inject_metadata(invoice_json, metadata)
    """
    return MetadataInjector().inject(invoice_json, metadata)