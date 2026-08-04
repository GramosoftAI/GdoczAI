# -*- coding: utf-8 -*-
"""
royal_tech_validator.py - Post-merge output validation and QtyCode normalisation.

Responsibilities
----------------
1. Validate the final merged invoice JSON against the Document 2 schema.
   * Top-level keys: Header, ItemsDetails, ShipmentContainerDetails.
   * All Header fields present (nulls allowed, missing keys are errors).
   * ItemsDetails is a list; each item contains all required item fields.
   * ShipmentContainerDetails contains all required container fields.

2. Apply Rule 49: QtyCode normalisation.
   * Read Header.QtyCode (injected by MetadataInjector or left null).
   * Normalise the raw string to a canonical unit code using the Rule 49 map.
   * Write the normalised value back to Header.QtyCode.
   * Normalisation runs AFTER metadata injection so the injected value is
     always processed.

3. Apply per-item ItemQTYCode normalisation using the same Rule 49 map.
   This ensures line-level unit codes are consistent with the header.

Public API
----------
    from royal_tech_validator import InvoiceValidator

    validator = InvoiceValidator()
    result    = validator.validate_and_normalise(invoice_json)

    if result.is_valid:
        clean_json = result.invoice_json
    else:
        print(result.errors)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


# ============================================================================
# Rule 49 - QtyCode / ItemQTYCode canonical normalisation map
# ============================================================================
# Keys   : all accepted raw strings (case-insensitive after strip/upper)
# Values : canonical output code written to the JSON

_QTY_CODE_MAP: dict[str, str] = {
    # Pieces / units
    "PCS":  "PCS",
    "PC":   "PCS",
    "PCE":  "PCS",
    "PIECE": "PCS",
    "PIECES": "PCS",
    "NOS":  "NOS",
    "NO":   "NOS",
    "NUM":  "NOS",
    "NUMBER": "NOS",

    # Sets
    "SET":  "SET",
    "SETS": "SET",

    # Packs / Packets
    "PAK":  "PAK",
    "PACK": "PAK",
    "PAC":  "PAC",
    "PKT":  "PAK",
    "PACKET": "PAK",

    # Metres / Kilometres
    "MTR":  "MTR",
    "MT":   "MTR",
    "METRE": "MTR",
    "METER": "MTR",
    "M":    "MTR",
    "KME":  "KME",
    "KM":   "KME",
    "KILOMETRE": "KME",
    "KILOMETER":  "KME",

    # Feet
    "FTS":  "FTS",
    "FT":   "FTS",
    "FEET": "FTS",
    "FOOT": "FTS",

    # Pairs
    "PRS":  "PRS",
    "PR":   "PRS",
    "PAIR": "PRS",
    "PAIRS": "PRS",

    # Metric tonnes / kilograms
    "MTS":  "MTS",
    "MT ":  "MTS",   # trailing-space variant
    "KGS":  "KGS",
    "KG":   "KGS",
    "KILOGRAM": "KGS",
    "KILOGRAMS": "KGS",
}

# Fields that carry a quantity-unit code in the Header and per-item dicts
_HEADER_QTYCODE_FIELD = "QtyCode"
_ITEM_QTYCODE_FIELD   = "ItemQTYCode"


# ============================================================================
# Required field manifests (Document 2 schema)
# ============================================================================

_ALL_HEADER_FIELDS: tuple[str, ...] = (
    "Branch", "BranchCode", "BuyerAdd1", "BuyerAdd2", "BuyerAdd3",
    "BuyerName", "BuyerOrderNo", "CartonNo", "Client", "Commision",
    "CommonItem", "CompanyID", "ConsigneeAdd1", "ConsigneeAdd2",
    "ConsigneeAdd3", "ConsigneeAdd4", "ConsigneeName", "CountryOfDischarge",
    "Currency", "Discount", "ExporterName", "Freight", "IEC", "Insurance",
    "InvoiceDate", "InvoiceNo", "InvoiceResponseTime", "InvoiceStartTime",
    "InvoiceValue", "JobNo", "JobStatus", "JobType", "KimballNo",
    "NotifyAddress1", "NotifyAddress2", "NotifyAddress3", "NotifyPartyName",
    "OrderNo", "OtherDed", "OtherReference", "Packingcharges", "PageCount",
    "PaymentPeriod", "PdfClientName", "PdfCount", "PortOfDischarge",
    "PortOfFinalDestination", "QtyCode", "SchemeCode", "Status", "Terms",
    "TermsOfPayment", "TotalCBM", "TotalCarton", "TotalGrossWeight",
    "TotalNetWeight", "UserID", "WorkingPeriod",
)

_ALL_ITEM_FIELDS: tuple[str, ...] = (
    "Amount", "DBK_SL_NO", "ExtraItemDesc", "ExtraQuantity",
    "HSNCode", "IGSTAmount", "IGSTRate", "InfoQty", "InfoUnitPrice",
    "ItemDesc", "ItemQTYCode", "Itemslno", "NetWeight",
    "Quantity", "Rate", "TaxableAmount",
)

_ALL_CONTAINER_FIELDS: tuple[str, ...] = (
    "ContainerNo", "ContainerSealDate", "ContainerSealNo",
    "ContainerSize", "Containerslno",
)


# ============================================================================
# Validation result dataclass
# ============================================================================

@dataclass
class ValidationResult:
    """
    Outcome of InvoiceValidator.validate_and_normalise().

    Attributes
    ----------
    is_valid : bool
        True when no structural errors were found.
        QtyCode normalisation warnings do NOT set this to False.
    invoice_json : dict
        The (possibly modified) invoice JSON -- always present.
    errors : list[str]
        Structural validation errors (missing keys, wrong types).
    warnings : list[str]
        Non-fatal notices (unknown QtyCode values, empty ItemsDetails, etc.)
    qty_codes_normalised : int
        Number of QtyCode / ItemQTYCode values that were successfully mapped.
    qty_codes_unknown : list[str]
        Raw values that could not be mapped and were left unchanged.
    """
    is_valid: bool
    invoice_json: dict
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    qty_codes_normalised: int = 0
    qty_codes_unknown: list[str] = field(default_factory=list)

    def log_summary(self) -> None:
        status = "VALID" if self.is_valid else "INVALID"
        logger.info("InvoiceValidator: %s", status)
        if self.errors:
            for err in self.errors:
                logger.error("  ? %s", err)
        if self.warnings:
            for w in self.warnings:
                logger.warning("  ? %s", w)
        logger.info(
            "  QtyCode: normalised=%d  unknown=%s",
            self.qty_codes_normalised,
            self.qty_codes_unknown,
        )


# ============================================================================
# QtyCode normaliser (Rule 49)
# ============================================================================

def _normalise_qty_code(raw: Any) -> tuple[str | None, bool]:
    """
    Normalise a single raw QtyCode value using _QTY_CODE_MAP.

    Parameters
    ----------
    raw : Any
        The raw value from the JSON field (may be None, str, int, etc.)

    Returns
    -------
    (normalised_value, was_mapped) : tuple
        normalised_value -- the canonical code string, or the original if
                           not found in the map, or None if raw was None.
        was_mapped       -- True when a canonical mapping was found.
    """
    if raw is None:
        return None, False

    key = str(raw).strip().upper()
    canonical = _QTY_CODE_MAP.get(key)
    if canonical:
        return canonical, True
    return raw, False   # leave unchanged; caller logs as unknown


# ============================================================================
# InvoiceValidator
# ============================================================================

class InvoiceValidator:
    """
    Validates and normalises the final merged invoice JSON.

    Two operations are always performed in this order:
    1. Structural validation  -- checks required keys / types.
    2. QtyCode normalisation  -- Rule 49 mapping on Header + all items.
    """

    def validate_and_normalise(
        self,
        invoice_json: dict[str, Any],
    ) -> ValidationResult:
        """
        Validate structure and apply Rule 49 QtyCode normalisation.

        Parameters
        ----------
        invoice_json : dict
            Merged invoice JSON after metadata injection.

        Returns
        -------
        ValidationResult
        """
        errors: list[str] = []
        warnings: list[str] = []
        qty_normalised = 0
        qty_unknown: list[str] = []

        if not isinstance(invoice_json, dict):
            return ValidationResult(
                is_valid=False,
                invoice_json=invoice_json,
                errors=[
                    f"invoice_json must be a dict, got {type(invoice_json).__name__}"
                ],
            )

        # -- 1. Structural validation -----------------------------------
        self._validate_top_level(invoice_json, errors)
        if errors:
            # Cannot safely proceed without top-level keys
            return ValidationResult(
                is_valid=False,
                invoice_json=invoice_json,
                errors=errors,
                warnings=warnings,
            )

        self._validate_header(invoice_json["Header"], errors)
        self._validate_items(invoice_json["ItemsDetails"], errors, warnings)
        self._validate_container(
            invoice_json["ShipmentContainerDetails"], errors
        )

        # -- 2. Rule 49 - QtyCode normalisation ------------------------
        qty_normalised, qty_unknown = self._normalise_all_qty_codes(
            invoice_json, warnings
        )

        result = ValidationResult(
            is_valid=len(errors) == 0,
            invoice_json=invoice_json,
            errors=errors,
            warnings=warnings,
            qty_codes_normalised=qty_normalised,
            qty_codes_unknown=qty_unknown,
        )
        result.log_summary()
        return result

    # ------------------------------------------------------------------
    # Structural validators
    # ------------------------------------------------------------------

    def _validate_top_level(
        self, data: dict, errors: list[str]
    ) -> None:
        for key in ("Header", "ItemsDetails", "ShipmentContainerDetails"):
            if key not in data:
                errors.append(f"Missing top-level key: '{key}'")

    def _validate_header(
        self, header: Any, errors: list[str]
    ) -> None:
        if not isinstance(header, dict):
            errors.append(
                f"'Header' must be a dict, got {type(header).__name__}"
            )
            return
        for fname in _ALL_HEADER_FIELDS:
            if fname not in header:
                errors.append(f"Header missing required field: '{fname}'")

    def _validate_items(
        self,
        items: Any,
        errors: list[str],
        warnings: list[str],
    ) -> None:
        if not isinstance(items, list):
            errors.append(
                f"'ItemsDetails' must be a list, got {type(items).__name__}"
            )
            return
        if not items:
            warnings.append("ItemsDetails is empty -- no line items extracted")
            return
        for i, item in enumerate(items):
            if not isinstance(item, dict):
                errors.append(f"ItemsDetails[{i}] is not a dict")
                continue
            for fname in _ALL_ITEM_FIELDS:
                if fname not in item:
                    errors.append(
                        f"ItemsDetails[{i}] missing required field: '{fname}'"
                    )

    def _validate_container(
        self, container: Any, errors: list[str]
    ) -> None:
        if not isinstance(container, dict):
            errors.append(
                f"'ShipmentContainerDetails' must be a dict, "
                f"got {type(container).__name__}"
            )
            return
        for fname in _ALL_CONTAINER_FIELDS:
            if fname not in container:
                errors.append(
                    f"ShipmentContainerDetails missing required field: '{fname}'"
                )

    # ------------------------------------------------------------------
    # Rule 49 - QtyCode normalisation
    # ------------------------------------------------------------------

    def _normalise_all_qty_codes(
        self,
        invoice_json: dict[str, Any],
        warnings: list[str],
    ) -> tuple[int, list[str]]:
        """
        Apply Rule 49 normalisation to:
        - Header.QtyCode
        - ItemsDetails[*].ItemQTYCode  (every line item)

        Returns
        -------
        (count_normalised, unknown_values)
        """
        normalised = 0
        unknown: list[str] = []

        header = invoice_json.get("Header", {})
        items  = invoice_json.get("ItemsDetails", [])

        # -- Header.QtyCode ---------------------------------------------
        raw_header_qty = header.get(_HEADER_QTYCODE_FIELD)
        canonical, mapped = _normalise_qty_code(raw_header_qty)
        if raw_header_qty is not None:
            header[_HEADER_QTYCODE_FIELD] = canonical
            if mapped:
                normalised += 1
                logger.debug(
                    "Rule49: Header.%s %r ? %r",
                    _HEADER_QTYCODE_FIELD, raw_header_qty, canonical,
                )
            else:
                unknown.append(str(raw_header_qty))
                warnings.append(
                    f"Header.QtyCode '{raw_header_qty}' not in Rule 49 map -- left unchanged"
                )

        # -- ItemsDetails[*].ItemQTYCode ---------------------------------
        if isinstance(items, list):
            for i, item in enumerate(items):
                if not isinstance(item, dict):
                    continue
                raw_item_qty = item.get(_ITEM_QTYCODE_FIELD)
                canonical_item, mapped_item = _normalise_qty_code(raw_item_qty)
                if raw_item_qty is not None:
                    item[_ITEM_QTYCODE_FIELD] = canonical_item
                    if mapped_item:
                        normalised += 1
                        logger.debug(
                            "Rule49: ItemsDetails[%d].%s %r ? %r",
                            i, _ITEM_QTYCODE_FIELD, raw_item_qty, canonical_item,
                        )
                    else:
                        if str(raw_item_qty) not in unknown:
                            unknown.append(str(raw_item_qty))
                        warnings.append(
                            f"ItemsDetails[{i}].ItemQTYCode '{raw_item_qty}' "
                            f"not in Rule 49 map -- left unchanged"
                        )

        logger.info(
            "Rule49 normalisation complete -- mapped=%d  unknown=%s",
            normalised, unknown,
        )
        return normalised, unknown


# ============================================================================
# Module-level convenience function
# ============================================================================

def validate_and_normalise(invoice_json: dict[str, Any]) -> ValidationResult:
    """
    Module-level shortcut -- creates an InvoiceValidator and calls
    validate_and_normalise().

    Example
    -------
        from royal_tech_validator import validate_and_normalise

        result = validate_and_normalise(invoice_json)
        if result.is_valid:
            use(result.invoice_json)
    """
    return InvoiceValidator().validate_and_normalise(invoice_json)