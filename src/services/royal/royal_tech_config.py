# -*- coding: utf-8 -*-
"""
royal_tech_config.py - Centralised configuration loader for the Royal Tech
Invoice Extraction System.

Reads all settings from royal_config.yaml (located in the same directory or
at the path given by the ROYAL_CONFIG_PATH environment variable).

Usage
-----
    from royal_tech_config import cfg

    gemini_key  = cfg.gemini.api_key
    batch_size  = cfg.batch_manager.batch_size
    db_host     = cfg.database.host
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml  # pip install pyyaml

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Locate the YAML file
# ---------------------------------------------------------------------------
_DEFAULT_YAML_NAME = "royal_config.yaml"


def _find_yaml_path() -> Path:
    """
    Resolution order:
    1. ROYAL_CONFIG_PATH environment variable (absolute or relative).
    2. Same directory as this file.
    3. Current working directory.
    """
    env_path = os.environ.get("ROYAL_CONFIG_PATH")
    if env_path:
        p = Path(env_path)
        if p.exists():
            return p
        raise FileNotFoundError(
            f"ROYAL_CONFIG_PATH is set to '{env_path}' but the file does not exist."
        )

    candidates = [
        Path(__file__).parent / _DEFAULT_YAML_NAME,
        Path.cwd() / _DEFAULT_YAML_NAME,
    ]
    for c in candidates:
        if c.exists():
            return c

    raise FileNotFoundError(
        f"'{_DEFAULT_YAML_NAME}' not found. "
        "Place it next to royal_tech_config.py or set ROYAL_CONFIG_PATH."
    )


def _load_yaml(path: Path) -> dict:
    # Read as bytes, decode with Windows-1252 fallback to handle
    # files saved on Windows with non-UTF-8 characters in comments.
    raw = path.read_bytes()
    try:
        content = raw.decode("utf-8")
    except UnicodeDecodeError:
        # Decode as Windows-1252; YAML structure is pure ASCII so this is safe
        content = raw.decode("windows-1252", errors="replace")
    data = yaml.safe_load(content) or {}
    logger.debug("Loaded configuration from: %s", path)
    return data


# ============================================================================
# Sub-config dataclasses
# ============================================================================

@dataclass
class DatabaseConfig:
    """PostgreSQL / relational DB settings."""

    host: str = "localhost"
    port: int = 5432
    name: str = "royal_tech_db"
    user: str = "postgres"
    password: str = ""
    pool_min_size: int = 1
    pool_max_size: int = 10
    connect_timeout: int = 10

    @classmethod
    def from_dict(cls, d: dict) -> "DatabaseConfig":
        return cls(
            host=d.get("host", cls.host),
            port=int(d.get("port", cls.port)),
            name=d.get("name", cls.name),
            user=d.get("user", cls.user),
            password=d.get("password", cls.password),
            pool_min_size=int(d.get("pool_min_size", cls.pool_min_size)),
            pool_max_size=int(d.get("pool_max_size", cls.pool_max_size)),
            connect_timeout=int(d.get("connect_timeout", cls.connect_timeout)),
        )


@dataclass
class OLMOCRConfig:
    """Settings that drive the OCR processor (MinerU / OLMOCR)."""

    api_key: str = ""
    model: str = "allenai/olmOCR-2-7B-1025"
    timeout: int = 600
    batch_size: int = 3
    pdf_dpi: int = 200
    image_format: str = "PNG"
    empty_page_variance_threshold: float = 100.0
    empty_page_bright_threshold: float = 250.0
    empty_page_dark_threshold: float = 5.0
    max_retries_per_page: int = 2
    max_tokens_per_page: int = 8192
    temperature: float = 0.0
    top_p: float = 0.95
    page_separator: str = "\n\n---PAGE {page_num}---\n\n"

    @classmethod
    def from_dict(cls, d: dict) -> "OLMOCRConfig":
        return cls(
            api_key=d.get("api_key", os.environ.get("DEEPINFRA_API_KEY", "")),
            model=d.get("model", cls.model),
            timeout=int(d.get("timeout", cls.timeout)),
            batch_size=int(d.get("batch_size", cls.batch_size)),
            pdf_dpi=int(d.get("pdf_dpi", cls.pdf_dpi)),
            image_format=d.get("image_format", cls.image_format),
            empty_page_variance_threshold=float(
                d.get("empty_page_variance_threshold", cls.empty_page_variance_threshold)
            ),
            empty_page_bright_threshold=float(
                d.get("empty_page_bright_threshold", cls.empty_page_bright_threshold)
            ),
            empty_page_dark_threshold=float(
                d.get("empty_page_dark_threshold", cls.empty_page_dark_threshold)
            ),
            max_retries_per_page=int(d.get("max_retries_per_page", cls.max_retries_per_page)),
            max_tokens_per_page=int(d.get("max_tokens_per_page", cls.max_tokens_per_page)),
            temperature=float(d.get("temperature", cls.temperature)),
            top_p=float(d.get("top_p", cls.top_p)),
            page_separator=d.get("page_separator", cls.page_separator),
        )


@dataclass
class GeminiConfig:
    """Settings for all Gemini 2.5 Flash calls."""

    api_key: str = ""
    model: str = "gemini-2.5-flash"
    api_base_url: str = "https://generativelanguage.googleapis.com/v1beta/models"
    temperature: float = 0.0
    top_p: float = 0.95
    top_k: int = 40
    max_output_tokens: int = 65535
    timeout: int = 300
    max_retries: int = 3
    retry_backoff_base: float = 2.0

    @classmethod
    def from_dict(cls, d: dict) -> "GeminiConfig":
        return cls(
            api_key=d.get("api_key", os.environ.get("GEMINI_API_KEY", "")),
            model=d.get("model", cls.model),
            api_base_url=d.get("api_base_url", cls.api_base_url),
            temperature=float(d.get("temperature", cls.temperature)),
            top_p=float(d.get("top_p", cls.top_p)),
            top_k=int(d.get("top_k", cls.top_k)),
            max_output_tokens=int(d.get("max_output_tokens", cls.max_output_tokens)),
            timeout=int(d.get("timeout", cls.timeout)),
            max_retries=int(d.get("max_retries", cls.max_retries)),
            retry_backoff_base=float(d.get("retry_backoff_base", cls.retry_backoff_base)),
        )


@dataclass
class PageDetectorConfig:
    """STEP 2 - identify which pages contain the main line-item table."""

    max_output_tokens: int = 65535
    fallback_to_all_pages_on_empty: bool = True

    @classmethod
    def from_dict(cls, d: dict) -> "PageDetectorConfig":
        return cls(
            max_output_tokens=int(d.get("max_output_tokens", cls.max_output_tokens)),
            fallback_to_all_pages_on_empty=bool(
                d.get("fallback_to_all_pages_on_empty", cls.fallback_to_all_pages_on_empty)
            ),
        )


@dataclass
class IdentifierExtractorConfig:
    """STEP 3 - per-page line-item identifier extraction."""

    max_output_tokens: int = 65535
    serial_zero_pad: int = 5
    identifier_type_material: str = "material_id"
    identifier_type_description: str = "description"
    skip_unidentifiable_rows: bool = True

    @classmethod
    def from_dict(cls, d: dict) -> "IdentifierExtractorConfig":
        return cls(
            max_output_tokens=int(d.get("max_output_tokens", cls.max_output_tokens)),
            serial_zero_pad=int(d.get("serial_zero_pad", cls.serial_zero_pad)),
            identifier_type_material=d.get(
                "identifier_type_material", cls.identifier_type_material
            ),
            identifier_type_description=d.get(
                "identifier_type_description", cls.identifier_type_description
            ),
            skip_unidentifiable_rows=bool(
                d.get("skip_unidentifiable_rows", cls.skip_unidentifiable_rows)
            ),
        )


@dataclass
class BatchManagerConfig:
    """STEP 4 - identifier grouping into extraction batches."""

    batch_size: int = 10
    single_shot_threshold: int = 0

    @classmethod
    def from_dict(cls, d: dict) -> "BatchManagerConfig":
        return cls(
            batch_size=int(d.get("batch_size", cls.batch_size)),
            single_shot_threshold=int(
                d.get("single_shot_threshold", cls.single_shot_threshold)
            ),
        )


@dataclass
class BatchExtractorConfig:
    """STEP 5 - full structured extraction per batch."""

    max_output_tokens: int = 65535
    extract_header_in_every_batch: bool = True
    extract_container_in_every_batch: bool = True
    null_placeholder: Optional[str] = None

    @classmethod
    def from_dict(cls, d: dict) -> "BatchExtractorConfig":
        return cls(
            max_output_tokens=int(d.get("max_output_tokens", cls.max_output_tokens)),
            extract_header_in_every_batch=bool(
                d.get("extract_header_in_every_batch", cls.extract_header_in_every_batch)
            ),
            extract_container_in_every_batch=bool(
                d.get("extract_container_in_every_batch", cls.extract_container_in_every_batch)
            ),
            null_placeholder=d.get("null_placeholder", cls.null_placeholder),
        )


@dataclass
class MergerConfig:
    """STEP 6 - merging batch results into the final unified JSON."""

    primary_batch_index: int = 0
    deduplicate_on_exact_identifier: bool = False

    @classmethod
    def from_dict(cls, d: dict) -> "MergerConfig":
        return cls(
            primary_batch_index=int(d.get("primary_batch_index", cls.primary_batch_index)),
            deduplicate_on_exact_identifier=bool(
                d.get("deduplicate_on_exact_identifier", cls.deduplicate_on_exact_identifier)
            ),
        )


@dataclass
class LoggingConfig:
    """Logging behaviour across the pipeline."""

    level: str = "INFO"
    format: str = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    datefmt: str = "%Y-%m-%d %H:%M:%S"
    log_file: Optional[str] = None

    @classmethod
    def from_dict(cls, d: dict) -> "LoggingConfig":
        return cls(
            level=d.get("level", cls.level).upper(),
            format=d.get("format", cls.format),
            datefmt=d.get("datefmt", cls.datefmt),
            log_file=d.get("log_file", cls.log_file),
        )


@dataclass
class PipelineConfig:
    """Top-level toggles for the overall extraction pipeline."""

    work_dir: str = "/tmp/royal_tech_extraction"
    debug_save_intermediate: bool = False
    max_pages: Optional[int] = None
    supported_extensions: tuple = (".pdf",)
    pipeline_workers: int = 4

    @classmethod
    def from_dict(cls, d: dict) -> "PipelineConfig":
        exts = d.get("supported_extensions", [".pdf"])
        if isinstance(exts, list):
            exts = tuple(exts)
        return cls(
            work_dir=d.get("work_dir", cls.work_dir),
            debug_save_intermediate=bool(
                d.get("debug_save_intermediate", cls.debug_save_intermediate)
            ),
            max_pages=d.get("max_pages", cls.max_pages),
            supported_extensions=exts,
            pipeline_workers=int(d.get("pipeline_workers", cls.pipeline_workers)),
        )


@dataclass
class APIConfig:
    """FastAPI server settings."""

    host: str = "0.0.0.0"
    port: int = 8000
    reload: bool = False
    workers: int = 1
    cors_origins: list = field(default_factory=lambda: ["*"])

    @classmethod
    def from_dict(cls, d: dict) -> "APIConfig":
        return cls(
            host=d.get("host", cls.host),
            port=int(d.get("port", cls.port)),
            reload=bool(d.get("reload", cls.reload)),
            workers=int(d.get("workers", cls.workers)),
            cors_origins=d.get("cors_origins", ["*"]),
        )


# ============================================================================
# Master config object
# ============================================================================

@dataclass
class SchemaConfig:
    """Schema field lists loaded from the yaml schema: section."""
    header_always_null_fields: list = field(default_factory=list)
    header_default_fields: dict = field(default_factory=dict)
    items_always_null_fields: list = field(default_factory=list)
    container_always_null_fields: list = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict) -> "SchemaConfig":
        return cls(
            header_always_null_fields=list(d.get("header_always_null_fields", [])),
            header_default_fields=dict(d.get("header_default_fields", {})),
            items_always_null_fields=list(d.get("items_always_null_fields", [])),
            container_always_null_fields=list(d.get("container_always_null_fields", [])),
        )



@dataclass
class RoyalConfig:
    """
    Aggregate configuration loaded from royal_config.yaml.

    Import the module-level singleton:
        from royal_tech_config import cfg
    """

    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    olmocr: OLMOCRConfig = field(default_factory=OLMOCRConfig)
    gemini: GeminiConfig = field(default_factory=GeminiConfig)
    page_detector: PageDetectorConfig = field(default_factory=PageDetectorConfig)
    identifier_extractor: IdentifierExtractorConfig = field(
        default_factory=IdentifierExtractorConfig
    )
    batch_manager: BatchManagerConfig = field(default_factory=BatchManagerConfig)
    batch_extractor: BatchExtractorConfig = field(default_factory=BatchExtractorConfig)
    merger: MergerConfig = field(default_factory=MergerConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)
    api: APIConfig = field(default_factory=APIConfig)
    schema: SchemaConfig = field(default_factory=SchemaConfig)

    # ------------------------------------------------------------------ #
    # Factory                                                              #
    # ------------------------------------------------------------------ #

    @classmethod
    def load(cls, yaml_path: Optional[Path] = None) -> "RoyalConfig":
        """
        Load and return a RoyalConfig from a YAML file.

        Parameters
        ----------
        yaml_path : Path | None
            Explicit path to the YAML file.  When None, _find_yaml_path() is
            used to locate royal_config.yaml automatically.
        """
        path = yaml_path or _find_yaml_path()
        raw = _load_yaml(path)

        instance = cls(
            database=DatabaseConfig.from_dict(raw.get("database", {})),
            olmocr=OLMOCRConfig.from_dict(raw.get("olmocr", {})),
            gemini=GeminiConfig.from_dict(raw.get("gemini", {})),
            page_detector=PageDetectorConfig.from_dict(raw.get("page_detector", {})),
            identifier_extractor=IdentifierExtractorConfig.from_dict(
                raw.get("identifier_extractor", {})
            ),
            batch_manager=BatchManagerConfig.from_dict(raw.get("batch_manager", {})),
            batch_extractor=BatchExtractorConfig.from_dict(raw.get("batch_extractor", {})),
            merger=MergerConfig.from_dict(raw.get("merger", {})),
            logging=LoggingConfig.from_dict(raw.get("logging", {})),
            pipeline=PipelineConfig.from_dict(raw.get("pipeline", {})),
            api=APIConfig.from_dict(raw.get("api", {})),
            schema=SchemaConfig.from_dict(raw.get("schema", {})),
        )

        logger.debug("RoyalConfig loaded successfully from %s", path)
        return instance

    # ------------------------------------------------------------------ #
    # Validation                                                           #
    # ------------------------------------------------------------------ #

    def validate(self) -> None:
        """
        Raise ValueError for any obviously broken configuration.
        Call this once at startup before running the pipeline.
        """
        errors: list[str] = []

        if not self.olmocr.api_key:
            errors.append(
                "OLMOCRConfig.api_key is empty. "
                "Set 'olmocr.api_key' in royal_config.yaml "
                "or export DEEPINFRA_API_KEY."
            )

        if not self.gemini.api_key:
            errors.append(
                "GeminiConfig.api_key is empty. "
                "Set 'gemini.api_key' in royal_config.yaml "
                "or export GEMINI_API_KEY."
            )

        if self.batch_manager.batch_size < 1:
            errors.append("BatchManagerConfig.batch_size must be >= 1.")

        if self.olmocr.pdf_dpi < 72:
            errors.append("OLMOCRConfig.pdf_dpi must be >= 72.")

        if not self.database.host:
            errors.append("DatabaseConfig.host must not be empty.")

        if errors:
            raise ValueError(
                "RoyalConfig validation failed:\n"
                + "\n".join(f"  * {e}" for e in errors)
            )


# ============================================================================
# Module-level singleton
# ============================================================================

def _build_cfg() -> RoyalConfig:
    """Build the singleton, falling back to defaults if YAML is missing."""
    try:
        return RoyalConfig.load()
    except FileNotFoundError as exc:
        logger.warning(
            "royal_config.yaml not found -- using built-in defaults. (%s)", exc
        )
        return RoyalConfig()


cfg: RoyalConfig = _build_cfg()