from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    ORIGINS: str = Field(default="localhost", env="ORIGINS")
    MONGO_URI: str = Field(default="mongodb://localhost:27017", env="MONGO_URI")
    RABBITMQ_URL: str = Field(
        default="amqp://guest:guest@rabbitmq/", env="RABBITMQ_URL"
    )
    RESOURCE_DATA_QUEUE: str = Field(default="resource_data", env="RESOURCE_DATA_QUEUE")
    RESOURCE_DELETED_QUEUE: str = Field(
        default="resource_deleted", env="RESOURCE_DELETED_QUEUE"
    )
    # Consumed from indicator-service: when an indicator is deleted, stop the
    # wrappers of its now-orphaned resources so they don't keep running.
    INDICATOR_DELETED_QUEUE: str = Field(
        default="indicator_deleted", env="INDICATOR_DELETED_QUEUE"
    )
    WRAPPER_TRANSLATIONS_QUEUE: str = Field(
        default="wrapper_translations", env="WRAPPER_TRANSLATIONS_QUEUE"
    )
    COLLECTED_DATA_QUEUE: str = Field(
        default="collected_data", env="COLLECTED_DATA_QUEUE"
    )
    CHUNK_SIZE_THRESHOLD: int = Field(default=1000, env="CHUNK_SIZE_THRESHOLD")

    # Wrapper Generation
    GEMINI_API_KEY: str = Field(..., env="GEMINI_API_KEY")
    # Pin to a SPECIFIC stable model id, not a moving "*-latest" alias: the
    # alias hot-swaps with ~2 weeks' notice and currently lands on a heavily
    # contended model that returns 503 "overloaded" and stalls sockets. Pinning
    # makes the served model deterministic. Override via env per environment /
    # API-key tier.
    # IMPORTANT (observed 2026-06-29 on the prod "Wrapper-Generator" project):
    # even on a paid Tier-1 project, the OLDER models gemini-2.5-flash /
    # gemini-2.5-flash-lite are still capped at the *free-tier* 20 requests/day,
    # while gemini-3.5-flash gets the real paid limits (1000 req/min). So the
    # primary must be a 3.x model to actually benefit from Tier-1 billing —
    # pinning 2.5-flash silently caps generation at ~1 wrapper/day.
    GEMINI_MODEL_NAME: str = Field(default="gemini-3.5-flash", env="GEMINI_MODEL_NAME")
    # Comma-separated models to fall back to when the primary is rate-limited
    # (429) or overloaded (503/UNAVAILABLE). Tried in order after the primary,
    # bounded by the generator's aggregate wall-clock budget. On the free tier
    # each DISTINCT model has its own per-day quota bucket (~20 req/day), so more
    # distinct models = more daily headroom; the generator now fails over to the
    # next model immediately on a 429 and skips any model that already hit its
    # daily cap this generation. NOTE: "*-latest" aliases resolve to a concrete
    # model and SHARE its bucket — they add resilience to retirements, not quota,
    # so prefer distinct concrete ids here. Order: lighter/cheaper first, then a
    # heavier model (pro) as a last resort, then a "*-latest" hedge. Override per
    # environment via the GEMINI_FALLBACK_MODELS env var (no rebuild needed).
    # (gemini-2.0-flash was REMOVED — it shut down 2026-06-01.)
    GEMINI_FALLBACK_MODELS: str = Field(
        default="gemini-3.5-flash-lite,gemini-2.5-flash,gemini-2.5-flash-lite,gemini-flash-latest",
        env="GEMINI_FALLBACK_MODELS",
    )
    DATA_RABBITMQ_URL: str = Field(
        default="amqp://user:password@data-mq:5672/", env="DATA_RABBITMQ_URL"
    )
    DATA_QUEUE_NAME: str = Field(default="data_queue", env="DATA_QUEUE_NAME")

    WRAPPER_CREATION_QUEUE_NAME: str = Field(
        default="wrapper_creation_queue", env="WRAPPER_CREATION_QUEUE_NAME"
    )

    WRAPPER_GENERATION_DEBUG_MODE: bool = Field(
        default=False, env="WRAPPER_GENERATION_DEBUG_MODE"
    )

    # Max Gemini tool-calling iterations when generating an API wrapper. Each
    # iteration is a billed Gemini request, so this is the dominant cost knob:
    # 15 burned ~16 calls per generation; 4 is plenty for the model to inspect a
    # typical endpoint (1-2 fetches) and cuts cost per AI generation ~3-4x.
    WRAPPER_MAX_TOOL_CALLS: int = Field(default=4, env="WRAPPER_MAX_TOOL_CALLS")

    # Max concurrent wrapper subprocesses. Each uses ~20MB; the ceiling
    # prevents a burst from OOM-killing the service.
    MAX_CONCURRENT_WRAPPERS: int = Field(
        default=100, env="MAX_CONCURRENT_WRAPPERS"
    )

    # Hard timeout for a single wrapper subprocess (seconds). 0 = disabled.
    WRAPPER_EXECUTION_TIMEOUT_SECONDS: int = Field(
        default=3600, env="WRAPPER_EXECUTION_TIMEOUT_SECONDS"
    )

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
