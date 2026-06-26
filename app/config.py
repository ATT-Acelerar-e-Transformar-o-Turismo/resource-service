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
    # API-key tier (e.g. a newer flash GA model if available to your project).
    GEMINI_MODEL_NAME: str = Field(default="gemini-2.5-flash", env="GEMINI_MODEL_NAME")
    # Comma-separated models to fall back to when the primary is overloaded
    # (503/UNAVAILABLE) or stalls. Tried in order after the primary fails over,
    # bounded by the generator's aggregate wall-clock budget. Pick currently
    # served models — a lighter, less-contended model first, then a "*-latest"
    # alias as a final hedge so a future model retirement still resolves to
    # something. (gemini-2.0-flash was REMOVED here — it shut down 2026-06-01.)
    GEMINI_FALLBACK_MODELS: str = Field(
        default="gemini-2.5-flash-lite,gemini-flash-latest",
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
