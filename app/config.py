from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    ORIGINS: str = Field(default="localhost", env="ORIGINS")
    MONGO_URI: str = Field(default="mongodb://localhost:27017", env="MONGO_URI")
    RABBITMQ_URL: str = Field(
        default="amqp://guest:guest@rabbitmq/", env="RABBITMQ_URL"
    )
    RESOURCE_DATA_QUEUE: str = Field(default="resource_data", env="RESOURCE_DATA_QUEUE")
    COLLECTED_DATA_QUEUE: str = Field(
        default="collected_data", env="COLLECTED_DATA_QUEUE"
    )
    CHUNK_SIZE_THRESHOLD: int = Field(default=1000, env="CHUNK_SIZE_THRESHOLD")
    
    # Wrapper Generation
    GEMINI_API_KEY: str = Field(..., env="GEMINI_API_KEY")
    GEMINI_MODEL_NAME: str = Field(default="gemini-1.5-flash", env="GEMINI_MODEL_NAME")
    DATA_RABBITMQ_URL: str = Field(
        default="amqp://user:password@data-mq:5672/", env="DATA_RABBITMQ_URL"
    )
    DATA_QUEUE_NAME: str = Field(default="data_queue", env="DATA_QUEUE_NAME")

    WRAPPER_CREATION_QUEUE_NAME: str = Field(default="wrapper_creation_queue", env="WRAPPER_CREATION_QUEUE_NAME")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
