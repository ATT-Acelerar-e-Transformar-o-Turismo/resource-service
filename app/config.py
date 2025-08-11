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

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
