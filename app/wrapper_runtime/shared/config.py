from pydantic import Field
from pydantic_settings import BaseSettings


class WrapperSettings(BaseSettings):
    """Configuration settings for wrapper runtime"""
    
    # Queue Configuration
    DATA_QUEUE: str = Field(default="resource_data", env="WRAPPER_DATA_QUEUE")
    
    # Message Configuration
    MAX_POINTS_PER_MESSAGE: int = Field(default=100000, env="WRAPPER_MAX_POINTS_PER_MESSAGE")
    
    # Rate Limiting
    DEFAULT_RATE_LIMIT_DELAY: float = Field(default=1.0, env="WRAPPER_RATE_LIMIT_DELAY")
    
    # Retry Configuration
    MAX_RETRIES: int = Field(default=3, env="WRAPPER_MAX_RETRIES")
    RETRY_DELAY: float = Field(default=2.0, env="WRAPPER_RETRY_DELAY")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


wrapper_settings = WrapperSettings()