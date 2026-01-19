from fastapi import HTTPException, status


class ResourceServiceException(Exception):
    """Base exception for resource-service."""
    pass


class ResourceNotFoundException(ResourceServiceException):
    """Raised when a resource is not found."""
    pass


class WrapperNotFoundError(ResourceServiceException):
    """Raised when a wrapper is not found."""
    pass


class DuplicateResourceError(ResourceServiceException):
    """Raised when attempting to create a duplicate resource."""
    pass


class InvalidResourceIdError(ResourceServiceException):
    """Raised when resource ID format is invalid."""
    pass


class DatabaseConnectionError(ResourceServiceException):
    """Raised when database connection fails."""
    pass


class WrapperGenerationError(ResourceServiceException):
    """Raised when wrapper generation fails."""
    pass


class WrapperExecutionError(ResourceServiceException):
    """Raised when wrapper execution fails."""
    pass


def resource_not_found(resource_id: str) -> HTTPException:
    """Create HTTPException for resource not found."""
    return HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Resource with id {resource_id} not found"
    )


def wrapper_not_found(wrapper_id: str) -> HTTPException:
    """Create HTTPException for wrapper not found."""
    return HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Wrapper with id {wrapper_id} not found"
    )


def duplicate_resource() -> HTTPException:
    """Create HTTPException for duplicate resource."""
    return HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Resource with this identifier already exists"
    )


def invalid_resource_id(resource_id: str) -> HTTPException:
    """Create HTTPException for invalid resource ID."""
    return HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Invalid resource ID format: {resource_id}"
    )


def wrapper_generation_failed(detail: str) -> HTTPException:
    """Create HTTPException for wrapper generation failure."""
    return HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Wrapper generation failed: {detail}"
    )


def wrapper_execution_failed(detail: str) -> HTTPException:
    """Create HTTPException for wrapper execution failure."""
    return HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Wrapper execution failed: {detail}"
    )
