from typing import List, Optional
from bson.objectid import ObjectId
from bson.errors import InvalidId
from pymongo.errors import DuplicateKeyError, OperationFailure
from dependencies.database import db
from utils.mongo_utils import serialize, deserialize
from schemas.resource import ResourceCreate, ResourceDelete, ResourceUpdate
from schemas.data_segment import TimePoint
from services.data_service import create_data_segment
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


async def get_all_resources(skip: int = 0, limit: int = 10) -> List[dict]:
    try:
        resources = await db.resources.find({"deleted": False}).skip(skip).limit(limit).to_list(limit)
        return [serialize(resource) for resource in resources]
    except OperationFailure as e:
        logger.error(f"Database operation failed in get_all_resources: {e}")
        raise


async def get_resource_by_id(resource_id: str) -> Optional[dict]:
    try:
        resource = await db.resources.find_one({"_id": ObjectId(resource_id), "deleted": False})
        return serialize(resource) if resource else None
    except InvalidId as e:
        logger.error(f"Invalid resource ID format: {resource_id}")
        raise ValueError(f"Invalid resource ID: {resource_id}")
    except OperationFailure as e:
        logger.error(f"Database operation failed in get_resource_by_id: {e}")
        raise


async def create_resource(resource_data: ResourceCreate) -> Optional[dict]:
    try:
        resource_dict = deserialize(resource_data.dict())
        resource_dict["deleted"] = False
        
        # Extract uploaded data before removing it from resource
        uploaded_data = resource_dict.get("data", [])
        uploaded_headers = resource_dict.get("headers", [])
        
        # Convert frontend string dates to database datetime fields
        if "start_period" in resource_dict and resource_dict["start_period"]:
            # For now, keep as string since frontend sends strings like "2004"
            # You can add proper date parsing here if needed
            resource_dict["startPeriod"] = None  # or parse the string to datetime
        else:
            resource_dict["startPeriod"] = None
            
        if "end_period" in resource_dict and resource_dict["end_period"]:
            resource_dict["endPeriod"] = None  # or parse the string to datetime
        else:
            resource_dict["endPeriod"] = None
        
        # Remove the frontend fields to avoid database conflicts
        resource_dict.pop("start_period", None)
        resource_dict.pop("end_period", None)
        
        # Ensure file_metadata is properly handled
        if "file_metadata" in resource_dict and resource_dict["file_metadata"] is not None:
            resource_dict["file_metadata"] = deserialize(resource_dict["file_metadata"])
        
        result = await db.resources.insert_one(resource_dict)
        resource_id = result.inserted_id
        
        if resource_id:
            # Process uploaded data into time-series format if available
            if uploaded_data and len(uploaded_data) > 0:
                try:
                    logger.info(f"Processing {len(uploaded_data)} rows of uploaded data for resource {resource_id}")
                    
                    # Convert rows to TimePoint format
                    # Each row is [x_value, y_value, ...] where x is timestamp/year and y is the value
                    time_points = []
                    for row in uploaded_data:
                        if len(row) >= 2:
                            x_value = row[0]
                            y_value = row[1]
                            
                            # Skip rows with missing data
                            if x_value is None or y_value is None:
                                continue
                            
                            # Convert x to datetime if it's not already
                            if isinstance(x_value, (int, float)):
                                # Assume it's a year (e.g., 2004)
                                try:
                                    x_datetime = datetime(int(x_value), 1, 1)
                                except (ValueError, TypeError):
                                    logger.warning(f"Skipping invalid x value: {x_value}")
                                    continue
                            elif isinstance(x_value, str):
                                # Try to parse as datetime
                                try:
                                    x_datetime = datetime.fromisoformat(x_value.replace('Z', '+00:00'))
                                except ValueError:
                                    # Try parsing as year
                                    try:
                                        x_datetime = datetime(int(x_value), 1, 1)
                                    except (ValueError, TypeError):
                                        logger.warning(f"Skipping invalid x value: {x_value}")
                                        continue
                            elif isinstance(x_value, datetime):
                                x_datetime = x_value
                            else:
                                logger.warning(f"Skipping unsupported x type: {type(x_value)}")
                                continue
                            
                            # Convert y to float
                            try:
                                y_float = float(y_value)
                            except (ValueError, TypeError):
                                logger.warning(f"Skipping invalid y value: {y_value}")
                                continue
                            
                            time_points.append(TimePoint(x=x_datetime, y=y_float))
                    
                    # Create data segment if we have valid points
                    if time_points:
                        logger.info(f"Creating data segment with {len(time_points)} points for resource {resource_id}")
                        await create_data_segment(resource_id, time_points)
                        logger.info(f"Successfully created data segment for resource {resource_id}")
                    else:
                        logger.warning(f"No valid time points extracted from uploaded data for resource {resource_id}")
                        
                except Exception as e:
                    logger.error(f"Failed to process uploaded data for resource {resource_id}: {e}")
                    # Don't fail the resource creation, just log the error
            
            return await get_resource_by_id(str(resource_id))
        return None
    except DuplicateKeyError as e:
        logger.error(f"Resource creation failed - duplicate key: {e}")
        raise ValueError("Resource with this identifier already exists")
    except OperationFailure as e:
        logger.error(f"Database operation failed in create_resource: {e}")
        raise


async def update_resource(resource_id: str, resource_data: ResourceUpdate) -> Optional[dict]:
    try:
        update_dict = deserialize(resource_data.dict(exclude_unset=True))
        
        # Convert frontend string dates to database datetime fields  
        if "start_period" in update_dict and update_dict["start_period"]:
            update_dict["startPeriod"] = None  # or parse the string to datetime
            update_dict.pop("start_period")
            
        if "end_period" in update_dict and update_dict["end_period"]:
            update_dict["endPeriod"] = None  # or parse the string to datetime
            update_dict.pop("end_period")
        
        # Ensure file_metadata is properly handled
        if "file_metadata" in update_dict and update_dict["file_metadata"] is not None:
            update_dict["file_metadata"] = deserialize(update_dict["file_metadata"])
        
        result = await db.resources.update_one(
            {"_id": ObjectId(resource_id), "deleted": False},
            {"$set": update_dict}
        )
        if result.modified_count > 0:
            return await get_resource_by_id(resource_id)
        return None
    except InvalidId as e:
        logger.error(f"Invalid resource ID format: {resource_id}")
        raise ValueError(f"Invalid resource ID: {resource_id}")
    except OperationFailure as e:
        logger.error(f"Database operation failed in update_resource: {e}")
        raise


async def delete_resource(resource_id: str) -> Optional[ResourceDelete]:
    try:
        # First check if the resource exists and is not already deleted
        existing_resource = await db.resources.find_one(
            {"_id": ObjectId(resource_id), "deleted": False}
        )
        
        if not existing_resource:
            logger.warning(f"Resource not found or already deleted: {resource_id}")
            return None
            
        # Perform the soft delete
        result = await db.resources.update_one(
            {"_id": ObjectId(resource_id), "deleted": False},
            {"$set": {"deleted": True}}
        )
        
        if result.modified_count > 0:
            return ResourceDelete(id=resource_id, deleted=True)
        return None
    except InvalidId as e:
        logger.error(f"Invalid resource ID format: {resource_id}")
        raise ValueError(f"Invalid resource ID: {resource_id}")
    except OperationFailure as e:
        logger.error(f"Database operation failed in delete_resource: {e}")
        raise
