from typing import List, Optional
from bson.objectid import ObjectId
from bson.errors import InvalidId
from pymongo.errors import DuplicateKeyError, OperationFailure
from dependencies.database import db
from dependencies.rabbitmq import rabbitmq_client
from utils.mongo_utils import serialize, deserialize
from schemas.resource import ResourceCreate, ResourceDelete, ResourceUpdate
from datetime import datetime
from config import settings
import json
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
        wrapper_id = resource_dict.get("wrapper_id")
        
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
            # Send uploaded data to message queue for processing (consistent with API wrappers)
            # Process ALL data columns as separate series
            if uploaded_data and len(uploaded_data) > 0 and wrapper_id:
                try:
                    logger.info(f"Processing {len(uploaded_data)} rows with {len(uploaded_headers)} columns for resource {resource_id}")
                    
                    # Determine how many data columns we have (excluding the first x column)
                    num_data_columns = len(uploaded_headers) - 1 if uploaded_headers else max(len(row) - 1 for row in uploaded_data if row)
                    
                    if num_data_columns == 0:
                        logger.warning(f"No data columns found for resource {resource_id}")
                        return await get_resource_by_id(str(resource_id))
                    
                    logger.info(f"File has {num_data_columns} data column(s), processing all columns")
                    logger.info(f"Headers: {uploaded_headers}")
                    
                    # Process each data column (starting from column 1, column 0 is x-axis)
                    for col_index in range(1, num_data_columns + 1):
                        series_name = uploaded_headers[col_index] if uploaded_headers and len(uploaded_headers) > col_index else f"Column {col_index}"
                        
                        # Convert rows to message format for this column
                        # Format: [{"x": "datetime_string", "y": float_value, "series": "column_name"}, ...]
                        data_points = []
                        for row in uploaded_data:
                            if len(row) > col_index:
                                x_value = row[0]
                                y_value = row[col_index]
                                
                                # Skip rows with missing data
                                if x_value is None or y_value is None:
                                    continue
                                
                                # Convert x to ISO format string
                                if isinstance(x_value, (int, float)):
                                    try:
                                        x_datetime = datetime(int(x_value), 1, 1)
                                        x_str = x_datetime.isoformat()
                                    except (ValueError, TypeError):
                                        logger.warning(f"Skipping invalid x value: {x_value}")
                                        continue
                                elif isinstance(x_value, str):
                                    try:
                                        x_datetime = datetime.fromisoformat(x_value.replace('Z', '+00:00'))
                                        x_str = x_datetime.isoformat()
                                    except ValueError:
                                        try:
                                            x_datetime = datetime(int(x_value), 1, 1)
                                            x_str = x_datetime.isoformat()
                                        except (ValueError, TypeError):
                                            logger.warning(f"Skipping invalid x value: {x_value}")
                                            continue
                                elif isinstance(x_value, datetime):
                                    x_str = x_value.isoformat()
                                else:
                                    logger.warning(f"Skipping unsupported x type: {type(x_value)}")
                                    continue
                                
                                # Convert y to float
                                try:
                                    y_float = float(y_value)
                                except (ValueError, TypeError):
                                    logger.warning(f"Skipping invalid y value: {y_value}")
                                    continue
                                
                                data_points.append({"x": x_str, "y": y_float, "series": series_name})
                        
                        # Publish to message queue if we have valid points for this series
                        if data_points:
                            message = {
                                "wrapper_id": wrapper_id,
                                "series_name": series_name,
                                "data": data_points
                            }
                            await rabbitmq_client.publish(
                                settings.COLLECTED_DATA_QUEUE,
                                json.dumps(message)
                            )
                            logger.info(f"Successfully sent {len(data_points)} points for series '{series_name}' to queue")
                        else:
                            logger.warning(f"No valid data points extracted for series '{series_name}'")
                    
                    logger.info(f"Completed processing {num_data_columns} series for resource {resource_id}")
                        
                except Exception as e:
                    logger.error(f"Failed to send uploaded data to queue for resource {resource_id}: {e}")
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
