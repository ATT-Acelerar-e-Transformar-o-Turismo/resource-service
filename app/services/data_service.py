from typing import List, Optional, Dict, Any, AsyncGenerator
from bson.objectid import ObjectId
from bson.errors import InvalidId
from pymongo.errors import OperationFailure
from dependencies.database import db
from schemas.data_segment import TimePoint, DataSegment
from datetime import datetime, UTC
import logging

logger = logging.getLogger(__name__)


async def create_data_segment(
    resource_id: ObjectId, points: List[TimePoint]
) -> Optional[DataSegment]:
    try:
        resource = await db.resources.find_one({"_id": resource_id})

        if not resource:
            return None

        new_datasegment = DataSegment(
            resource_id=resource_id,
            points=points,
            created_at=datetime.now(UTC), # It's here where the priority during merge is determined.
        )

        # Store raw data segment (convert to json)
        await db.data_segments.insert_one(new_datasegment.model_dump())

        # Merge logic with created_at-based override
        # For multi-series resources, key by (x, series) instead of just x
        all_points_cursor = db.data_segments.find(
            {"resource_id": resource_id}, {"points": 1, "created_at": 1, "_id": 0}
        )

        points_map = {}
        async for segment in all_points_cursor:
            segment_created_at = segment.get("created_at")
            if not segment_created_at:
                continue
            for point in segment.get("points", []):
                x_val = point["x"]
                series_val = point.get("series")  # None for single-series resources
                
                # Create composite key: (x, series) for multi-series, just x for single-series
                key = (x_val, series_val) if series_val else x_val
                
                if (
                    key not in points_map
                    or segment_created_at > points_map[key]["created_at"]
                ):
                    points_map[key] = {
                        "x": x_val,
                        "y": point["y"],
                        "series": series_val,
                        "created_at": segment_created_at,
                    }

        if not points_map:
            return new_datasegment

        merged_points = sorted(list(points_map.values()), key=lambda p: (p.get("series") or "", p["x"]))

        # Remove created_at from merged_points before storing, preserve series if present
        merged_data_points = []
        for p in merged_points:
            point_dict = {"x": p["x"], "y": p["y"]}
            if p.get("series"):
                point_dict["series"] = p["series"]
            merged_data_points.append(point_dict)

        # Store merged data
        await db.resources_data.update_one(
            {"resource_id": resource_id},
            {"$set": {"data": merged_data_points}},
            upsert=True,
        )

        # Update resource start and end periods
        min_x = merged_points[0]["x"]
        max_x = merged_points[-1]["x"]
        update = {}
        if not resource.get("startPeriod") or min_x < resource.get("startPeriod"):
            update["startPeriod"] = min_x
        if not resource.get("endPeriod") or max_x > resource.get("endPeriod"):
            update["endPeriod"] = max_x
        if update:
            await db.resources.update_one({"_id": resource_id}, {"$set": update})

        return new_datasegment
        
    except OperationFailure as e:
        logger.error(f"Database operation failed in create_data_segment: {e}")
        raise


async def get_data_by_resource_id(
    resource_id: ObjectId,
    min_time: Optional[datetime] = None,
    max_time: Optional[datetime] = None,
    chunk_size: Optional[int] = None,
    sorted: bool = False
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Retrieves data for a given resource_id from the resources_data collection,
    with optional time window filtering and chunking.
    """
    try:
        pipeline = [
            {"$match": {"resource_id": resource_id}},
            {"$unwind": "$data"},
            {"$project": {"x": "$data.x", "y": "$data.y", "series": "$data.series", "_id": 0}},
        ]

        match_filter = {}
        if min_time:
            match_filter["$gte"] = min_time
        if max_time:
            match_filter["$lte"] = max_time

        if match_filter:
            pipeline.append({"$match": {"x": match_filter}})

        if sorted:
            pipeline.append({"$sort": {"x": 1}})

        cursor = db.resources_data.aggregate(pipeline)

        if chunk_size:
            chunk = []
            async for point in cursor:
                chunk.append(point)
                if len(chunk) == chunk_size:
                    yield {"data": chunk}
                    chunk = []
            if chunk:
                yield {"data": chunk}
        else:
            all_data = await cursor.to_list(length=None)
            if all_data:
                yield {"data": all_data}
                
    except OperationFailure as e:
        logger.error(f"Database operation failed in get_data_by_resource_id: {e}")
        raise
