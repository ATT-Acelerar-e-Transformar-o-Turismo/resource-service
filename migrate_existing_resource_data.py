#!/usr/bin/env python3
"""
Migration script to process existing resources with embedded data.

This script finds all resources that have embedded data but no entries in the
resources_data collection, and processes that data into the proper time-series format.

Usage:
    python migrate_existing_resource_data.py [--dry-run]

Options:
    --dry-run    Show what would be done without making changes
"""

import asyncio
import sys
import os
from datetime import datetime
from typing import List

# Add app directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

from motor.motor_asyncio import AsyncIOMotorClient
from config import settings
from schemas.data_segment import TimePoint
from services.data_service import create_data_segment
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def migrate_resource_data(dry_run: bool = False):
    """Migrate existing resource data to time-series format"""
    
    # Connect to MongoDB
    client = AsyncIOMotorClient(settings.MONGO_URL)
    db = client[settings.MONGO_DB_NAME]
    
    try:
        # Find all resources that have data
        resources_with_data = await db.resources.find({
            "deleted": False,
            "data": {"$exists": True, "$ne": None, "$ne": []}
        }).to_list(length=None)
        
        logger.info(f"Found {len(resources_with_data)} resources with embedded data")
        
        processed_count = 0
        skipped_count = 0
        error_count = 0
        
        for resource in resources_with_data:
            resource_id = resource["_id"]
            resource_name = resource.get("name", "Unknown")
            data = resource.get("data", [])
            
            logger.info(f"Processing resource: {resource_name} (ID: {resource_id})")
            
            # Check if this resource already has data in resources_data
            existing_data = await db.resources_data.find_one({"resource_id": resource_id})
            if existing_data and existing_data.get("data"):
                logger.info(f"  ⏭️  Skipping - already has processed data ({len(existing_data.get('data', []))} points)")
                skipped_count += 1
                continue
            
            if not data or len(data) == 0:
                logger.info(f"  ⏭️  Skipping - no data to process")
                skipped_count += 1
                continue
            
            try:
                # Convert rows to TimePoint format
                time_points = []
                for row in data:
                    if not isinstance(row, list) or len(row) < 2:
                        continue
                    
                    x_value = row[0]
                    y_value = row[1]
                    
                    # Skip rows with missing data
                    if x_value is None or y_value is None:
                        continue
                    
                    # Convert x to datetime
                    if isinstance(x_value, (int, float)):
                        try:
                            x_datetime = datetime(int(x_value), 1, 1)
                        except (ValueError, TypeError):
                            logger.warning(f"  ⚠️  Skipping invalid x value: {x_value}")
                            continue
                    elif isinstance(x_value, str):
                        try:
                            x_datetime = datetime.fromisoformat(x_value.replace('Z', '+00:00'))
                        except ValueError:
                            try:
                                x_datetime = datetime(int(x_value), 1, 1)
                            except (ValueError, TypeError):
                                logger.warning(f"  ⚠️  Skipping invalid x value: {x_value}")
                                continue
                    elif isinstance(x_value, datetime):
                        x_datetime = x_value
                    else:
                        logger.warning(f"  ⚠️  Skipping unsupported x type: {type(x_value)}")
                        continue
                    
                    # Convert y to float
                    try:
                        y_float = float(y_value)
                    except (ValueError, TypeError):
                        logger.warning(f"  ⚠️  Skipping invalid y value: {y_value}")
                        continue
                    
                    time_points.append(TimePoint(x=x_datetime, y=y_float))
                
                if not time_points:
                    logger.warning(f"  ⚠️  No valid time points extracted from data")
                    error_count += 1
                    continue
                
                logger.info(f"  📊 Extracted {len(time_points)} valid time points")
                
                if dry_run:
                    logger.info(f"  🔍 DRY RUN - Would create data segment")
                else:
                    # Create data segment
                    await create_data_segment(resource_id, time_points)
                    logger.info(f"  ✅ Successfully created data segment")
                
                processed_count += 1
                
            except Exception as e:
                logger.error(f"  ❌ Error processing resource {resource_id}: {e}")
                error_count += 1
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("MIGRATION SUMMARY")
        logger.info("="*60)
        logger.info(f"Total resources found:    {len(resources_with_data)}")
        logger.info(f"Successfully processed:   {processed_count}")
        logger.info(f"Skipped (already done):   {skipped_count}")
        logger.info(f"Errors:                   {error_count}")
        
        if dry_run:
            logger.info("\n⚠️  This was a DRY RUN - no changes were made")
            logger.info("Run without --dry-run to apply changes")
        else:
            logger.info("\n✅ Migration complete!")
        
    finally:
        client.close()


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    
    if dry_run:
        print("\n🔍 Running in DRY RUN mode\n")
    
    asyncio.run(migrate_resource_data(dry_run=dry_run))

