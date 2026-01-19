import asyncio
import time
from typing import Optional, Dict, Any, List, Callable, Awaitable
from requests.models import Response
from datetime import datetime, timedelta
import aio_pika
import json
from config import wrapper_settings as settings

def mask_credentials(url: str) -> str:
    """
    Mask credentials in URLs for secure logging
    Example: amqp://user:password@host:port -> amqp://***:***@host:port
    """
    if not url or '://' not in url:
        return url

    try:
        protocol, rest = url.split('://', 1)

        if '@' not in rest:
            return url

        credentials_part, host_part = rest.rsplit('@', 1)

        if ':' in credentials_part:
            masked_credentials = '***:***'
        else:
            masked_credentials = '***'

        return f"{protocol}://{masked_credentials}@{host_part}"
    except (ValueError, AttributeError, IndexError):
        return "***MASKED_URL***"

class RateLimitHandler:
    """
    Handle API rate limiting based on response headers and status codes
    """
    def __init__(self):
        self.last_request_time = 0
        self.default_delay = 1.0  # Default 1 second between requests
        
    def _get_reset_delay(self, headers: dict) -> Optional[float]:
        """Get delay based on reset time headers"""
        reset_headers = ['x-ratelimit-reset', 'x-rate-limit-reset', 'ratelimit-reset']
        
        for reset_header in reset_headers:
            reset_time = headers.get(reset_header)
            if reset_time:
                try:
                    reset_dt = datetime.fromisoformat(reset_time.replace('Z'))
                    now = datetime.now(reset_dt.tzinfo)
                    return max((reset_dt - now).total_seconds(), 1.0)
                except:
                    continue
        return None
        
    def parse_rate_limit_headers(self, headers: dict) -> Optional[float]:
        """
        Parse rate limit information from response headers
        Prioritizes remaining requests over retry-after header
        Returns delay in seconds if rate limit detected, None otherwise
        """
        # Check remaining requests first (priority over retry-after)
        remaining_headers = ['x-ratelimit-remaining', 'x-rate-limit-remaining', 'ratelimit-remaining']
        
        for header in remaining_headers:
            remaining = headers.get(header)
            if remaining:
                try:
                    remaining_count = int(remaining)
                    if remaining_count <= 0:
                        # No requests remaining, check reset time or retry-after
                        reset_delay = self._get_reset_delay(headers)
                        if reset_delay:
                            return reset_delay
                        
                        # If no reset time, check retry-after
                        retry_after = headers.get('retry-after')
                        if retry_after:
                            try:
                                return float(retry_after)
                            except ValueError:
                                pass
                        
                        return 10 * self.default_delay  # Default when no requests remaining
                    elif remaining_count <= 1:
                        return 10 * self.default_delay  # 10 second delay
                    elif remaining_count <= 3:
                        return 5 * self.default_delay  # 5 second delay
                    elif remaining_count <= 5:
                        return 2 * self.default_delay  # 2 second delay
                    else:
                        # Sufficient requests remaining, no delay needed
                        return None
                except ValueError:
                    continue
        
        # Only use retry-after if no remaining info found
        retry_after = headers.get('retry-after')
        if retry_after:
            try:
                return float(retry_after)
            except ValueError:
                pass
        
        return None
    
    def parse_rate_limit(self, response: Response) -> Optional[float]:
        """
        Parse rate limit information from complete response
        Handles status code 429 and response headers
        
        Args:
            response: Response object
        
        Returns:
            Delay in seconds if rate limit detected, None otherwise
        """
        headers = response.headers
        
        # Handle 429 Too Many Requests
        if response.status_code == 429:
            print("Rate limit exceeded (HTTP 429)")
            
            # For 429, prioritize retry-after header
            retry_after = headers.get('retry-after')
            if retry_after:
                try:
                    delay = float(retry_after)
                    print(f"Using retry-after header: {delay} seconds")
                    return delay
                except ValueError:
                    pass
            
            # Check reset time for 429
            reset_delay = self._get_reset_delay(headers)
            if reset_delay:
                print(f"Using reset time: {reset_delay} seconds")
                return max(reset_delay, 5.0)  # At least 5 seconds for 429
            
            # Default delay for 429 if no specific timing found
            print("No specific timing found, using default 60 second delay for 429")
            return 60.0
        
        # For non-429 responses, use header-based logic (prioritizes remaining)
        return self.parse_rate_limit_headers(headers)
    
    async def handle_rate_limit(self, response: Response):
        """
        Handle rate limiting based on response headers
        """
        delay = self.parse_rate_limit(response)
        
        if delay:
            print(f"Rate limit detected, waiting {delay} seconds...")
            await asyncio.sleep(delay)
        else:
            # Ensure minimum delay between requests
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            
            if time_since_last < self.default_delay:
                wait_time = self.default_delay - time_since_last
                await asyncio.sleep(wait_time)
        
        self.last_request_time = time.time()

class HistoricalDataFetcher:
    """
    Generic historical data fetcher with adaptive batch sizing
    Handles the complex logic of fetching historical data in optimal batches
    """
    
    def __init__(self, wrapper_id: str, get_interval_seconds_func: Callable[[], int]):
        self.wrapper_id = wrapper_id
        self.get_interval_seconds = get_interval_seconds_func
        
    async def fetch_all_historical_data(self, 
                                      fetch_date_range_func: Callable[[datetime, datetime], Awaitable[List[Dict[str, Any]]]],
                                      send_to_queue_func: Callable[[List[Dict[str, Any]]], Awaitable[None]]) -> int:
        """
        Fetch all available historical data using adaptive batch sizing
        
        Args:
            fetch_date_range_func: Function to fetch data for a specific date range
            send_to_queue_func: Function to send data to queue
            
        Returns:
            Total number of data points sent
        """
        print(f"Wrapper {self.wrapper_id}: Starting historical data fetch...")
        total_points_sent = 0
        
        # Start with a reasonable batch size
        current_date = datetime.now()
        batch_size_seconds = 100 * self.get_interval_seconds()
        max_time_span_found = 0
        stable_iterations = 0
        consecutive_errors = 0
        previous_oldest_date = None
        no_progress_iterations = 0
        
        while True:
            end_date = current_date
            start_date = current_date - timedelta(seconds=batch_size_seconds)
            
            try:
                batch_data = await fetch_date_range_func(start_date, end_date)
                consecutive_errors = 0  # Reset error counter on success
                
                if not batch_data:
                    print(f"Wrapper {self.wrapper_id}: No data found for range {start_date.year}-{end_date.year}. Ending historical fetch.")
                    break
                    
                # Get the first and last datetime from the batch data
                batch_data = sorted(batch_data, key=lambda x: x['x'])
                first_datetime = datetime.fromisoformat(batch_data[0]['x'])
                last_datetime = datetime.fromisoformat(batch_data[-1]['x'])
                time_span_seconds = (last_datetime - first_datetime).total_seconds()
                
                # Check if we're making progress (getting older data)
                if previous_oldest_date is None:
                    previous_oldest_date = first_datetime
                    no_progress_iterations = 0
                elif first_datetime >= previous_oldest_date:
                    # We're not getting older data, increment no progress counter
                    no_progress_iterations += 1
                    print(f"Wrapper {self.wrapper_id}: No progress - oldest date still {first_datetime} (iteration {no_progress_iterations})")
                    
                    # If we haven't made progress for 3 iterations, all historical data was fetched
                    if no_progress_iterations >= 3:
                        print(f"Wrapper {self.wrapper_id}: No progress for {no_progress_iterations} iterations. All historical data appears to be fetched.")
                        break
                else:
                    # We got older data, reset counter and update oldest date
                    previous_oldest_date = first_datetime
                    no_progress_iterations = 0
                
                # Update current_date to the oldest date found to continue fetching older data
                current_date = first_datetime
                
                # Track the maximum time span we've seen
                if time_span_seconds > max_time_span_found:
                    max_time_span_found = time_span_seconds
                    stable_iterations = 0
                else:
                    stable_iterations += 1
                
                print(f"Wrapper {self.wrapper_id}: Time span: {int(time_span_seconds)}s, Batch size: {batch_size_seconds}s, Stable iterations: {stable_iterations}, No progress: {no_progress_iterations}")
                
                # Adaptive batch sizing logic
                if stable_iterations >= 3:
                    print(f"Wrapper {self.wrapper_id}: API appears to limit time span to ~{int(time_span_seconds)} seconds")
                    batch_size_seconds = int(time_span_seconds) + self.get_interval_seconds()
                elif time_span_seconds >= batch_size_seconds * 0.8:
                    # If we got at least 80% of requested time span, try doubling
                    old_batch_size = batch_size_seconds
                    batch_size_seconds *= 2
                    print(f"Wrapper {self.wrapper_id}: Doubling batch size from {old_batch_size}s to {batch_size_seconds}s")
                elif time_span_seconds < batch_size_seconds * 0.5:
                    # If we got less than 50% of requested time span, use what we got
                    old_batch_size = batch_size_seconds
                    batch_size_seconds = int(time_span_seconds) + self.get_interval_seconds()
                    print(f"Wrapper {self.wrapper_id}: Reducing batch size from {old_batch_size}s to {batch_size_seconds}s")
                
                # Send batch data immediately to queue instead of accumulating
                await send_to_queue_func(batch_data)
                total_points_sent += len(batch_data)
                print(f"Wrapper {self.wrapper_id}: Sent {len(batch_data)} historical points to queue")

            except (ValueError, KeyError, TypeError, ConnectionError, TimeoutError) as e:
                error_message = str(e).lower()
                consecutive_errors += 1
                
                # Check if it's a 400 error or time range related error
                if "400" in error_message or "bad request" in error_message or "time range" in error_message or "date range" in error_message:
                    old_batch_size = batch_size_seconds
                    batch_size_seconds = batch_size_seconds // 2
                    stable_iterations = 3
                    print(f"Wrapper {self.wrapper_id}: API rejected time range ({error_message}). Reducing batch size from {old_batch_size}s to {batch_size_seconds}s")
                    
                    # If batch size becomes too small, break to avoid infinite loop
                    if batch_size_seconds <= self.get_interval_seconds():
                        print(f"Wrapper {self.wrapper_id}: Batch size reduced to minimum interval. Stopping historical fetch.")
                        break
                        
                elif consecutive_errors >= 3:
                    print(f"Wrapper {self.wrapper_id}: Too many consecutive errors ({consecutive_errors}). Stopping historical fetch. Last error: {str(e)}")
                    break
            
        print(f"Wrapper {self.wrapper_id}: Historical data fetch completed. Total points sent: {total_points_sent}")
        return total_points_sent


class MessageQueueSender:
    """
    Handles sending data to RabbitMQ with message chunking support
    """
    MAX_POINTS_PER_MESSAGE = settings.MAX_POINTS_PER_MESSAGE
    
    def __init__(self, wrapper_id: str, rabbitmq_url: str, metadata: dict = None):
        self.wrapper_id = wrapper_id
        self.rabbitmq_url = rabbitmq_url
        self.metadata = metadata or {}
    
    async def send_to_queue(self, data_points: List[Dict[str, Any]]):
        """Send formatted data to RabbitMQ queue with automatic chunking"""
        if not data_points:
            print(f"Wrapper {self.wrapper_id}: No data points to send")
            return
            
        total_points = len(data_points)
        
        try:
            print(f"Wrapper {self.wrapper_id}: Connecting to RabbitMQ at {mask_credentials(self.rabbitmq_url)}")
            connection = await aio_pika.connect_robust(self.rabbitmq_url)
            
            async with connection:
                channel = await connection.channel()
                queue = await channel.declare_queue(settings.DATA_QUEUE, durable=True)
                
                if total_points <= self.MAX_POINTS_PER_MESSAGE:
                    message_data = {
                        "wrapper_id": self.wrapper_id,
                        "data": data_points,
                        "metadata": self.metadata,
                        "total_segments": 1,
                        "segment_number": 1
                    }
                    
                    message = aio_pika.Message(
                        body=json.dumps(message_data).encode(),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                    )
                    
                    await channel.default_exchange.publish(message, routing_key=queue.name)
                    print(f"Wrapper {self.wrapper_id}: Sent {total_points} data points in 1 message")
                else:
                    total_segments = (total_points + self.MAX_POINTS_PER_MESSAGE - 1) // self.MAX_POINTS_PER_MESSAGE
                    print(f"Wrapper {self.wrapper_id}: Splitting {total_points} data points into {total_segments} messages")
                    
                    for segment_num in range(total_segments):
                        start_idx = segment_num * self.MAX_POINTS_PER_MESSAGE
                        end_idx = min(start_idx + self.MAX_POINTS_PER_MESSAGE, total_points)
                        chunk = data_points[start_idx:end_idx]
                        
                        message_data = {
                            "wrapper_id": self.wrapper_id,
                            "data": chunk,
                            "metadata": self.metadata,
                            "total_segments": total_segments,
                            "segment_number": segment_num + 1
                        }
                        
                        message = aio_pika.Message(
                            body=json.dumps(message_data).encode(),
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                        )
                        
                        await channel.default_exchange.publish(message, routing_key=queue.name)
                        print(f"Wrapper {self.wrapper_id}: Sent segment {segment_num + 1}/{total_segments} with {len(chunk)} data points")
                    
                    print(f"Wrapper {self.wrapper_id}: Completed sending {total_points} data points in {total_segments} messages")

        except (ConnectionError, TimeoutError, OSError) as e:
            print(f"Error sending data to queue: {str(e)}")
            raise


def get_interval_from_periodicity(periodicity: str) -> int:
    """
    Convert periodicity string to seconds
    
    Args:
        periodicity: String like 'Daily', 'Hourly', 'Weekly', etc.
    
    Returns:
        Number of seconds for the interval
    """
    periodicity_lower = periodicity.lower()
    
    if 'minute' in periodicity_lower:
        if '5' in periodicity_lower:
            return 5 * 60
        elif '10' in periodicity_lower:
            return 10 * 60
        elif '15' in periodicity_lower:
            return 15 * 60
        elif '30' in periodicity_lower:
            return 30 * 60
        else:
            return 60  # Default to 1 minute
    elif 'hour' in periodicity_lower:
        return 3600
    elif 'daily' in periodicity_lower or 'day' in periodicity_lower:
        return 3600 * 24
    elif 'weekly' in periodicity_lower or 'week' in periodicity_lower:
        return 3600 * 24 * 7
    elif 'monthly' in periodicity_lower or 'month' in periodicity_lower:
        return 3600 * 24 * 30
    else:
        # Default to daily if not recognized
        return 3600 * 24


class ContinuousExecutor:
    """
    Handles continuous execution of wrappers with proper error handling and intervals
    """
    
    def __init__(self, wrapper_id: str, interval_seconds: int):
        self.wrapper_id = wrapper_id
        self.interval_seconds = interval_seconds
        
    async def run_continuous(self, run_once_func: Callable[[], Awaitable[None]], 
                           fetch_historical_func: Optional[Callable[[], Awaitable[None]]] = None,
                           source_type: str = "API"):
        """
        Run the wrapper continuously with intervals based on periodicity
        
        Args:
            run_once_func: Async function to execute once per cycle
            fetch_historical_func: Optional async function to fetch historical data
            source_type: Type of source (used for determining execution mode)
        """
        # For API sources, first fetch historical data if function provided
        if fetch_historical_func and source_type == "API":
            try:
                print(f"Wrapper {self.wrapper_id}: Fetching historical data before starting continuous mode...")
                await fetch_historical_func()
                print(f"Wrapper {self.wrapper_id}: Historical data fetch completed")
            except (ValueError, KeyError, TypeError, ConnectionError, TimeoutError, OSError) as e:
                print(f"Wrapper {self.wrapper_id}: Error fetching historical data: {str(e)}")
                print("Proceeding with continuous mode for current data only...")
        
        print(f"Wrapper {self.wrapper_id}: Starting continuous execution for {source_type} source")
        print(f"Interval: {self.interval_seconds}s")
        
        while True:
            try:
                await run_once_func()
                print(f"Wrapper {self.wrapper_id}: Waiting {self.interval_seconds}s until next execution...")
                await asyncio.sleep(self.interval_seconds)
            except KeyboardInterrupt:
                print(f"Wrapper {self.wrapper_id}: Stopping execution")
                break
            except (ValueError, KeyError, TypeError, ConnectionError, TimeoutError, OSError) as e:
                print(f"Wrapper {self.wrapper_id}: Error in continuous execution: {str(e)}")
                print(f"Waiting {self.interval_seconds}s before retry...")
                await asyncio.sleep(self.interval_seconds)
