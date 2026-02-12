import asyncio
import os
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
    if not url or "://" not in url:
        return url

    try:
        protocol, rest = url.split("://", 1)

        if "@" not in rest:
            return url

        credentials_part, host_part = rest.rsplit("@", 1)

        if ":" in credentials_part:
            masked_credentials = "***:***"
        else:
            masked_credentials = "***"

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
        reset_headers = ["x-ratelimit-reset", "x-rate-limit-reset", "ratelimit-reset"]

        for reset_header in reset_headers:
            reset_time = headers.get(reset_header)
            if reset_time:
                try:
                    reset_dt = datetime.fromisoformat(reset_time.replace("Z"))
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
        remaining_headers = [
            "x-ratelimit-remaining",
            "x-rate-limit-remaining",
            "ratelimit-remaining",
        ]

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
                        retry_after = headers.get("retry-after")
                        if retry_after:
                            try:
                                return float(retry_after)
                            except ValueError:
                                pass

                        return (
                            10 * self.default_delay
                        )  # Default when no requests remaining
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
        retry_after = headers.get("retry-after")
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
            retry_after = headers.get("retry-after")
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


class HistoricalFetchResult:
    """Result of a historical data fetch with water marks for resumability"""

    def __init__(self):
        self.total_points_sent: int = 0
        self.high_water_mark: Optional[datetime] = None
        self.low_water_mark: Optional[datetime] = None

    def update(self, batch_data: List[Dict[str, Any]]):
        timestamps = [datetime.fromisoformat(p["x"]) for p in batch_data]
        batch_max = max(timestamps)
        batch_min = min(timestamps)
        if self.high_water_mark is None or batch_max > self.high_water_mark:
            self.high_water_mark = batch_max
        if self.low_water_mark is None or batch_min < self.low_water_mark:
            self.low_water_mark = batch_min
        self.total_points_sent += len(batch_data)


class HistoricalDataFetcher:
    """
    Generic historical data fetcher with adaptive batch sizing.
    Handles the complex logic of fetching historical data in optimal batches.
    """

    def __init__(self, wrapper_id: str, get_interval_seconds_func: Callable[[], int]):
        self.wrapper_id = wrapper_id
        self.get_interval_seconds = get_interval_seconds_func

    async def fetch_all_historical_data(
        self,
        fetch_date_range_func: Callable[
            [datetime, datetime], Awaitable[List[Dict[str, Any]]]
        ],
        send_to_queue_func: Callable[[List[Dict[str, Any]]], Awaitable[None]],
    ) -> HistoricalFetchResult:
        """
        Fetch all available historical data using adaptive batch sizing.

        Reads env vars for resume:
        - RESUME_PHASE: "historical" or "continuous"
        - RESUME_HIGH_WATER_MARK: newest data point timestamp already sent (ISO format)
        - RESUME_LOW_WATER_MARK: oldest data point timestamp already sent (ISO format)

        Resume strategies:
        - phase=historical: fill gap (high_water_mark→now), then continue backward from low_water_mark
        - phase=continuous: fill gap (high_water_mark→now) only
        - no env vars: full historical fetch from now→oldest
        """
        resume_phase = os.environ.get("RESUME_PHASE")
        resume_hwm = self._parse_env_datetime("RESUME_HIGH_WATER_MARK")
        resume_lwm = self._parse_env_datetime("RESUME_LOW_WATER_MARK")

        result = HistoricalFetchResult()

        if resume_phase and resume_hwm:
            print(
                f"Wrapper {self.wrapper_id}: Resuming from phase={resume_phase}, hwm={resume_hwm}, lwm={resume_lwm}"
            )

            # Fill the gap: high_water_mark → now
            gap_points = await self._fetch_range_adaptive(
                fetch_date_range_func,
                send_to_queue_func,
                result,
                start_boundary=resume_hwm,
                end_boundary=datetime.now(),
                direction="forward",
            )
            print(
                f"Wrapper {self.wrapper_id}: Gap fill complete. {gap_points} points sent."
            )

            if resume_phase == "historical" and resume_lwm:
                # Continue backward from low_water_mark → oldest
                print(
                    f"Wrapper {self.wrapper_id}: Continuing historical fetch backward from {resume_lwm}"
                )
                await self._fetch_range_adaptive(
                    fetch_date_range_func,
                    send_to_queue_func,
                    result,
                    start_boundary=None,
                    end_boundary=resume_lwm,
                    direction="backward",
                )
        else:
            # Fresh start: now → oldest
            print(f"Wrapper {self.wrapper_id}: Starting full historical data fetch...")
            await self._fetch_range_adaptive(
                fetch_date_range_func,
                send_to_queue_func,
                result,
                start_boundary=None,
                end_boundary=datetime.now(),
                direction="backward",
            )

        print(
            f"Wrapper {self.wrapper_id}: Historical data fetch completed. Total points sent: {result.total_points_sent}"
        )
        return result

    def _parse_env_datetime(self, key: str) -> Optional[datetime]:
        val = os.environ.get(key)
        if not val:
            return None
        try:
            return datetime.fromisoformat(val)
        except ValueError:
            print(f"Wrapper {self.wrapper_id}: Invalid {key} value '{val}', ignoring")
            return None

    async def _fetch_range_adaptive(
        self,
        fetch_date_range_func,
        send_to_queue_func,
        result: HistoricalFetchResult,
        start_boundary: Optional[datetime],
        end_boundary: datetime,
        direction: str,
    ) -> int:
        """Adaptive batch fetcher that works in either direction.

        Args:
            start_boundary: Stop condition for backward fetch (None = go until no data).
                           Start point for forward fetch.
            end_boundary: Start point for backward fetch. Stop condition for forward fetch.
            direction: "backward" (newer→older) or "forward" (older→newer)
        """
        batch_size_seconds = 100 * self.get_interval_seconds()
        max_time_span_found = 0
        stable_iterations = 0
        consecutive_errors = 0
        previous_edge_date = None
        no_progress_iterations = 0
        points_sent = 0

        if direction == "backward":
            cursor = end_boundary
        else:
            cursor = start_boundary

        while True:
            if direction == "backward":
                end_date = cursor
                start_date = cursor - timedelta(seconds=batch_size_seconds)
            else:
                start_date = cursor
                end_date = cursor + timedelta(seconds=batch_size_seconds)
                if end_date > end_boundary:
                    end_date = end_boundary

            try:
                batch_data = await fetch_date_range_func(start_date, end_date)
                consecutive_errors = 0

                if not batch_data:
                    print(
                        f"Wrapper {self.wrapper_id}: No data for {start_date.year}-{end_date.year}. Done."
                    )
                    break

                batch_data = sorted(batch_data, key=lambda x: x["x"])

                # Filter out data outside boundaries
                if start_boundary and direction == "backward":
                    original_count = len(batch_data)
                    batch_data = [
                        p
                        for p in batch_data
                        if datetime.fromisoformat(p["x"]) > start_boundary
                    ]
                    if not batch_data:
                        print(f"Wrapper {self.wrapper_id}: Reached boundary. Done.")
                        break
                    if len(batch_data) < original_count:
                        print(
                            f"Wrapper {self.wrapper_id}: Filtered {original_count - len(batch_data)} points outside boundary"
                        )

                first_datetime = datetime.fromisoformat(batch_data[0]["x"])
                last_datetime = datetime.fromisoformat(batch_data[-1]["x"])
                time_span_seconds = (last_datetime - first_datetime).total_seconds()

                # Progress tracking
                edge_date = first_datetime if direction == "backward" else last_datetime
                if previous_edge_date is None:
                    previous_edge_date = edge_date
                    no_progress_iterations = 0
                elif (direction == "backward" and edge_date >= previous_edge_date) or (
                    direction == "forward" and edge_date <= previous_edge_date
                ):
                    no_progress_iterations += 1
                    if no_progress_iterations >= 3:
                        print(
                            f"Wrapper {self.wrapper_id}: No progress for {no_progress_iterations} iterations. Done."
                        )
                        break
                else:
                    previous_edge_date = edge_date
                    no_progress_iterations = 0

                # Move cursor
                if direction == "backward":
                    cursor = first_datetime
                else:
                    cursor = last_datetime
                    if cursor >= end_boundary:
                        await send_to_queue_func(batch_data)
                        result.update(batch_data)
                        points_sent += len(batch_data)
                        print(f"Wrapper {self.wrapper_id}: Reached end boundary. Done.")
                        break

                # Adaptive batch sizing
                if time_span_seconds > max_time_span_found:
                    max_time_span_found = time_span_seconds
                    stable_iterations = 0
                else:
                    stable_iterations += 1

                if stable_iterations >= 3:
                    batch_size_seconds = (
                        int(time_span_seconds) + self.get_interval_seconds()
                    )
                elif time_span_seconds >= batch_size_seconds * 0.8:
                    batch_size_seconds *= 2
                elif time_span_seconds < batch_size_seconds * 0.5:
                    batch_size_seconds = (
                        int(time_span_seconds) + self.get_interval_seconds()
                    )

                await send_to_queue_func(batch_data)
                result.update(batch_data)
                points_sent += len(batch_data)
                print(
                    f"Wrapper {self.wrapper_id}: Sent {len(batch_data)} points ({direction})"
                )

            except (
                ValueError,
                KeyError,
                TypeError,
                ConnectionError,
                TimeoutError,
            ) as e:
                error_message = str(e).lower()
                consecutive_errors += 1

                if (
                    "400" in error_message
                    or "bad request" in error_message
                    or "time range" in error_message
                    or "date range" in error_message
                ):
                    batch_size_seconds = batch_size_seconds // 2
                    stable_iterations = 3
                    if batch_size_seconds <= self.get_interval_seconds():
                        print(
                            f"Wrapper {self.wrapper_id}: Batch size at minimum. Stopping."
                        )
                        break
                elif consecutive_errors >= 3:
                    print(
                        f"Wrapper {self.wrapper_id}: Too many errors ({consecutive_errors}). Stopping. Last: {e}"
                    )
                    break

        return points_sent


class MessageQueueSender:
    """
    Handles sending data to RabbitMQ with message chunking support
    """

    MAX_POINTS_PER_MESSAGE = settings.MAX_POINTS_PER_MESSAGE

    def __init__(self, wrapper_id: str, rabbitmq_url: str, metadata: dict = None):
        self.wrapper_id = wrapper_id
        self.rabbitmq_url = rabbitmq_url
        self.metadata = metadata or {}
        self.current_phase: str = "historical"

    async def send_to_queue(self, data_points: List[Dict[str, Any]]):
        """Send formatted data to RabbitMQ queue with automatic chunking"""
        if not data_points:
            print(f"Wrapper {self.wrapper_id}: No data points to send")
            return

        total_points = len(data_points)

        try:
            print(
                f"Wrapper {self.wrapper_id}: Connecting to RabbitMQ at {mask_credentials(self.rabbitmq_url)}"
            )
            connection = await aio_pika.connect_robust(self.rabbitmq_url)

            async with connection:
                channel = await connection.channel()
                queue = await channel.declare_queue(settings.DATA_QUEUE, durable=True)

                if total_points <= self.MAX_POINTS_PER_MESSAGE:
                    message_data = {
                        "wrapper_id": self.wrapper_id,
                        "data": data_points,
                        "metadata": self.metadata,
                        "phase": self.current_phase,
                        "total_segments": 1,
                        "segment_number": 1,
                    }

                    message = aio_pika.Message(
                        body=json.dumps(message_data).encode(),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    )

                    await channel.default_exchange.publish(
                        message, routing_key=queue.name
                    )
                    print(
                        f"Wrapper {self.wrapper_id}: Sent {total_points} data points in 1 message"
                    )
                else:
                    total_segments = (
                        total_points + self.MAX_POINTS_PER_MESSAGE - 1
                    ) // self.MAX_POINTS_PER_MESSAGE
                    print(
                        f"Wrapper {self.wrapper_id}: Splitting {total_points} data points into {total_segments} messages"
                    )

                    for segment_num in range(total_segments):
                        start_idx = segment_num * self.MAX_POINTS_PER_MESSAGE
                        end_idx = min(
                            start_idx + self.MAX_POINTS_PER_MESSAGE, total_points
                        )
                        chunk = data_points[start_idx:end_idx]

                        message_data = {
                            "wrapper_id": self.wrapper_id,
                            "data": chunk,
                            "metadata": self.metadata,
                            "phase": self.current_phase,
                            "total_segments": total_segments,
                            "segment_number": segment_num + 1,
                        }

                        message = aio_pika.Message(
                            body=json.dumps(message_data).encode(),
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        )

                        await channel.default_exchange.publish(
                            message, routing_key=queue.name
                        )
                        print(
                            f"Wrapper {self.wrapper_id}: Sent segment {segment_num + 1}/{total_segments} with {len(chunk)} data points"
                        )

                    print(
                        f"Wrapper {self.wrapper_id}: Completed sending {total_points} data points in {total_segments} messages"
                    )

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

    if "minute" in periodicity_lower:
        if "5" in periodicity_lower:
            return 5 * 60
        elif "10" in periodicity_lower:
            return 10 * 60
        elif "15" in periodicity_lower:
            return 15 * 60
        elif "30" in periodicity_lower:
            return 30 * 60
        else:
            return 60  # Default to 1 minute
    elif "hour" in periodicity_lower:
        return 3600
    elif "daily" in periodicity_lower or "day" in periodicity_lower:
        return 3600 * 24
    elif "weekly" in periodicity_lower or "week" in periodicity_lower:
        return 3600 * 24 * 7
    elif "monthly" in periodicity_lower or "month" in periodicity_lower:
        return 3600 * 24 * 30
    else:
        # Default to daily if not recognized
        return 3600 * 24


class ContinuousExecutor:
    """
    Handles continuous execution of wrappers with proper error handling and intervals.
    After historical fetch, uses fetch_date_range(high_water_mark, now) to avoid data loss.
    """

    def __init__(self, wrapper_id: str, interval_seconds: int):
        self.wrapper_id = wrapper_id
        self.interval_seconds = interval_seconds
        self.high_water_mark: Optional[datetime] = None

    async def run_continuous(
        self,
        run_once_func: Callable[[], Awaitable[None]],
        fetch_historical_func: Optional[
            Callable[[], Awaitable[HistoricalFetchResult]]
        ] = None,
        fetch_date_range_func: Optional[
            Callable[[datetime, datetime], Awaitable[List[Dict[str, Any]]]]
        ] = None,
        send_to_queue_func: Optional[
            Callable[[List[Dict[str, Any]]], Awaitable[None]]
        ] = None,
        source_type: str = "API",
        on_phase_change: Optional[Callable[[str], None]] = None,
    ):
        """Run the wrapper continuously with intervals based on periodicity.

        For API sources with fetch_date_range_func: uses high_water_mark-based
        continuous fetching to guarantee no data gaps.
        Falls back to run_once_func if fetch_date_range_func is not provided.
        """
        if fetch_historical_func and source_type == "API":
            try:
                print(
                    f"Wrapper {self.wrapper_id}: Fetching historical data before starting continuous mode..."
                )
                result = await fetch_historical_func()
                if isinstance(result, HistoricalFetchResult) and result.high_water_mark:
                    self.high_water_mark = result.high_water_mark
                    print(
                        f"Wrapper {self.wrapper_id}: Historical fetch complete. High water mark: {self.high_water_mark}"
                    )
                else:
                    print(
                        f"Wrapper {self.wrapper_id}: Historical data fetch completed (no water mark)"
                    )
            except (
                ValueError,
                KeyError,
                TypeError,
                ConnectionError,
                TimeoutError,
                OSError,
            ) as e:
                print(
                    f"Wrapper {self.wrapper_id}: Error fetching historical data: {str(e)}"
                )
                print("Proceeding with continuous mode for current data only...")

        # If resuming and no historical fetch ran, read high_water_mark from env
        if self.high_water_mark is None:
            hwm_env = os.environ.get("RESUME_HIGH_WATER_MARK")
            if hwm_env:
                try:
                    self.high_water_mark = datetime.fromisoformat(hwm_env)
                except ValueError:
                    pass

        # Transition to continuous phase
        if on_phase_change:
            on_phase_change("continuous")

        use_date_range = (
            fetch_date_range_func is not None
            and send_to_queue_func is not None
            and self.high_water_mark is not None
        )

        if use_date_range:
            print(
                f"Wrapper {self.wrapper_id}: Continuous mode using date-range fetching from {self.high_water_mark}"
            )
        else:
            print(f"Wrapper {self.wrapper_id}: Continuous mode using run_once")
        print(f"Interval: {self.interval_seconds}s")

        while True:
            try:
                if use_date_range:
                    now = datetime.now()
                    gap_seconds = (now - self.high_water_mark).total_seconds()

                    if gap_seconds > self.interval_seconds * 2:
                        # Large gap: use adaptive batching
                        print(
                            f"Wrapper {self.wrapper_id}: Filling gap of {int(gap_seconds)}s using adaptive batching"
                        )
                        gap_fetcher = HistoricalDataFetcher(
                            self.wrapper_id, lambda: self.interval_seconds
                        )
                        gap_result = HistoricalFetchResult()
                        await gap_fetcher._fetch_range_adaptive(
                            fetch_date_range_func,
                            send_to_queue_func,
                            gap_result,
                            start_boundary=self.high_water_mark,
                            end_boundary=now,
                            direction="forward",
                        )
                        if (
                            gap_result.high_water_mark
                            and gap_result.high_water_mark > self.high_water_mark
                        ):
                            self.high_water_mark = gap_result.high_water_mark
                            print(
                                f"Wrapper {self.wrapper_id}: Gap filled. {gap_result.total_points_sent} points. HWM: {self.high_water_mark}"
                            )
                        else:
                            print(f"Wrapper {self.wrapper_id}: No new data in gap")
                    else:
                        data = await fetch_date_range_func(self.high_water_mark, now)
                        if data:
                            new_data = [
                                p
                                for p in data
                                if datetime.fromisoformat(p["x"]) > self.high_water_mark
                            ]
                            if new_data:
                                await send_to_queue_func(new_data)
                                newest = max(
                                    datetime.fromisoformat(p["x"]) for p in new_data
                                )
                                self.high_water_mark = newest
                                print(
                                    f"Wrapper {self.wrapper_id}: Sent {len(new_data)} points. HWM: {newest}"
                                )
                            else:
                                print(
                                    f"Wrapper {self.wrapper_id}: No new data since {self.high_water_mark}"
                                )
                        else:
                            print(
                                f"Wrapper {self.wrapper_id}: No data for {self.high_water_mark} to {now}"
                            )
                else:
                    await run_once_func()

                print(
                    f"Wrapper {self.wrapper_id}: Waiting {self.interval_seconds}s until next execution..."
                )
                await asyncio.sleep(self.interval_seconds)
            except KeyboardInterrupt:
                print(f"Wrapper {self.wrapper_id}: Stopping execution")
                break
            except (
                ValueError,
                KeyError,
                TypeError,
                ConnectionError,
                TimeoutError,
                OSError,
            ) as e:
                print(
                    f"Wrapper {self.wrapper_id}: Error in continuous execution: {str(e)}"
                )
                print(f"Waiting {self.interval_seconds}s before retry...")
                await asyncio.sleep(self.interval_seconds)
