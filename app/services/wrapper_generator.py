import asyncio
import ast
import aio_pika
import json
import logging
import re
from google import genai
from google.genai import types
from datetime import datetime, timedelta
import os
import importlib.util
import tempfile
import sys
from typing import Dict, List, Any, Optional
import requests
import random
import contextvars
import pandas as pd
from .prompt_manager import PromptManager
from .wrapper_generation_tools import create_tool_runtime

# google.genai error type, imported optionally so retry logic still works (and
# the module still imports) if the SDK reshuffles things in a future version.
try:
    from google.genai import errors as _genai_errors  # type: ignore
    _GENAI_API_ERROR_TYPE = getattr(_genai_errors, "APIError", None)
except Exception:  # pragma: no cover — defensive
    _GENAI_API_ERROR_TYPE = None

logger = logging.getLogger(__name__)

# --- Per-wrapper generation progress log -----------------------------------
# The "view logs" modal reads /app/wrapper_logs/{id}_stdout.log — the same file
# the executing subprocess writes to. During the generation phase there is no
# subprocess, so we append progress lines here to stream what's happening
# (model in use, retries, fallbacks, tool calls, validation) instead of a
# single static "generating" message. Best-effort: never fail generation over
# a log write.
WRAPPER_LOGS_DIR = "/app/wrapper_logs"

# Bound the active wrapper_id to the current async task so deep helpers
# (retry/fallback) can emit progress without threading the id through every
# call. Set per-generation in generate_wrapper; contextvars are task-local so
# concurrent generations don't clobber each other.
_progress_wrapper_id: "contextvars.ContextVar[Optional[str]]" = contextvars.ContextVar(
    "progress_wrapper_id", default=None
)


def append_wrapper_log(wrapper_id: str, message: str, reset: bool = False) -> None:
    """Append (or, with reset=True, truncate-then-write) a progress line to a
    wrapper's stdout log file. Shared by the generator and the orchestration
    service so the logs modal shows a live trail during generation."""
    if not wrapper_id:
        return
    try:
        os.makedirs(WRAPPER_LOGS_DIR, exist_ok=True)
        with open(f"{WRAPPER_LOGS_DIR}/{wrapper_id}_stdout.log", "w" if reset else "a") as f:
            f.write(f"[{datetime.now().isoformat()}] {message}\n")
    except OSError:
        pass


def _emit_progress(message: str) -> None:
    """Emit a progress line for whichever wrapper the current task is generating."""
    append_wrapper_log(_progress_wrapper_id.get(), message)


class IndicatorMetadata:
    """
    Class to hold indicator metadata
    """

    def __init__(
        self,
        name: str,
        domain: str,
        subdomain: str,
        description: str,
        unit: str,
        source: str,
        scale: str,
        governance_indicator: bool,
        carrying_capacity: Optional[float],
        periodicity: str,
    ):
        self.name = name
        self.domain = domain
        self.subdomain = subdomain
        self.description = description
        self.unit = unit
        self.source = source
        self.scale = scale
        self.governance_indicator = governance_indicator
        self.carrying_capacity = carrying_capacity
        self.periodicity = periodicity


class DataSourceConfig:
    """
    Configuration for different data source types
    """

    def __init__(
        self,
        source_type: str,  # "API", "CSV", "XLSX"
        location: str,  # endpoint URL or file path
        auth_config: Optional[Dict[str, Any]] = None,
    ):  # authentication config for APIs
        self.source_type = source_type
        self.location = location
        self.auth_config = auth_config or {}


class DebugLogger:
    """Logger for debug mode that saves prompts and responses"""

    def __init__(self, debug_mode: bool = False, debug_dir: str = "prompts"):
        self.debug_mode = debug_mode
        self.debug_dir = debug_dir

        if self.debug_mode:
            self._setup_debug_directory()

    def _setup_debug_directory(self):
        """Setup debug directory"""
        if not os.path.exists(self.debug_dir):
            os.makedirs(self.debug_dir)
            print(f"Debug mode activated: {self.debug_dir}/")

    def log_prompt_response(
        self, prompt: str, response: str, metadata: Dict[str, Any] = None
    ):
        """Save prompt and response to debug directory using wrapper_id"""
        if not self.debug_mode:
            return

        # Use wrapper_id from metadata as directory name
        wrapper_id = metadata.get("wrapper_id", "unknown") if metadata else "unknown"
        prompt_dir = os.path.join(self.debug_dir, str(wrapper_id))

        try:
            os.makedirs(prompt_dir, exist_ok=True)

            # Save prompt
            prompt_file = os.path.join(prompt_dir, "prompt.txt")
            with open(prompt_file, "w", encoding="utf-8") as f:
                f.write(prompt)

            # Save response
            response_file = os.path.join(prompt_dir, "response.txt")
            with open(response_file, "w", encoding="utf-8") as f:
                f.write(response)

            # Save metadata if provided
            if metadata:
                metadata_file = os.path.join(prompt_dir, "metadata.json")
                with open(metadata_file, "w", encoding="utf-8") as f:
                    json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)

            print(f"Debug: Wrapper {wrapper_id} prompt saved to {prompt_dir}/")

        except (OSError, IOError, json.JSONDecodeError) as e:
            print(f"Error saving debug prompt for wrapper {wrapper_id}: {e}")

    def log_error(self, error: str, context: Dict[str, Any] = None):
        """Save error to debug using wrapper_id"""
        if not self.debug_mode:
            return

        # Use wrapper_id from context as directory name
        wrapper_id = context.get("wrapper_id", "unknown") if context else "unknown"
        error_dir = os.path.join(self.debug_dir, f"{wrapper_id}_ERROR")

        try:
            os.makedirs(error_dir, exist_ok=True)

            # Save error
            error_file = os.path.join(error_dir, "error.txt")
            with open(error_file, "w", encoding="utf-8") as f:
                f.write(f"ERROR: {error}\n")
                f.write(f"Timestamp: {datetime.now().isoformat()}\n")
                if context:
                    f.write(
                        f"\nContext:\n{json.dumps(context, indent=2, ensure_ascii=False, default=str)}"
                    )

            print(f"Debug: Wrapper {wrapper_id} error saved to {error_dir}/")

        except (OSError, IOError, json.JSONDecodeError) as e:
            print(f"Error saving debug error for wrapper {wrapper_id}: {e}")

    def log_generation_trace(self, wrapper_id: str, trace: Dict[str, Any]):
        """Save full generation trace including requests, responses, and tool calls"""
        if not self.debug_mode:
            return

        prompt_dir = os.path.join(self.debug_dir, str(wrapper_id))

        try:
            os.makedirs(prompt_dir, exist_ok=True)

            trace_file = os.path.join(prompt_dir, "generation_trace.json")
            with open(trace_file, "w", encoding="utf-8") as f:
                json.dump(trace, f, indent=2, ensure_ascii=False, default=str)

        except (OSError, IOError, json.JSONDecodeError) as e:
            print(f"Error saving generation trace for wrapper {wrapper_id}: {e}")

    def log_lint_result(self, wrapper_id: str, lint_result: Dict[str, Any]):
        """Save lint check result to debug directory"""
        if not self.debug_mode:
            return

        prompt_dir = os.path.join(self.debug_dir, str(wrapper_id))

        try:
            os.makedirs(prompt_dir, exist_ok=True)

            lint_file = os.path.join(prompt_dir, "lint_result.json")
            with open(lint_file, "w", encoding="utf-8") as f:
                json.dump(lint_result, f, indent=2, ensure_ascii=False, default=str)

        except (OSError, IOError, json.JSONDecodeError) as e:
            print(f"Error saving lint result for wrapper {wrapper_id}: {e}")


class WrapperGenerator:
    def __init__(
        self,
        gemini_api_key: str,
        rabbitmq_url: str = "amqp://guest:guest@localhost/",
        debug_mode: bool = False,
        debug_dir: str = "prompts",
        model_name: str = "gemini-2.5-flash",
        fallback_models: Optional[List[str]] = None,
    ):
        """
        Initialize the wrapper generator with Gemini API key and RabbitMQ connection

        Args:
            gemini_api_key: API key for Gemini
            rabbitmq_url: RabbitMQ connection URL
            debug_mode: Enable debug mode for saving prompts/responses
            debug_dir: Directory to save debug files
            model_name: Gemini model name to use (default: gemini-2.5-flash)
        """
        self.gemini_api_key = gemini_api_key
        self.rabbitmq_url = rabbitmq_url
        self.debug_logger = DebugLogger(debug_mode, debug_dir)
        # Give the transport its own timeout (ms) so a stalled connection can
        # surface below the per-attempt asyncio.wait_for ceiling. NOTE: some
        # google-genai versions don't honour this on the async path
        # (python-genai #911), so it's belt-and-suspenders — the real backstops
        # are the per-attempt asyncio.wait_for and the aggregate budget below.
        try:
            self.client = genai.Client(
                api_key=gemini_api_key,
                http_options=types.HttpOptions(timeout=60_000),
            )
        except Exception:  # pragma: no cover — SDK without HttpOptions.timeout
            self.client = genai.Client(api_key=gemini_api_key)
        self.model_name = model_name
        # Models to fall back to when the primary is overloaded/unavailable.
        self.fallback_models = fallback_models or []
        self.prompt_manager = PromptManager()
        logger.info(
            f"WrapperGenerator using Gemini model '{self.model_name}'"
            + (f" (fallbacks: {', '.join(self.fallback_models)})"
               if self.fallback_models else " (no fallbacks)")
        )

    def _build_api_auth_config(self, source_config: DataSourceConfig) -> Dict[str, Any]:
        auth_config: Dict[str, Any] = {}
        if getattr(source_config, "auth_type", None) == "api_key" and getattr(
            source_config, "api_key", None
        ):
            auth_config["api_key"] = source_config.api_key
            auth_config["header_name"] = (
                getattr(source_config, "api_key_header", None) or "X-API-Key"
            )
        elif getattr(source_config, "auth_type", None) == "bearer" and getattr(
            source_config, "bearer_token", None
        ):
            auth_config["headers"] = {
                "Authorization": f"Bearer {source_config.bearer_token}"
            }
        elif (
            getattr(source_config, "auth_type", None) == "basic"
            and getattr(source_config, "username", None)
            and getattr(source_config, "password", None)
        ):
            import base64

            credentials = base64.b64encode(
                f"{source_config.username}:{source_config.password}".encode()
            ).decode()
            auth_config["headers"] = {"Authorization": f"Basic {credentials}"}

        if getattr(source_config, "custom_headers", None):
            auth_config.setdefault("headers", {}).update(source_config.custom_headers)
        if getattr(source_config, "query_params", None):
            auth_config["params"] = source_config.query_params

        return auth_config

    def _validate_generated_code(self, generated_code: str) -> Optional[str]:
        try:
            ast.parse(generated_code)
        except SyntaxError as e:
            return f"SyntaxError at line {e.lineno}, col {e.offset}: {e.msg}"

        if re.search(r"^\s*\.\.\.\s*$", generated_code, re.MULTILINE):
            return "Critical linting error: unresolved placeholder '...' found in code"
        if "PLACEHOLDER" in generated_code:
            return "Critical linting error: unresolved PLACEHOLDER token found in code"

        return None

    # Per-attempt ceiling for a single generate_content call. A flash code-gen
    # call returns in well under a minute; anything approaching this is a
    # stalled connection (the client has no reliable transport-level timeout —
    # and an overloaded gemini-2.5-flash is known to hold the socket open with
    # no data instead of returning 503, python-genai #1893). Keep it tight so a
    # hang is detected fast and we fail over instead of sitting in "generating".
    _GEMINI_CALL_TIMEOUT_S = 60
    # A hung socket almost never clears on an in-place retry against the SAME
    # model, so timeouts get a tiny budget and then fall over to the next model,
    # rather than burning the full transient-error budget below.
    _MAX_TIMEOUT_RETRIES = 1
    # Hard aggregate ceiling for one logical generation call across ALL its
    # per-attempt retries and model fail-overs. Without this, 6 timeouts ×
    # several candidate models stacked into multi-minute "generating" stalls.
    _GEMINI_TOTAL_BUDGET_S = 180

    # Transient Gemini failures — 503 "the model is overloaded", 429
    # rate-limits, and 5xx server errors — are common and almost always clear
    # within seconds. Retry with exponential backoff + jitter instead of
    # failing the whole generation on the first blip and forcing the admin to
    # click Regenerate by hand.
    _MAX_GEMINI_RETRIES = 5
    _BASE_BACKOFF_S = 2.0
    _MAX_BACKOFF_S = 30.0
    # A server-suggested retry longer than this signals a long quota window
    # (e.g. a daily free-tier reset), not a transient blip — surface it
    # instead of spinning on it.
    _MAX_HONORED_RETRY_DELAY_S = 120.0
    _RETRYABLE_CODES = frozenset({429, 500, 502, 503, 504})
    _RETRYABLE_STATUSES = frozenset(
        {"UNAVAILABLE", "RESOURCE_EXHAUSTED", "INTERNAL", "DEADLINE_EXCEEDED"}
    )
    _GENAI_API_ERROR = _GENAI_API_ERROR_TYPE

    @classmethod
    def _is_transient_gemini_error(cls, exc: Exception) -> bool:
        """True if exc is a Gemini API error worth retrying. Only genai
        APIErrors qualify — never an arbitrary exception that happens to carry
        a numeric .code attribute."""
        if cls._GENAI_API_ERROR is not None and not isinstance(
            exc, cls._GENAI_API_ERROR
        ):
            return False
        code = getattr(exc, "code", None)
        status = (getattr(exc, "status", None) or "").upper()
        return code in cls._RETRYABLE_CODES or status in cls._RETRYABLE_STATUSES

    @staticmethod
    def _gemini_retry_delay_s(exc: Exception) -> Optional[float]:
        """Extract the server-suggested retry delay (seconds) from a genai
        APIError's RetryInfo, if present. Returns None when absent or
        unparseable."""
        details = getattr(exc, "details", None)
        error = details.get("error") if isinstance(details, dict) else None
        items = error.get("details") if isinstance(error, dict) else None
        if not isinstance(items, list):
            return None
        for item in items:
            if isinstance(item, dict) and str(item.get("@type", "")).endswith(
                "RetryInfo"
            ):
                raw = str(item.get("retryDelay", "")).strip().rstrip("s")
                try:
                    return float(raw)
                except ValueError:
                    return None
        return None

    async def _generate_content_with_retry(
        self, *, timeout: Optional[int] = None, **gen_kwargs
    ):
        """Generate content, trying the configured model then any fallback
        models. Each model gets its own per-attempt timeout + backoff retry on
        transient errors; when one model is exhausted (e.g. a persistent 503
        overload) we fall over to the next so a single flaky model doesn't
        block all wrapper generation. Raises the last error only when every
        candidate model fails."""
        requested = gen_kwargs.pop("model", None) or self.model_name
        candidates = [requested] + [
            m for m in self.fallback_models if m and m != requested
        ]
        last_exc: Optional[Exception] = None
        try:
            # One hard ceiling for the whole call — across every per-attempt
            # retry AND every fallback model — so a persistently overloaded /
            # stalling model can never keep a wrapper in "generating" for
            # minutes. Cancellation propagates as CancelledError (BaseException,
            # so the per-candidate `except Exception` below won't swallow it).
            async with asyncio.timeout(self._GEMINI_TOTAL_BUDGET_S):
                for idx, model in enumerate(candidates):
                    try:
                        return await self._generate_one_model(
                            model, timeout=timeout, **gen_kwargs
                        )
                    except Exception as exc:  # noqa: BLE001 — re-raised after last candidate
                        last_exc = exc
                        if idx < len(candidates) - 1:
                            logger.warning(
                                f"Gemini model '{model}' failed "
                                f"({type(exc).__name__}: {exc}); falling back to "
                                f"'{candidates[idx + 1]}'"
                            )
                            _emit_progress(
                                f"Model '{model}' unavailable — trying "
                                f"'{candidates[idx + 1]}'…"
                            )
                            continue
                        raise
        except asyncio.TimeoutError as exc:
            raise ValueError(
                f"Gemini generation exceeded the overall "
                f"{self._GEMINI_TOTAL_BUDGET_S}s budget across "
                f"{len(candidates)} model(s)"
            ) from (last_exc or exc)
        if last_exc:
            raise last_exc

    async def _generate_one_model(
        self, model: str, *, timeout: Optional[int] = None, **gen_kwargs
    ):
        """Single-model generate with a per-attempt timeout and exponential-
        backoff retry on transient Gemini errors. Raises ValueError on repeated
        timeout; re-raises the underlying error once retries are exhausted (or
        immediately for non-transient errors)."""
        per_attempt_timeout = timeout or self._GEMINI_CALL_TIMEOUT_S
        attempt = 0          # transient-error (503/429/5xx) retries
        timeout_attempts = 0  # hung-socket timeout retries (kept small)
        while True:
            try:
                return await asyncio.wait_for(
                    self.client.aio.models.generate_content(model=model, **gen_kwargs),
                    timeout=per_attempt_timeout,
                )
            except asyncio.TimeoutError as exc:
                # A hung socket rarely clears on an in-place retry against the
                # SAME model (an overloaded 2.5-flash just keeps stalling), so
                # timeouts get a tiny budget and then fail over to the next
                # candidate model instead of burning the full transient budget.
                timeout_attempts += 1
                if timeout_attempts > self._MAX_TIMEOUT_RETRIES:
                    raise ValueError(
                        f"Gemini call to '{model}' timed out after "
                        f"{per_attempt_timeout}s "
                        f"({self._MAX_TIMEOUT_RETRIES + 1} attempt(s))"
                    ) from exc
                delay = self._BASE_BACKOFF_S + random.uniform(0, 1.0)  # small jitter
                logger.warning(
                    f"Gemini call to '{model}' timed out after "
                    f"{per_attempt_timeout}s; retry "
                    f"{timeout_attempts}/{self._MAX_TIMEOUT_RETRIES} in {delay:.1f}s"
                )
                _emit_progress(
                    f"Gemini ('{model}') timed out — retrying…"
                )
                await asyncio.sleep(delay)
            except Exception as exc:  # noqa: BLE001 — re-raised below unless transient
                if not self._is_transient_gemini_error(exc):
                    raise
                attempt += 1
                if attempt >= self._MAX_GEMINI_RETRIES:
                    logger.warning(
                        f"Gemini still failing after {self._MAX_GEMINI_RETRIES} "
                        f"retries; giving up ({type(exc).__name__})"
                    )
                    raise
                suggested = self._gemini_retry_delay_s(exc)
                if (
                    suggested is not None
                    and suggested > self._MAX_HONORED_RETRY_DELAY_S
                ):
                    logger.warning(
                        f"Gemini asked to retry in {suggested:.0f}s "
                        f"(> {self._MAX_HONORED_RETRY_DELAY_S:.0f}s); not waiting"
                    )
                    raise
                backoff = min(
                    self._BASE_BACKOFF_S * (2 ** (attempt - 1)), self._MAX_BACKOFF_S
                )
                delay = suggested if suggested is not None else backoff
                delay += random.uniform(0, max(delay, 1.0) * 0.25)  # jitter
                code = getattr(exc, "code", None) or getattr(exc, "status", "?")
                logger.warning(
                    f"Gemini transient error ({code}); retry "
                    f"{attempt}/{self._MAX_GEMINI_RETRIES} in {delay:.1f}s"
                )
                _emit_progress(
                    f"Gemini ('{model}') busy ({code}) — retry "
                    f"{attempt}/{self._MAX_GEMINI_RETRIES} in {delay:.0f}s…"
                )
                await asyncio.sleep(delay)

    async def _call_model(self, prompt: str) -> str:
        response = await self._generate_content_with_retry(
            model=self.model_name,
            contents=prompt,
        )
        return (response.text or "").strip()

    # Sidecar: ask Gemini to produce PT/EN translations for the data columns
    # we ingested. Used to seed the column-level translation editor in the
    # admin UI — the admin can override anything wrong. Returns a dict of
    # {original_label: {"pt": str, "en": str}}; failures yield {} so the
    # caller can fall back gracefully without breaking wrapper completion.
    async def translate_value_columns(
        self,
        value_columns: List[str],
        indicator_name: str = "",
        indicator_description: str = "",
    ) -> Dict[str, Dict[str, str]]:
        labels = [c for c in (value_columns or []) if isinstance(c, str) and c.strip()]
        if not labels:
            return {}

        prompt = (
            "You translate data-column headers for a sustainability/tourism "
            "indicators dashboard. Given the indicator context and a list of "
            "column names, return a JSON object mapping each original column "
            "name to {\"pt\": Portuguese (Portugal) label, \"en\": English "
            "label}. Keep labels short (<= 60 chars), Title Case, no quotes "
            "or punctuation beyond what is meaningful, and keep proper nouns "
            "(places, organisations) untranslated. If a label is already in "
            "the target language, copy it as-is.\n\n"
            f"INDICATOR NAME: {indicator_name or '-'}\n"
            f"INDICATOR DESCRIPTION: {indicator_description or '-'}\n"
            f"COLUMN NAMES: {json.dumps(labels, ensure_ascii=False)}\n\n"
            "Return ONLY the JSON object — no prose, no code fences."
        )

        try:
            config = types.GenerateContentConfig(response_mime_type="application/json")
            response = await self._generate_content_with_retry(
                model=self.model_name,
                contents=prompt,
                config=config,
                timeout=60,
            )
            raw = (response.text or "").strip()
            if not raw:
                return {}
            parsed = json.loads(raw)
            if not isinstance(parsed, dict):
                return {}
            cleaned: Dict[str, Dict[str, str]] = {}
            for label in labels:
                entry = parsed.get(label)
                if isinstance(entry, dict):
                    pt = str(entry.get("pt") or "").strip()
                    en = str(entry.get("en") or "").strip()
                else:
                    pt, en = "", ""
                cleaned[label] = {"pt": pt or label, "en": en or label}
            return cleaned
        except (asyncio.TimeoutError, json.JSONDecodeError, ValueError) as exc:
            logger.warning(
                f"translate_value_columns failed: {type(exc).__name__}: {exc}"
            )
            return {}
        except Exception as exc:  # noqa: BLE001 — never block wrapper completion
            logger.warning(
                f"translate_value_columns unexpected error: {type(exc).__name__}: {exc}"
            )
            return {}

    async def _call_model_with_tools(
        self,
        prompt: str,
        auth_config: Dict[str, Any],
        max_tool_calls: int = 15,
        max_chars: int = 2500,
        wrapper_id: str = None,
    ) -> str:
        runtime = create_tool_runtime(
            auth_config=auth_config,
            max_chars=max_chars,
        )

        config = types.GenerateContentConfig(
            tools=runtime.get_tools(),
            automatic_function_calling=types.AutomaticFunctionCallingConfig(
                disable=True
            ),
            tool_config=types.ToolConfig(
                function_calling_config=types.FunctionCallingConfig(mode="AUTO")
            ),
        )

        contents: List[types.Content] = [
            types.Content(role="user", parts=[types.Part.from_text(text=prompt)])
        ]
        tool_calls_used = 0
        trace: Dict[str, Any] = {
            "initial_prompt": prompt,
            "turns": [],
            "tool_calls": [],
            "final_response": None,
        }

        while True:
            response = await self._generate_content_with_retry(
                model=self.model_name,
                contents=contents,
                config=config,
            )

            function_calls = response.function_calls or []

            turn_data = {
                "timestamp": datetime.now().isoformat(),
                "has_function_calls": bool(function_calls),
                "function_call_count": len(function_calls),
                "response_text": response.text if not function_calls else None,
            }
            trace["turns"].append(turn_data)

            if not function_calls:
                trace["final_response"] = (response.text or "").strip()
                if wrapper_id:
                    self.debug_logger.log_generation_trace(wrapper_id, trace)
                return trace["final_response"]

            if response.candidates and response.candidates[0].content:
                contents.append(response.candidates[0].content)

            for function_call in function_calls:
                if tool_calls_used >= max_tool_calls:
                    break

                print(
                    f"Tool call {tool_calls_used + 1}/{max_tool_calls}: {function_call.name}({dict(function_call.args or {})})"
                )
                _emit_progress(
                    f"Inspecting source: {function_call.name} "
                    f"({tool_calls_used + 1}/{max_tool_calls})…"
                )
                tool_result = runtime.execute(
                    function_name=function_call.name,
                    args=dict(function_call.args or {}),
                )

                tool_calls_used += 1

                tool_call_event = {
                    "tool": function_call.name,
                    "args": dict(function_call.args or {}),
                    "result": tool_result,
                    "timestamp": datetime.now().isoformat(),
                }
                trace["tool_calls"].append(tool_call_event)

                contents.append(
                    types.Content(
                        role="tool",
                        parts=[
                            types.Part.from_function_response(
                                name=function_call.name,
                                response=tool_result,
                            )
                        ],
                    )
                )

            if tool_calls_used >= max_tool_calls:
                contents.append(
                    types.Content(
                        role="user",
                        parts=[
                            types.Part.from_text(
                                text="Tool call limit reached. Use available fetched samples and return the complete Python wrapper code now."
                            )
                        ],
                    )
                )
                final_response = await self._generate_content_with_retry(
                    model=self.model_name,
                    contents=contents,
                    config=types.GenerateContentConfig(),
                )
                trace["final_response"] = (final_response.text or "").strip()
                trace["turns"].append(
                    {
                        "timestamp": datetime.now().isoformat(),
                        "forced_completion": True,
                        "response_text": trace["final_response"],
                    }
                )
                if wrapper_id:
                    self.debug_logger.log_generation_trace(wrapper_id, trace)
                return trace["final_response"]

    def get_csv_sample(self, file_path: str, max_lines: int = 20) -> str:
        """
        Extract first 20 lines from CSV file for analysis
        """
        try:
            sample_lines = []
            with open(file_path, "r", encoding="utf-8") as file:
                for i, line in enumerate(file):
                    if i >= max_lines:
                        break
                    sample_lines.append(line.strip())

            return "\n".join(sample_lines)
        except (FileNotFoundError, IOError, UnicodeDecodeError) as e:
            return f"Error reading CSV file: {str(e)}"

    def get_xlsx_sample(self, file_path: str, max_lines_per_sheet: int = 15) -> str:
        """
        Extract pages and first 15 lines of each page from XLSX file
        """
        try:
            # Read Excel file and get all sheet names
            excel_file = pd.ExcelFile(file_path)
            sheet_names = excel_file.sheet_names

            sample_data = []
            sample_data.append(f"XLSX File: {file_path}")
            sample_data.append(f"Total sheets: {len(sheet_names)}")
            sample_data.append(f"Sheet names: {sheet_names}")
            sample_data.append("")

            # Sample from each sheet
            for sheet_name in sheet_names:
                sample_data.append(f"=== Sheet: {sheet_name} ===")
                try:
                    df = pd.read_excel(
                        file_path, sheet_name=sheet_name, nrows=max_lines_per_sheet
                    )
                    sample_data.append(f"Columns: {list(df.columns)}")
                    sample_data.append(f"Shape: {df.shape}")
                    sample_data.append("Sample data:")
                    sample_data.append(df.to_string())
                except (pd.errors.ParserError, ValueError) as sheet_error:
                    sample_data.append(
                        f"Error reading sheet {sheet_name}: {sheet_error}"
                    )
                sample_data.append("")

            return "\n".join(sample_data)
        except (FileNotFoundError, IOError, pd.errors.ParserError, ValueError) as e:
            return f"Error reading XLSX file: {str(e)}"

    def get_api_sample(
        self, endpoint: str, auth_config: Dict[str, Any], max_chars: int = 2500
    ) -> str:
        """
        Make API call and return response sample
        """
        try:
            print(f"Making API sample call to: {endpoint}")
            print(f"Auth config: {auth_config}")
            # Prepare headers
            headers = auth_config.get("headers", {})

            # Add API key to headers if specified
            if "api_key" in auth_config and "header_name" in auth_config:
                headers[auth_config["header_name"]] = auth_config["api_key"]

            # Prepare parameters
            params = auth_config.get("params", {})

            # Make API call with timeout
            response = requests.get(
                endpoint, headers=headers, params=params, timeout=30
            )

            # Check if request was successful
            response.raise_for_status()

            # Try to parse as JSON
            try:
                json_data = response.json()
                # Limit the sample size for large responses
                sample_text = json.dumps(json_data, indent=2)
                if len(sample_text) > max_chars:
                    sample_text = sample_text[:max_chars] + "\n... (truncated)"
                return f"API Response (Status: {response.status_code}):\n{sample_text}"
            except json.JSONDecodeError:
                # If not JSON, return text content
                content = response.text
                if len(content) > max_chars:
                    content = content[:max_chars] + "\n... (truncated)"
                return f"API Response (Status: {response.status_code}):\n{content}"

        except requests.exceptions.RequestException as e:
            return f"Error calling API: {str(e)}"
        except (json.JSONDecodeError, ValueError) as e:
            return f"Error parsing API response: {str(e)}"

    async def generate_wrapper(
        self,
        indicator_metadata: IndicatorMetadata,
        source_config: DataSourceConfig,
        source_type: str,
        wrapper_id: str,
    ) -> str:
        """
        Use Gemini to generate a customized wrapper based on indicator metadata and source configuration
        Note: source_config.location is always populated (computed from file_id for CSV/XLSX in the route handler)
        """

        auth_config: Dict[str, Any] = {}

        # Bind this wrapper id so deep helpers (retry/fallback/tool calls) can
        # stream progress into the per-wrapper log the modal reads.
        _progress_wrapper_id.set(wrapper_id)

        # Get data sample based on source type
        print(f"Extracting sample from {source_type} source...")
        _emit_progress(f"Reading {source_type} data sample…")
        if source_type == "CSV":
            data_sample = self.get_csv_sample(source_config.location)
        elif source_type == "XLSX":
            data_sample = self.get_xlsx_sample(source_config.location)
        elif source_type == "API":
            auth_config = self._build_api_auth_config(source_config)
            data_sample = self.get_api_sample(source_config.location, auth_config)
        else:
            data_sample = "Unknown source type"

        print(
            f"Data sample: {data_sample[:400]}" + "..."
            if len(data_sample) > 400
            else ""
        )

        prompt = self.prompt_manager.generate_wrapper_prompt(
            indicator_metadata, source_config, source_type, wrapper_id, data_sample
        )

        # Prepare metadata for debug
        debug_metadata = {
            "wrapper_id": wrapper_id,
            "indicator_name": indicator_metadata.name,
            "source_type": source_type,
            "source_location": source_config.location,
            "timestamp": datetime.now().isoformat(),
            "prompt_length": len(prompt),
            "data_sample_length": len(data_sample),
        }

        try:
            print("Calling Gemini...")
            _emit_progress(f"Asking Gemini ({self.model_name}) to write the wrapper…")
            if source_type == "API":
                generated_code = await self._call_model_with_tools(
                    prompt=prompt,
                    auth_config=auth_config,
                    max_tool_calls=15,
                    max_chars=2500,
                    wrapper_id=wrapper_id,
                )
            else:
                generated_code = await self._call_model(prompt)

            # Log prompt and response in debug mode
            self.debug_logger.log_prompt_response(
                prompt=prompt, response=generated_code, metadata=debug_metadata
            )

            # Clean markdown code block markers if present
            generated_code = self._clean_code_response(generated_code)

            # Verify that the code was actually customized (not just the template)
            template_code = self.prompt_manager.get_wrapper_template(
                source_type, indicator_metadata.periodicity
            )
            if generated_code == template_code:
                error_msg = "Gemini returned unchanged template"

                # If debug_mode is activated, show prompt and response
                if self.debug_logger.debug_mode:
                    print(f"\n{'=' * 80}")
                    print(f"DEBUG: {error_msg}")
                    print(f"Wrapper ID: {wrapper_id}")
                    print(f"{'=' * 80}")
                    print(f"\nPROMPT SENT TO GEMINI:")
                    print(f"{'-' * 40}")
                    print(prompt[:2000] + "..." if len(prompt) > 2000 else prompt)
                    print(f"\n{'-' * 40}")
                    print(f"\nGEMINI RESPONSE:")
                    print(f"{'-' * 40}")
                    print(
                        generated_code[:2000] + "..."
                        if len(generated_code) > 2000
                        else generated_code
                    )
                    print(f"\n{'-' * 40}")
                    print(f"Response length: {len(generated_code)} characters")
                    print(f"Contains PLACEHOLDER: {'PLACEHOLDER' in generated_code}")
                    print(f"Contains ...: {'...' in generated_code}")
                    print(f"Equals template: {generated_code == template_code}")
                    print(f"{'=' * 80}\n")

                self.debug_logger.log_error(error_msg, debug_metadata)
                raise ValueError(error_msg)

            _emit_progress("Validating generated code…")
            lint_error = self._validate_generated_code(generated_code)

            self.debug_logger.log_lint_result(
                wrapper_id,
                {
                    "passed": lint_error is None,
                    "error": lint_error,
                    "timestamp": datetime.now().isoformat(),
                },
            )

            if lint_error:
                retry_prompt = self.prompt_manager.generate_wrapper_lint_retry_prompt(
                    generated_code=generated_code,
                    linting_errors=lint_error,
                )
                retry_response = await self._call_model(retry_prompt)

                self.debug_logger.log_prompt_response(
                    prompt=retry_prompt,
                    response=retry_response,
                    metadata={**debug_metadata, "stage": "lint_retry"},
                )

                generated_code = self._clean_code_response(retry_response)
                retry_lint_error = self._validate_generated_code(generated_code)
                if retry_lint_error:
                    raise ValueError(
                        f"Critical linting errors after retry: {retry_lint_error}"
                    )

            return generated_code

        except (ValueError, AttributeError, TypeError) as e:
            error_msg = f"Error generating wrapper with Gemini: {str(e)}"
            print(error_msg)

            # Log error in debug mode
            debug_metadata["error"] = str(e)
            self.debug_logger.log_error(error_msg, debug_metadata)

            raise ValueError(f"Failed to generate customized wrapper: {error_msg}")

    def _clean_code_response(self, code: str) -> str:
        """
        Clean markdown code block markers from Gemini response
        """
        # Remove markdown code block markers
        if code.startswith("```python"):
            code = code[9:]  # Remove ```python
        elif code.startswith("```"):
            code = code[3:]  # Remove ```

        if code.endswith("```"):
            code = code[:-3]  # Remove trailing ```

        # Strip any remaining whitespace
        code = code.strip()

        return code

    def save_wrapper(self, wrapper_code: str, wrapper_id: str) -> str:
        """
        Save the generated wrapper to a file
        """
        # Create wrappers directory
        wrapper_dir = "/app/generated_wrappers"
        os.makedirs(wrapper_dir, exist_ok=True)

        # Simple filename pattern
        filename = f"{wrapper_id}.py"
        file_path = os.path.join(wrapper_dir, filename)

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(wrapper_code)

        print(f"Wrapper saved to: {file_path}")
        return file_path

    async def execute_wrapper(
        self,
        wrapper_file: str,
        wrapper_id: int,
        mode: str = "once",
        timeout_seconds: int = 8,
    ):
        """
        Execute the generated wrapper with timeout
        """
        try:
            # Load and execute the wrapper module
            spec = importlib.util.spec_from_file_location(
                f"wrapper_{wrapper_id}", wrapper_file
            )
            wrapper_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(wrapper_module)

            # Create wrapper instance
            wrapper = wrapper_module.ATTWrapper(wrapper_id)

            # Execute wrapper with timeout
            try:
                if mode == "once":
                    await asyncio.wait_for(wrapper.run_once(), timeout=timeout_seconds)
                else:
                    # For continuous mode, the wrapper handles its own scheduling
                    await asyncio.wait_for(
                        wrapper.run_continuous(), timeout=timeout_seconds
                    )

            except asyncio.TimeoutError:
                print(
                    f"Wrapper {wrapper_id} execution timed out after {timeout_seconds} seconds"
                )
                # Timeout is expected for continuous mode, not an error
                if mode == "continuous":
                    print(
                        f"Continuous wrapper {wrapper_id} stopped after timeout (normal behavior)"
                    )
                else:
                    print(
                        f"Warning: 'once' mode wrapper {wrapper_id} timed out (may indicate an issue)"
                    )

        except (ImportError, AttributeError, FileNotFoundError) as e:
            print(f"Error executing wrapper: {str(e)}")
            raise

    async def generate_and_run_wrapper(
        self,
        indicator_metadata: IndicatorMetadata,
        source_config: DataSourceConfig,
        wrapper_id: int,
        mode: str = "once",
    ):
        """
        Complete workflow: generate, save and execute wrapper
        """
        print(
            f"Generating wrapper {wrapper_id} for indicator: {indicator_metadata.name}..."
        )

        # Generate wrapper code
        wrapper_code = await self.generate_wrapper(
            indicator_metadata,
            source_config,
            source_config.source_type,
            str(wrapper_id),
        )

        # Save to file
        wrapper_file = self.save_wrapper(wrapper_code, str(wrapper_id))

        # Execute wrapper
        print(f"Executing wrapper {wrapper_id} in {mode} mode...")
        await self.execute_wrapper(wrapper_file, wrapper_id, mode, timeout_seconds=60)


def create_wrapper_from_config(
    # Indicator metadata
    indicator_name: str,
    domain: str,
    subdomain: str,
    description: str,
    unit: str,
    source: str,
    scale: str,
    governance_indicator: bool,
    carrying_capacity: Optional[float],
    periodicity: str,
    # Source configuration
    source_type: str,
    location: str,
    # Generation parameters
    wrapper_id: int,
    gemini_api_key: str,
    mode: str = "once",
    auth_config: Optional[Dict[str, Any]] = None,
    model_name: str = "gemini-2.5-flash",
) -> None:
    """
    Utility function to create a wrapper from configuration parameters

    Args:
        indicator_name: Name of the indicator
        domain: Main domain (e.g., "Economy", "Environment")
        subdomain: Subdomain (e.g., "Fishing", "Climate")
        description: Description of the indicator
        unit: Unit of measurement (e.g., "%", "°C")
        source: Data source (e.g., "INE", "gov")
        scale: Scale of measurement (e.g., "Municipal", "National")
        governance_indicator: Whether this is a governance indicator
        carrying_capacity: Maximum capacity value (if applicable)
        periodicity: How often data is updated ("Annual", "Monthly", etc.)
        source_type: Type of source ("API", "CSV", "XLSX")
        location: URL for API or file path for CSV/XLSX
        wrapper_id: Unique identifier for the wrapper
        gemini_api_key: API key for Gemini
        mode: Execution mode ("once" or "continuous")
        auth_config: Authentication configuration for APIs (optional)
        model_name: Gemini model name to use (default: gemini-2.5-flash)
    """

    async def _create_wrapper():
        generator = WrapperGenerator(gemini_api_key, model_name=model_name)

        metadata = IndicatorMetadata(
            name=indicator_name,
            domain=domain,
            subdomain=subdomain,
            description=description,
            unit=unit,
            source=source,
            scale=scale,
            governance_indicator=governance_indicator,
            carrying_capacity=carrying_capacity,
            periodicity=periodicity,
        )

        source_config = DataSourceConfig(
            source_type=source_type, location=location, auth_config=auth_config
        )

        await generator.generate_and_run_wrapper(
            indicator_metadata=metadata,
            source_config=source_config,
            wrapper_id=wrapper_id,
            mode=mode,
        )

    asyncio.run(_create_wrapper())


if __name__ == "__main__":
    print("WrapperGenerator module loaded successfully!")
    print("Use integration_tests.py to run integration tests")
    print("Use test_wrapper_generator.py to run unit tests")
    print("Use example_usage.py to see usage examples")
