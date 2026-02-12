import json
import re
import inspect
from typing import Any, Callable, Dict, List, Protocol

import requests


def tool(func: Callable[..., Dict[str, Any]]) -> Callable[..., Dict[str, Any]]:
    setattr(func, "__is_wrapper_tool__", True)
    return func


class ToolRuntime(Protocol):
    def get_tools(self) -> List[Callable[..., Any]]: ...

    def execute(self, function_name: str, args: Dict[str, Any]) -> Dict[str, Any]: ...


class HttpToolRuntime:
    def __init__(self, auth_config: Dict[str, Any], max_chars: int = 2500):
        self.auth_config = auth_config
        self.max_chars = max_chars

    def _build_request_headers(self) -> Dict[str, Any]:
        headers = dict(self.auth_config.get("headers", {}))
        if "api_key" in self.auth_config and "header_name" in self.auth_config:
            headers[self.auth_config["header_name"]] = self.auth_config["api_key"]
        return headers

    @tool
    def fetch_url(self, url: str) -> Dict[str, Any]:
        """Fetches a URL and returns truncated content for wrapper generation context."""
        if not isinstance(url, str) or not url.strip():
            return {"error": "Missing URL"}
        if not re.match(r"^https?://", url.strip(), re.IGNORECASE):
            return {"error": "Only http/https URLs are allowed"}

        headers = self._build_request_headers()
        try:
            response = requests.get(url.strip(), headers=headers, timeout=30)
            response.raise_for_status()
            truncated = False
            try:
                content = json.dumps(response.json(), indent=2)
            except json.JSONDecodeError:
                content = response.text

            if len(content) > self.max_chars:
                content = content[: self.max_chars] + "\n... (truncated)"
                truncated = True

            return {
                "url": url.strip(),
                "status": response.status_code,
                "content": content,
                "truncated": truncated,
            }
        except requests.exceptions.RequestException as e:
            return {"url": url.strip(), "error": str(e)}

    def _iter_tools(self) -> List[Callable[..., Any]]:
        tools: List[Callable[..., Any]] = []
        for _, method in inspect.getmembers(self, predicate=callable):
            if getattr(method, "__is_wrapper_tool__", False):
                tools.append(method)
        return tools

    def get_tools(self) -> List[Callable[..., Any]]:
        return self._iter_tools()

    def execute(self, function_name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        for method in self._iter_tools():
            if method.__name__ == function_name:
                try:
                    return method(**(args or {}))
                except TypeError as e:
                    return {
                        "error": f"Invalid arguments for {function_name}: {str(e)}",
                        "args": args,
                    }

        return {"error": f"Unsupported tool: {function_name}"}


def create_tool_runtime(
    auth_config: Dict[str, Any], max_chars: int = 2500
) -> ToolRuntime:
    return HttpToolRuntime(auth_config=auth_config, max_chars=max_chars)
