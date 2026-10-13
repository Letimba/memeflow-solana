"""GitHub-backed data lake via the github_app MCP connector.

All writes are single-commit push_files calls, all reads go through
get_file_contents. Credentials are resolved by the Mistral platform at
runtime (on behalf of the triggering user) — no tokens in this repo.
"""
from __future__ import annotations

import json
from typing import Any

from mistralai.workflows.plugins.mistralai.connectors import connector

github_connector = connector("github_app", run_as="auto")

NL = chr(10)


def _extract_text(response: Any) -> str | None:
    """Extract file text from an MCP tool response across known shapes."""
    if response is None:
        return None
    if isinstance(response, str):
        return response
    if isinstance(response, list):
        parts = [
            item.get("text")
            for item in response
            if isinstance(item, dict) and isinstance(item.get("text"), str)
        ]
        return NL.join(parts) if parts else None
    if isinstance(response, dict):
        content = response.get("content")
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts = [
                item.get("text")
                for item in content
                if isinstance(item, dict) and isinstance(item.get("text"), str)
            ]
            if parts:
                return NL.join(parts)
        if isinstance(response.get("text"), str):
            return response["text"]
    return None


def _extract_entries(response: Any) -> list[dict[str, Any]]:
    if isinstance(response, list):
        return [item for item in response if isinstance(item, dict)]
    if isinstance(response, dict):
        for key in ("entries", "files", "items", "content"):
            value = response.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


class GitHubStore:
    """Minimal data-lake API on top of the github_app MCP connector."""

    def __init__(self, client: Any, owner: str, repo: str, branch: str = "main") -> None:
        self.client = client
        self.owner = owner
        self.repo = repo
        self.branch = branch

    async def _call(self, tool_name: str, arguments: dict[str, Any]) -> Any:
        return await self.client.call_tool(tool_name=tool_name, arguments=arguments)

    async def read_text(self, path: str) -> str | None:
        try:
            response = await self._call(
                "get_file_contents",
                {
                    "owner": self.owner,
                    "repo": self.repo,
                    "path": path,
                    "ref": "refs/heads/" + self.branch,
                },
            )
        except Exception:
            return None
        return _extract_text(response)

    async def read_json(self, path: str) -> Any:
        text = await self.read_text(path)
        if not text:
            return None
        try:
            return json.loads(text)
        except (json.JSONDecodeError, TypeError):
            return None

    async def read_jsonl(self, path: str) -> list[dict[str, Any]]:
        text = await self.read_text(path) or ""
        records: list[dict[str, Any]] = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return records

    async def list_files(self, path: str) -> list[str]:
        try:
            response = await self._call(
                "get_file_contents",
                {
                    "owner": self.owner,
                    "repo": self.repo,
                    "path": path,
                    "ref": "refs/heads/" + self.branch,
                    "fields": ["name", "type"],
                },
            )
        except Exception:
            return []
        names: list[str] = []
        for entry in _extract_entries(response):
            name = entry.get("name")
            if isinstance(name, str) and entry.get("type") == "file":
                names.append(name)
        return names

    async def push(self, files: list[tuple[str, str]], message: str) -> None:
        await self._call(
            "push_files",
            {
                "owner": self.owner,
                "repo": self.repo,
                "branch": self.branch,
                "message": message,
                "files": [
                    {"path": path, "content": content} for path, content in files
                ],
            },
        )
