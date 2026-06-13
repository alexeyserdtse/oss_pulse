import json
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel


class EntitySpec(BaseModel):
    name: str
    table_name: str
    endpoint: str
    key_field: str
    single_object: bool
    supports_since: bool
    since_field: str | None = None
    paginate: bool
    discriminator: Literal["default", "issue_or_pr"] = "default"

    def extract_since_value(self, raw: dict[str, Any]) -> str | None:
        if self.since_field is None:
            return None
        parts = self.since_field.split(".")
        node: Any = raw
        for part in parts:
            if not isinstance(node, dict):
                return None
            node = node.get(part)
            if node is None:
                return None
        return str(node) if node is not None else None


def load_entities(path: Path) -> list[EntitySpec]:
    raw = json.loads(path.read_text())
    return [EntitySpec.model_validate(item) for item in raw["entities"]]


def load_repos(path: Path) -> list[str]:
    raw = json.loads(path.read_text())
    return list(raw["repos"])
