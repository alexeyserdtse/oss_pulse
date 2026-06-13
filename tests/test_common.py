import json
import re
from pathlib import Path

import pytest
from pydantic import ValidationError

from github_ingest.common import EntitySpec, load_entities, load_repos
from github_ingest.config import Settings

_COMMON_DIR = Path(__file__).parent.parent / "common"
_COMMON_INGEST_DIR = _COMMON_DIR / "ingest"


def test_load_entities_valid(tmp_path: Path) -> None:
    data = {
        "entities": [
            {
                "name": "repo",
                "table_name": "bronze.repos",
                "endpoint": "/repos/{owner}/{name}",
                "key_field": "id",
                "single_object": True,
                "supports_since": False,
                "paginate": False,
                "discriminator": "default",
            }
        ]
    }
    f = tmp_path / "entities.json"
    f.write_text(json.dumps(data))
    specs = load_entities(f)
    assert len(specs) == 1
    assert specs[0].name == "repo"
    assert specs[0].single_object is True
    assert specs[0].discriminator == "default"


def test_load_entities_missing_field_raises(tmp_path: Path) -> None:
    data = {
        "entities": [
            {
                "name": "repo",
                "table_name": "bronze.repos",
                "endpoint": "/repos/{owner}/{name}",
                "key_field": "id",
                "single_object": True,
            }
        ]
    }
    f = tmp_path / "entities.json"
    f.write_text(json.dumps(data))
    with pytest.raises(ValidationError):
        load_entities(f)


def test_load_entities_bad_discriminator_raises(tmp_path: Path) -> None:
    data = {
        "entities": [
            {
                "name": "repo",
                "table_name": "bronze.repos",
                "endpoint": "/repos/{owner}/{name}",
                "key_field": "id",
                "single_object": True,
                "supports_since": False,
                "paginate": False,
                "discriminator": "totally_unknown",
            }
        ]
    }
    f = tmp_path / "entities.json"
    f.write_text(json.dumps(data))
    with pytest.raises(ValidationError):
        load_entities(f)


def test_load_repos_round_trip(tmp_path: Path) -> None:
    repos = ["duckdb/duckdb", "pola-rs/polars", "dbt-labs/dbt-core"]
    f = tmp_path / "repos.json"
    f.write_text(json.dumps({"repos": repos}))
    result = load_repos(f)
    assert result == repos


def test_load_entities_from_common_ingest_dir_all_four() -> None:
    specs = load_entities(_COMMON_INGEST_DIR / "entities.json")
    names = {s.name for s in specs}
    assert names == {"repo", "issue", "commit", "contributor"}


def test_load_repos_from_common_ingest_dir() -> None:
    repos = load_repos(_COMMON_INGEST_DIR / "repos.json")
    assert "duckdb/duckdb" in repos
    assert len(repos) >= 1


def test_settings_json_provides_default() -> None:
    settings = Settings(github_token=None)
    assert settings.github_base_url == "https://api.github.com"
    assert settings.page_size == 100
    assert settings.max_retries == 3
    assert settings.request_timeout == 30


def test_settings_constructor_arg_overrides_json() -> None:
    settings = Settings(github_token=None, page_size=50, max_retries=1)
    assert settings.page_size == 50
    assert settings.max_retries == 1


def test_settings_json_no_token_shaped_key() -> None:
    data = json.loads((_COMMON_DIR / "settings.json").read_text())
    secret_pattern = re.compile(r"(token|secret|password|credential|key|auth)", re.IGNORECASE)
    for k in data:
        assert not secret_pattern.search(k), f"secret-shaped key found in settings.json: {k!r}"


def test_entity_spec_issue_or_pr_discriminator() -> None:
    spec = EntitySpec(
        name="issue",
        table_name="bronze.issues",
        endpoint="/repos/{owner}/{name}/issues",
        key_field="id",
        single_object=False,
        supports_since=True,
        paginate=True,
        discriminator="issue_or_pr",
    )
    assert spec.discriminator == "issue_or_pr"
