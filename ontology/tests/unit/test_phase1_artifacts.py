from __future__ import annotations

import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_cdk_environment_file_has_required_envs() -> None:
    env_file = PROJECT_ROOT / "cdk" / "environments.json"
    data = json.loads(env_file.read_text(encoding="utf-8"))

    for env_name in ("dev", "stg", "prod"):
        assert env_name in data
        for key in ("account", "region", "tenantId", "stackPrefix"):
            assert key in data[env_name]


def test_required_migration_files_exist() -> None:
    migration_dir = PROJECT_ROOT / "db" / "migrations"
    expected = [
        "001_create_schema.sql",
        "002_create_entity_master.sql",
        "003_create_entity_aliases.sql",
        "004_create_entity_roles.sql",
        "005_create_entity_policies.sql",
        "006_create_entity_audit_log.sql",
        "007_create_functions.sql",
        "008_create_roles.sql",
    ]

    missing = [name for name in expected if not (migration_dir / name).exists()]
    assert not missing, f"Missing migration files: {missing}"


def test_deploy_scripts_exist() -> None:
    scripts_dir = PROJECT_ROOT / "scripts"
    assert (scripts_dir / "deploy_env.ps1").exists()
    assert (scripts_dir / "seed_ssm_parameters.ps1").exists()
