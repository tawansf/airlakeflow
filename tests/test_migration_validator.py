"""Tests for migration_validator: only CREATE TABLE/VIEW/INDEX allowed in migrations."""

from airlakeflow.migration_validator import (
    validate_migration_content,
    validate_migration_file,
    validate_migrations_dir,
)

# --- validate_migration_content: allowed statements ---


def test_validate_allows_create_table():
    sql = "CREATE TABLE silver.example (id SERIAL PRIMARY KEY, name VARCHAR(255));"
    assert validate_migration_content(sql) == []


def test_validate_allows_create_table_if_not_exists():
    sql = "CREATE TABLE IF NOT EXISTS bronze.raw (id INT);"
    assert validate_migration_content(sql) == []


def test_validate_allows_create_view():
    sql = "CREATE VIEW gold.daily AS SELECT * FROM silver.example;"
    assert validate_migration_content(sql) == []


def test_validate_allows_create_or_replace_view():
    sql = "CREATE OR REPLACE VIEW silver.vw_example AS SELECT 1;"
    assert validate_migration_content(sql) == []


def test_validate_allows_create_index():
    sql = "CREATE INDEX idx_example_id ON silver.example(id);"
    assert validate_migration_content(sql) == []


def test_validate_allows_create_unique_index():
    sql = "CREATE UNIQUE INDEX idx_uniq ON silver.example(name);"
    assert validate_migration_content(sql) == []


def test_validate_allows_drop_table():
    sql = "DROP TABLE IF EXISTS silver.example CASCADE;"
    assert validate_migration_content(sql) == []


def test_validate_allows_drop_view():
    sql = "DROP VIEW IF EXISTS gold.daily;"
    assert validate_migration_content(sql) == []


def test_validate_allows_multiple_allowed_statements():
    sql = """
    CREATE TABLE silver.example (id SERIAL PRIMARY KEY);
    CREATE INDEX idx_ex ON silver.example(created_at);
    """
    assert validate_migration_content(sql) == []


def test_validate_ignores_comments():
    sql = """
    -- setup table
    CREATE TABLE silver.example (id INT);
    """
    assert validate_migration_content(sql) == []


# --- validate_migration_content: forbidden statements ---


def test_validate_forbids_create_schema():
    sql = "CREATE SCHEMA IF NOT EXISTS silver;"
    err = validate_migration_content(sql)
    assert len(err) == 1
    assert "CREATE SCHEMA" in err[0]
    assert "scripts/" in err[0]


def test_validate_forbids_create_database():
    sql = "CREATE DATABASE datawarehouse;"
    err = validate_migration_content(sql)
    assert len(err) == 1
    assert "CREATE DATABASE" in err[0]


def test_validate_forbids_drop_schema():
    sql = "DROP SCHEMA IF EXISTS silver CASCADE;"
    err = validate_migration_content(sql)
    assert len(err) == 1
    assert "DROP SCHEMA" in err[0]


def test_validate_forbids_grant():
    sql = "GRANT SELECT ON silver.example TO reader;"
    err = validate_migration_content(sql)
    assert len(err) == 1
    assert "GRANT" in err[0]


def test_validate_forbids_insert():
    sql = "INSERT INTO silver.example (id) VALUES (1);"
    err = validate_migration_content(sql)
    assert len(err) == 1
    assert "INSERT" in err[0]


def test_validate_forbids_update():
    sql = "UPDATE silver.example SET name = 'x' WHERE id = 1;"
    err = validate_migration_content(sql)
    assert len(err) == 1
    assert "UPDATE" in err[0]


def test_validate_forbids_delete():
    sql = "DELETE FROM silver.example WHERE id = 1;"
    err = validate_migration_content(sql)
    assert len(err) == 1
    assert "DELETE" in err[0]


def test_validate_forbids_create_function():
    sql = "CREATE FUNCTION silver.fn() RETURNS INT AS $$ SELECT 1 $$ LANGUAGE sql;"
    err = validate_migration_content(sql)
    assert len(err) == 1
    assert "CREATE FUNCTION" in err[0]


def test_validate_filename_in_error():
    sql = "CREATE SCHEMA bronze;"
    err = validate_migration_content(sql, "V001__setup_schema.sql")
    assert len(err) == 1
    assert "V001__setup_schema.sql" in err[0]


def test_validate_empty_content():
    assert validate_migration_content("") == []
    assert validate_migration_content("   \n  -- comment only\n  ") == []


# --- validate_migration_file ---


def test_validate_migration_file_reads_path(tmp_path):
    f = tmp_path / "V001__setup_silver_example.sql"
    f.write_text("CREATE TABLE silver.example (id SERIAL PRIMARY KEY);")
    assert validate_migration_file(f) == []


def test_validate_migration_file_reports_forbidden(tmp_path):
    f = tmp_path / "V001__setup_schemas.sql"
    f.write_text("CREATE SCHEMA IF NOT EXISTS silver;")
    err = validate_migration_file(f)
    assert len(err) == 1
    assert "V001__setup_schemas.sql" in err[0]


def test_validate_migration_file_nonexistent(tmp_path):
    f = tmp_path / "nonexistent.sql"
    err = validate_migration_file(f)
    assert len(err) == 1
    assert "error reading" in err[0].lower()


# --- validate_migrations_dir ---


def test_validate_migrations_dir_empty_dir(tmp_path):
    (tmp_path / "migrations").mkdir()
    assert validate_migrations_dir(tmp_path / "migrations") == []


def test_validate_migrations_dir_nonexistent(tmp_path):
    assert validate_migrations_dir(tmp_path / "nonexistent") == []


def test_validate_migrations_dir_valid_only(tmp_path):
    mig = tmp_path / "migrations"
    mig.mkdir()
    (mig / "V001__setup_silver_example.sql").write_text("CREATE TABLE silver.example (id SERIAL);")
    (mig / "V002__setup_gold_daily.sql").write_text("CREATE VIEW gold.daily AS SELECT 1;")
    assert validate_migrations_dir(mig) == []


def test_validate_migrations_dir_mixed_valid_and_invalid(tmp_path):
    mig = tmp_path / "migrations"
    mig.mkdir()
    (mig / "V001__setup_silver_example.sql").write_text("CREATE TABLE silver.example (id SERIAL);")
    (mig / "V002__setup_schemas.sql").write_text("CREATE SCHEMA IF NOT EXISTS bronze;")
    err = validate_migrations_dir(mig)
    assert len(err) >= 1
    assert any("V002" in e for e in err)
