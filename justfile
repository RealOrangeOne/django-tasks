# Recipes
@default:
  just --list

test *ARGS:
    python -m manage check
    python -m manage makemigrations --dry-run --check --noinput
    python -m coverage run --source=django_tasks -m manage test --shuffle --noinput {{ ARGS }}
    python -m coverage report
    python -m coverage html

format:
    python -m ruff check django_tasks tests --fix
    python -m ruff format django_tasks tests

lint:
    python -m ruff check django_tasks tests
    python -m ruff format django_tasks tests --check
    python -m mypy django_tasks tests

start-dbs:
    docker-compose pull
    docker-compose up -d

test-sqlite *ARGS:
    python -m manage test --shuffle --noinput {{ ARGS }}

test-postgres *ARGS:
    DATABASE_URL=postgres://postgres:postgres@localhost:15432/postgres python -m manage test --shuffle --noinput {{ ARGS }}

test-mysql *ARGS:
    DATABASE_URL=mysql://root:django@127.0.0.1:13306/django python -m manage test --shuffle --noinput {{ ARGS }}

test-dbs *ARGS: start-dbs test-postgres test-mysql test-sqlite
