# Variables
PYTHON_VERSION := shell("python -c \"import sys;v=f'{sys.version_info.major}.{sys.version_info.minor}';sys.stdout.write(v)\"")
ENV_DIR := ".env_python" + PYTHON_VERSION
IN_ENV := ". " + ENV_DIR / "/bin/activate &&"
PIP_CMD := "python" + PYTHON_VERSION + " -m pip --disable-pip-version-check --no-input --retries 2"

# Recipes
@default:
  just --list

setup-env *args:
  @python{{PYTHON_VERSION}} -m venv {{ENV_DIR}} --upgrade-deps
  @{{IN_ENV}} {{PIP_CMD}} install -e .[{{trim_end_match("dev," + replace(trim(args), " ", ","), ",")}}]

test *ARGS:
    {{IN_ENV}} python -m manage check
    {{IN_ENV}} python -m manage makemigrations --dry-run --check --noinput
    {{IN_ENV}} python -m coverage run --source=django_tasks -m manage test --shuffle --noinput {{ ARGS }}
    {{IN_ENV}} python -m coverage report
    {{IN_ENV}} python -m coverage html

format:
    {{IN_ENV}} python -m ruff check django_tasks tests --fix
    {{IN_ENV}} python -m ruff format django_tasks tests

lint:
    {{IN_ENV}} python -m ruff check django_tasks tests
    {{IN_ENV}} python -m ruff format django_tasks tests --check
    {{IN_ENV}} python -m mypy django_tasks tests

mypy:
    {{IN_ENV}} python -m mypy django_tasks tests

start-dbs:
    docker-compose pull
    docker-compose up -d

test-sqlite *ARGS:
    {{IN_ENV}} python -m manage test --shuffle --noinput {{ ARGS }}

test-postgres *ARGS:
    DATABASE_URL=postgres://postgres:postgres@localhost:15432/postgres python -m manage test --shuffle --noinput {{ ARGS }}

test-mysql *ARGS:
    DATABASE_URL=mysql://root:django@127.0.0.1:13306/django python -m manage test --shuffle --noinput {{ ARGS }}

test-dbs *ARGS: start-dbs test-postgres test-mysql test-sqlite

git-clean: clean
  @git fsck
  @git reflog expire --expire=now --all
  @git repack -ad
  @git prune

clean:
  @git clean -dfX >> /dev/null 2>&1
  @rm -rf {{ENV_DIR}}
  @rm -rf .env*
  @rm -rf .venv*
