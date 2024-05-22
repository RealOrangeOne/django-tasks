# Recipes
@default:
  just --list

test *ARGS:
    coverage run --source=django_tasks manage.py test {{ ARGS }}
    coverage report
    coverage html

format:
    ruff check django_tasks tests --fix
    ruff format django_tasks tests

lint:
    ruff check django_tasks tests
    ruff format django_tasks tests --check
    mypy django_tasks tests
