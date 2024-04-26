export PATH := "./env/bin:" + env_var('PATH')

# Recipes
@default:
  just --list

test *ARGS:
    coverage run --source=django_core_tasks manage.py test {{ ARGS }}
    coverage report
    coverage html

format:
    ruff check django_core_tasks tests --fix
    ruff format django_core_tasks tests

lint:
    ruff check django_core_tasks tests
    ruff format django_core_tasks tests --check
    mypy django_core_tasks tests
