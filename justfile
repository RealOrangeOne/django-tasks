export PATH := "./env/bin:" + env_var('PATH')

# Recipes
@default:
  just --list

test:
    python3 manage.py test

format:
    ruff check django_core_tasks tests --fix
    ruff format django_core_tasks tests
