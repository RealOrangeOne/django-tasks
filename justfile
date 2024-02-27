export PATH := "./env/bin:" + env_var('PATH')

# Recipes
@default:
  just --list

test:
    coverage run --source=django_core_tasks manage.py test
    coverage report
    coverage html

format:
    ruff check django_core_tasks tests --fix
    ruff format django_core_tasks tests
