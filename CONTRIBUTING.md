# Contributing

Found a bug? Want to fix an open issue? Got an idea for an improvement? Please contribute!

**All** contributions are welcome, from absolutely anyone. Just open a PR, Issue or Discussion (as relevant) - no need to ask beforehand. If you're going to work on an issue, it's a good idea to say so on the issue, to make sure work isn't duplicated.

## Development set up

Fork, then clone the repo:

```sh
git clone git@github.com:your-username/django-tasks.git
```

Set up a venv:

```sh
python -m venv .venv
source .venv/bin/activate
python -m pip install -e --group dev
```

> [!TIP]
> To include support for a specific database, you can stack group flags (e.g. `--group dev --group postgres`). This is optional.

Then you can run the tests with the [just](https://just.systems/man/en/) command runner:

```sh
just test
```

If you don't have `just` installed, you can look in the `justfile` for the commands that are run.

To help with testing on different databases, there's a `docker-compose.yml` file to run PostgreSQL and MySQL in Docker, as well as some additional `just` commands for testing:

```sh
just start-dbs
just test-postgres
just test-mysql
just test-sqlite

# To run all of the above:
just test-dbs
```

Due to database worker process' tests, tests cannot run using an in-memory database, which means tests run quite slow locally. If you're not modifying the worker, and want you tests run run quicker, run:

```sh
just test-fast
```
