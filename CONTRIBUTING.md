# Contributing

Found a bug? Want to fix an open issue? Got an idea for an improvement? Please contribute!

**All** contributions are welcome, from absolutely anyone. Just open a PR, Issue or Discussion (as relevant) - no need to ask beforehand. If you're going to work on an issue, it's a good idea to say so on the issue, to make sure work isn't duplicated.

## Development set up

Fork, then clone the repo:

```sh
git clone git@github.com:your-username/django-tasks.git
```

Set up a venv:
Use the [just](https://just.systems/man/en/) command runner to build the virtual environment:

```sh
just setup-env
```

> [!TIP]
> Add an extra name for each database you want to develop with (e.g. `mysql`,  `postgres` or even `mysql postgres`). This is optional:

```sh
just setup-env postgres mysql
```

Then you can run the tests:

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
