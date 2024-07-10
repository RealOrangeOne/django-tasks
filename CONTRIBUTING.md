## Contributing

Fork, then clone the repo:

```sh
git clone git@github.com:your-username/django-tasks.git
```

Set up a venv:

```sh
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
```

> [!TIP]
> Add an extra name for each database you want to develop with (e.g. `[dev,mysql]`,  `[dev,postgres]` or `[dev,mysql,postgres]`). This is optional.

Then you can run the tests with the [just](https://just.systems/man/en/) command runner:

```sh
just test
```

If you don't have `just` installed, you can look in the `justfile` for the commands that are run.

To help with testing on Docker, there's a `docker-compose.yml` file to run PostgreSQL and MySQL in Docker, as well as some additional `just` commands for testing:

```sh
just start-dbs
just test-postgres
just test-mysql
just test-sqlite

# To run all of the above:
just test-dbs
```
