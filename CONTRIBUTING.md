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

Then you can run the tests with the [just](https://just.systems/man/en/) command runner:

```sh
just test
```

If you don't have `just` installed, you can look in the `justfile` for the commands that are run.
