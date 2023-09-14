PYTHON=`which python`

dev:
	$(PYTHON) -m pip install --upgrade pip flit
	$(PYTHON) -m flit install --deps=develop

build: clean
	$(PYTHON) -m flit build

install: build
	$(PYTHON) -m flit install --deps=production

clean:
	$(PYTHON) setup.py clean
	find . -name '*.pyc' -delete
	find . -name '*~' -delete
	rm -rf clickhouse_cli.egg-info build dist

format:
	black clickhouse_cli

lint:
	flake8 clickhouse_cli

test:
	tox

register:
	$(PYTHON) setup.py register -r pypi

upload:
	$(PYTHON) setup.py sdist upload -r pypi
