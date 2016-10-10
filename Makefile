install: clean
	python setup.py install

clean:
	rm -rf clickhouse_cli.egg-info build dist
