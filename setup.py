from setuptools import setup
from clickhouse_cli import __version__

setup(
    version=__version__,
    long_description='',
    package_data={'clickhouse_cli': ['clickhouse-cli.rc.sample']},
    packages=[
        'clickhouse_cli',
        'clickhouse_cli.clickhouse',
        'clickhouse_cli.ui',
        'clickhouse_cli.ui.parseutils',
    ],
    zip_safe=False,
)
