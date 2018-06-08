from setuptools import setup
from clickhouse_cli import __version__

setup(
    name='clickhouse-cli',
    version=__version__,
    description='A third-party client for the Clickhouse DBMS server.',
    long_description='',
    keywords='clickhouse',
    url='https://github.com/hatarist/clickhouse-cli',
    author='Igor Hatarist',
    author_email='igor@hatari.st',
    license='MIT',
    package_data={'clickhouse_cli': ['clickhouse-cli.rc.sample']},
    packages=[
        'clickhouse_cli',
        'clickhouse_cli.clickhouse',
        'clickhouse_cli.ui',
        'clickhouse_cli.ui.parseutils',
    ],
    install_requires=[
        'click>=6.6',
        'prompt-toolkit>=1.0.8,<2.0',
        'pygments>=2.1.3',
        'requests>=2.11.1',
        'sqlparse>=0.2.2',
    ],
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'clickhouse-cli = clickhouse_cli.cli:run_cli'
        ]
    }
)
