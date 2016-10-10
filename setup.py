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
    packages=[
        'clickhouse_cli',
        'clickhouse_cli.clickhouse',
        'clickhouse_cli.ui',
    ],
    install_requires=[
        'click==6.6',
        'prompt-toolkit==1.0.7',
        'pygments==2.1.3',
        'requests==2.11.1',
    ],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'clickhouse-cli = clickhouse_cli.cli:run'
        ]
    }
)
