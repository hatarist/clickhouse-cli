import os
import shutil

from configparser import ConfigParser

from clickhouse_cli.ui.style import Echo


PACKAGE_ROOT = os.path.dirname(__file__)
DEFAULT_CONFIG = os.path.join(PACKAGE_ROOT, 'clickhouse-cli.rc.sample')
USER_CONFIG = '~/.clickhouse-cli.rc'


echo = Echo()


def read_config():
    config = ConfigParser()
    try:
        config.read_file(open(DEFAULT_CONFIG))
        config.read([os.path.expanduser(USER_CONFIG)])
    except (IOError, OSError) as e:
        echo.warning("You don't have permission to read '{0}'.".format(e.filename))
        return

    return config


def write_default_config(source, destination, overwrite=False):
    destination = os.path.expanduser(destination)
    if not overwrite and os.path.exists(destination):
        return

    shutil.copyfile(source, destination)
