import os
import logging

from configparser import NoOptionError
from configparser import SafeConfigParser

from logs import get_logger

LOGGER = get_logger()


def get_config_path():
    try:
        path = os.environ["PROTON_CONFIG"]
    except KeyError:
        path = "./settings.conf"
    return path


class Settings(object):
    def __init__(self, path=None):
        if not path:
            self.path = get_config_path()
        else:
            self.path = path
        self.config = SafeConfigParser()
        if os.path.exists(self.path):
            self.config.read(self.path)
            self.first_start = False
        else:
            self.config.add_section('empty')
            if not os.path.exists(os.path.dirname(self.path)):
                os.mkdir(os.path.dirname(self.path))
            self.config.write(self.path)

    def set(self, section, parameter, value):
        if self.config.has_section(section=section):
            self.config.set(section, parameter, value)
        else:
            self.config.add_section(section)
            self.config.set(section, parameter, value)

    def remove(self, section, parameter):
        if self.config.has_option(section, parameter):
            self.config.remove_option(section, parameter)

    def commit(self):
        try:
            with open(self.path, "w") as file:
                self.config.write(file)
        except BaseException as e:
            LOGGER.log(level=logging.ERROR, msg='Failed to write config file: %s' % e.args)

    def get(self, section, parameter):
        if self.config.has_option(section, parameter):
            try:
                response = self.config.get(section, parameter)
            except NoOptionError:
                response = False
        else:
            response = None
        return response
