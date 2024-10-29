from data_framework.modules.config.core import config, Environment

debug_code = lambda: config().environment != Environment.PRODUCTION