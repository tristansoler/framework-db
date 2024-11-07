from data_framework.modules.config.core import config
from data_framework.modules.config.model.flows import Environment

debug_code = lambda: config().environment != Environment.PRODUCTION
