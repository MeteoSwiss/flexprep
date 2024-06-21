from mch_python_commons.audit.logger import LoggingSettings
from mch_python_commons.config.base_settings import BaseServiceSettings
from pydantic import BaseModel


class AppSettings(BaseModel):
    app_name: str


class ServiceSettings(BaseServiceSettings):
    logging: LoggingSettings
    main: AppSettings
