from pydantic import BaseModel  # pylint: disable=no-name-in-module


class Greeting(BaseModel):
    message: str
