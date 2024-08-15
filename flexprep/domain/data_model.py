import typing
from dataclasses import asdict, dataclass


@dataclass
class IFSForecast:
    forecast_ref_time: str
    step: int
    key: str
    processed: bool

    def to_dict(self) -> dict[str, typing.Any]:
        return asdict(self)
