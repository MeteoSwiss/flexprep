import typing
from dataclasses import dataclass, asdict

@dataclass
class IFSForecast:
    forecast_ref_time: str
    step: int
    location: str
    processed: bool

    def to_dict(self) -> dict[str, typing.Any]:
        return asdict(self)