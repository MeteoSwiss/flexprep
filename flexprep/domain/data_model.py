import typing

class IFSForecast:
    def __init__(self, forecast_ref_time: str, step: int, location: str, processed: bool) -> None:
        self.forecast_ref_time = forecast_ref_time
        self.step = step
        self.location = location
        self.processed = processed

    def to_dict(self) -> dict[str, typing.Any]:
        return {
            "forecast_ref_time": self.forecast_ref_time,
            "step": self.step,
            "location": self.location,
            "processed": self.processed
        }