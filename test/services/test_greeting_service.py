# First-party
from flexprep import CONFIG
from flexprep.domain.greeting import Greeting
from flexprep.services import greeting_service


def test_greeting_service():
    # given
    name = "World"

    # when
    result = greeting_service.get_greeting(name)

    expected = Greeting(message=f"Hello, {name} from {CONFIG.main.app_name}!")

    # then
    assert result == expected
