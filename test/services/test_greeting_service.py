from pilotecmwf_pp_starter import CONFIG
from pilotecmwf_pp_starter.services import greeting_service
from pilotecmwf_pp_starter.domain.greeting import Greeting

def test_greeting_service():
    # given
    name = 'World'

    # when
    result = greeting_service.get_greeting(name)
    
    expected = Greeting(message=f'Hello, {name} from {CONFIG.main.app_name}!')

    # then
    assert result == expected