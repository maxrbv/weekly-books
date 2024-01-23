import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry


MAX_RETRY_FOR_SESSION = 3
BACK_OFF_FACTOR = 0.3
ERROR_CODES = (404, 500, 501, 502, 503)
PROXY_STATE = 0


def get_session(
        retries: int = MAX_RETRY_FOR_SESSION,
        back_off_factor: int = BACK_OFF_FACTOR,
        status_force_list: list = ERROR_CODES
):
    session = requests.Session()
    retry = Retry(total=retries, read=retries, connect=retries,
                  backoff_factor=back_off_factor,
                  status_forcelist=status_force_list,
                  allowed_methods=frozenset(['GET', 'POST']))
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    return session