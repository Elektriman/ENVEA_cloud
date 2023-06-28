#  _____                            _
# |_   _|                          | |
#   | |  _ __ ___  _ __   ___  _ __| |_ ___
#   | | | '_ ` _ \| '_ \ / _ \| '__| __/ __|
#  _| |_| | | | | | |_) | (_) | |  | |_\__ \
# |_____|_| |_| |_| .__/ \___/|_|   \__|___/
#                 | |
#                 |_|

import requests
import time
from logging_config import logger, log_errors


#  ______                _   _
# |  ____|              | | (_)
# | |__ _   _ _ __   ___| |_ _  ___  _ __  ___
# |  __| | | | '_ \ / __| __| |/ _ \| '_ \/ __|
# | |  | |_| | | | | (__| |_| | (_) | | | \__ \
# |_|   \__,_|_| |_|\___|\__|_|\___/|_| |_|___/

def ceil(x: float,
         n: int = 0
         ) -> float:
    """
    returns the entry value 'X' rounded UP to the Nth decimal

    :param x: the number you want to round up
    :type x: float
    :param n: The number of decimal places to round to, defaults to 0
    :type n: int (optional)
    :return: The ceiling of the number x, rounded to n decimal places.
    """
    return float((int(x * 10 ** n) + 1) / 10 ** n)


#   _____ _
#  / ____| |
# | |    | | __ _ ___ ___  ___  ___
# | |    | |/ _` / __/ __|/ _ \/ __|
# | |____| | (_| \__ \__ \  __/\__ \
#  \_____|_|\__,_|___/___/\___||___/

class APIDialog:
    """
    An object to connect to, fetch data from and disconnect from an API

    :param entry: the general URL used as entry point
    :type entry: str
    :param loginURI: the path to the login page
    :type loginURI: str
    :param logoutURI: the path to the logout page
    :type logoutURI: str
    :param login_kwargs: a dictionnary with your credentials
    :type login_kwargs: dict
    :param max_requests_per_seconds: the program will sleep to avoid exceeding this number of requests in one second
    :type max_requests_per_seconds: int
    """

    def __init__(self,
                 entry: str,
                 loginURI: str,
                 logoutURI: str,
                 login_kwargs: dict = None,
                 max_requests_per_seconds: int = 10):

        if login_kwargs is None:
            login_kwargs = dict()
        logger.debug(f"New APIDialog(entry:{entry})")
        self.entry = entry
        self.loginURI = loginURI
        self.logoutURI = logoutURI
        self.login_kwargs = login_kwargs
        self.cookie = None
        self.T = []
        self.max_requests_per_second = max_requests_per_seconds

    @log_errors
    def __request(self,
                  verb: str,
                  ressource: str,
                  **kwargs
                  ) -> requests.Response:
        """
        It sends a request to the API, and returns the response

        :param verb: the HTTP verb to use, e.g. GET, POST, PUT, DELETE, etc
        :type verb: str
        :param ressource: the ressource you want to access
        :type ressource: str
        :return: A response object
        """

        if len(kwargs) > 0:
            logger.debug(f"{verb} request sent at {self.entry}/{ressource} with kwargs {tuple(kwargs)}")
        else:
            logger.debug(f"{verb} request sent at {self.entry}/{ressource}")

        t = time.time()
        self.T.append(t)
        while self.T[-1] - self.T[0] > 1:
            self.T.pop(0)

        if len(self.T) > self.max_requests_per_second:
            time.sleep(ceil(abs(1 - (self.T[-1] - self.T[0])), 3))
            self.T[-1] = time.time()

        response = requests.request(verb, self.entry + "/" + ressource, **kwargs)

        match response.status_code:
            case 200:
                return response
            case _:
                raise ConnectionError(f'{response.status_code} {response.reason}')

    def __enter__(self):
        """
        This function is used to connect to the API using the credentials provided in the constructor
        :return: The object itself.
        """

        try:
            if response := self.__request(verb="POST", ressource=self.loginURI, **self.login_kwargs):
                self.cookie = response.cookies
                logger.info("successfully connected to API using credentials")
            else :
                raise ConnectionError("empty response from the server")
        except Exception as e:
            raise EnvironmentError('Failed to initialise APIDialog connection') from e
        else :
            return self

    def __exit__(self,
                 exc_type,
                 exc_val,
                 exc_tb
                 ):
        """
        A function that is used to close the connection to the API.

        :param exc_type: the exception type
        :param exc_val: the exception value
        :param exc_tb: traceback object
        """

        if self.__request("GET", self.logoutURI, cookies=self.cookie):
            del self.cookie
            logger.info("successfully disconnected from API")
        else:
            raise EnvironmentError('Failed to close APIDialog connection')

    def _retrieve(self,
                  ressource: str,
                  **kwargs
                  ) -> requests.Response:
        """
        This function retrieves a ressource from the server

        :param ressource: the ressource you want to retrieve
        :type ressource: str
        :return: A response object
        """

        if response := self.__request(verb="GET",
                                      ressource=ressource,
                                      cookies=self.cookie,
                                      **kwargs):
            return response


#  __  __       _
# |  \/  |     (_)
# | \  / | __ _ _ _ __
# | |\/| |/ _` | | '_ \
# | |  | | (_| | | | | |
# |_|  |_|\__,_|_|_| |_|

if __name__ == "__main__":
    raise NotImplementedError("Cannot invoke package as `__main__`, use `import`")
