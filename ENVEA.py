#  _____                            _
# |_   _|                          | |
#   | |  _ __ ___  _ __   ___  _ __| |_ ___
#   | | | '_ ` _ \| '_ \ / _ \| '__| __/ __|
#  _| |_| | | | | | |_) | (_) | |  | |_\__ \
# |_____|_| |_| |_| .__/ \___/|_|   \__|___/
#                 | |
#                 |_|

import datetime
import requests
import credentials
from collections.abc import Iterable
from APIDialog import APIDialog
from logging_config import logger


#   _____ _
#  / ____| |
# | |    | | __ _ ___ ___  ___  ___
# | |    | |/ _` / __/ __|/ _ \/ __|
# | |____| | (_| \__ \__ \  __/\__ \
#  \_____|_|\__,_|___/___/\___||___/

class ENVEA(APIDialog):
    """
    An object to interact with ENVEA's API

    :param maxData: the maximum amount of data (Column*Lines) you can extract at once
    :type maxData: int
    :param maxCols: the maximum amount of columns you can extract at once
    :type maxCols: int
    :param dt: the smallest time from one data point to another
    :type dt: datetime.timedelta
    :param tz: an optional parameter to implement timezone-aware datetimes
    :type tz: datetime.timezone
    """

    def __init__(self,
                 maxData: int = 10000,
                 maxCols: int = 500,
                 dt: datetime.timedelta = datetime.timedelta(0, 0, 0, 0, 15, 0, 0),
                 tz: datetime.timezone = datetime.timezone(datetime.timedelta(0, 0, 0, 0, 0, 1)),
                 **kwargs):

        super().__init__(entry="https://36294460.envea-dms.cloud/dms-api",
                         loginURI="authentification/login",
                         logoutURI="authentification/logout",
                         login_kwargs={"headers": {"Content-Type": "application/x-www-form-urlencoded"},
                                       "data": credentials.get_login_payload()},
                         **kwargs)

        self.maxData = maxData
        self.maxCols = maxCols
        self.dt = dt
        self.NOW = datetime.datetime.now(tz=tz)

    @staticmethod
    def parse_kwargs(kwargs
                     ) -> str:

        res = []
        order = ["from", "to", "measures", "sites", "groups", "dataTypes", "validOnly", "lastHours", "updatedSince"]
        for arg in order:
            if arg in kwargs.keys():
                if isinstance(kwargs[arg], Iterable):
                    value = ','.join(kwargs.pop(arg))
                elif isinstance(kwargs[arg], datetime.datetime):
                    value = kwargs.pop(arg).strftime("%Y-%m-%dT%H:%M:%SZ")
                else:
                    value = kwargs.pop(arg)
                res.append(f'{arg}={value}')

        return f'{kwargs.pop("ressource")}?' + '&'.join(res)

    def __batch_requests(self,
                         maxLines: int = None,
                         **kwargs
                         ) -> list[requests.Response]:

        if not maxLines:
            maxLines = self.maxData // 6

        # batch the requests to be in groups of 500 variables
        if "measures" in kwargs.keys():
            if len(kwargs["measures"]) > self.maxCols:
                kwargs1, kwargs2 = kwargs.copy(), kwargs.copy()
                kwargs1["measures"] = {kwargs["measures"].pop() for _ in range(self.maxCols)}
                kwargs2["measures"] = kwargs["measures"].copy()
                return self.__batch_requests(maxLines=self.maxData // len(kwargs1["measures"]), **kwargs1) + \
                       self.__batch_requests(maxLines=self.maxData // len(kwargs2["measures"]), **kwargs2)

        # split the timeframes to fit the maximum amount of data authorised in one batch
        if "from" in kwargs.keys() and "to" in kwargs.keys():
            if (kwargs["to"] - kwargs["from"]) / self.dt > maxLines:
                kwargs1, kwargs2 = kwargs.copy(), kwargs.copy()
                kwargs1["to"] = kwargs2["from"] = kwargs["from"] + maxLines * self.dt
                return self.__batch_requests(**kwargs1) + \
                       self.__batch_requests(**kwargs2)

        elif "lastHours" in kwargs.keys():

            # increase the lower limit of lastHours to 1h
            if kwargs["lastHours"] < 1:
                kwargs1 = kwargs.copy()
                kwargs1["lastHours"] = 1
                return self.__batch_requests(**kwargs1)

            # edit the request from using the `lastHour` kwarg to using the `from`, `updatedSince` kwargs if the number of hours is too big
            elif kwargs["lastHours"] > 168:
                kwargs1 = kwargs.copy()
                kwargs1.pop("lastHours")
                kwargs1["from"] = self.NOW - maxLines * datetime.timedelta(0, 0, 0, 0, 0, kwargs["lastHours"], 0)
                kwargs1["updatedSince"] = self.NOW
                return self.__batch_requests(**kwargs1)

            # if there are too much hours in one batch, transform the expression in a `from`,`to` batch for reduction
            elif datetime.timedelta(0, 0, 0, 0, 0, kwargs["lastHours"], 0) / self.dt > maxLines:
                kwargs1 = kwargs.copy()
                kwargs1.pop("lastHours")
                kwargs1["from"], kwargs1["to"] = self.NOW - datetime.timedelta(0, 0, 0, 0, 0, 168, 0), self.NOW
                return self.__batch_requests(**kwargs1)

        # if there is too much data in one batch, transform the expression in a `from`,`to` batch for reduction
        elif "updatedSince" in kwargs.keys():
            if (kwargs["updatedSince"] - self.NOW) / self.dt > maxLines:
                kwargs1, kwargs2 = kwargs.copy(), kwargs.copy()
                kwargs1["from"], kwargs1["to"] = kwargs["updatedSince"], kwargs["from"] + maxLines * self.dt
                kwargs2["from"], kwargs2["to"] = kwargs["from"] + maxLines * self.dt, self.NOW
                kwargs1.pop("updatedSince"), kwargs2.pop("updatedSince")
                return self.__batch_requests(**kwargs1) + \
                       self.__batch_requests(**kwargs2)

        ressource = self.parse_kwargs(kwargs)
        return [super()._retrieve(ressource, **kwargs)]

    def __form_request(self,
                       **kwargs
                       ) -> list[requests.Response]:

        S = set(kwargs.keys()).intersection({"from", "to", "lastHours", "updatedSince"})
        if S in [{"from", "to"}, {"lastHours"}, {"updatedSince"}]:
            return self.__batch_requests(**kwargs)
        else:
            raise ValueError("Wrong time arguments were passed. "
                             "Use (`from` & `to`), `lastHours` or `updatedSince` independently")

    def retrieve(self,
                 ressource: str,
                 **kwargs
                 ) -> list[requests.Response]:
        """
        This function retrieves a ressource from the server

        :param ressource: the ressource you want to retrieve
        :type ressource: str
        :return: A list of response objects
        """

        if ressource == 'restricted/v1/data':
            if response := self.__form_request(ressource=ressource, **kwargs):
                if len(response) > 1:
                    logger.info(f"successfully retrieved {ressource} in {len(response)} batches")
                else:
                    logger.info(f"successfully retrieved {ressource}")
            return response
        else:
            if response := super()._retrieve(ressource, **kwargs):
                logger.info(f"successfully retrieved {ressource}")
            return [response]


if __name__ == '__main__':
    raise NotImplementedError("Cannot invoke package as `__main__`, use `import`")
