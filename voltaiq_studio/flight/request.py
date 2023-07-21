"""Module for the low level requests to Flight Server"""
from __future__ import annotations
import typing as t
import os
import json

import pyarrow.flight as fl
import voltaiq_studio.global_config as GLOBAL_CONFIG

if t.TYPE_CHECKING:
    from .query import FlightQuery


class ClientAuthMW(fl.ClientMiddleware):
    """Auth Middleware to set headers with auth token"""

    def __init__(self, token: str) -> None:
        super().__init__()
        self.__token: str = token

    def sending_headers(self, *args, **kwargs) -> dict:
        """Get Headers for request"""
        return {"authorization": f"Bearer {self.__token}"}


class ClientAuthMWFactory(fl.ClientMiddlewareFactory):
    """Middleware Factory to create ClientAuthMW with the studio token"""

    def __init__(self) -> None:
        """Initialize the factory, ensuring environment is setup"""
        super().__init__()
        token = os.environ.get("STUDIO_TOKEN", None)
        if token is None:
            raise ValueError(
                "No Token Set, please set environment variable of `STUDIO_TOKEN` with your token"
            )
        self.__token: str = token

    def start_call(self, *args, **kwargs) -> ClientAuthMW:
        """Get the ClientAuthMW for this call"""
        return ClientAuthMW(self.__token)


def get_client() -> fl.FlightClient:
    """Obtain a client at the provided location"""
    location = __get_root_location()
    generic_options = [
        ("GRPC_ARG_MIN_RECONNECT_BACKOFF_MS", GLOBAL_CONFIG.FLIGHT_RETRY_MIN),
        ("GRPC_ARG_MAX_RECONNECT_BACKOFF_MS", GLOBAL_CONFIG.FLIGHT_RETRY_MAX)
        ]
    return fl.FlightClient(location=location, middleware=[ClientAuthMWFactory()], generic_options=generic_options)


def __get_root_location():
    location = os.environ.get("FLIGHT_SERVER_LOCATION", None)
    if location is None:
        raise ValueError(
            "No flight location set, please set environment variable of `FLIGHT_SERVER_LOCATION` with url"  # pylint: disable=line-too-long
        )
    return location


def fetch_flight(query: FlightQuery) -> fl.FlightStreamReader:
    """Get a Flight Stream Reader for the provided time series query

    Parameters
    ----------
    query : FlightQuery
        Query to obtain time series data for

    Returns
    -------
    fl.FlightStreamReader
        Reader with Desired Time Series Data
    """
    command = query.to_query()
    description = fl.FlightDescriptor.for_command(
        json.dumps(command) if isinstance(command, dict) else command
    )

    root_client = get_client()

    flight_info = root_client.get_flight_info(description)
    # TODO refactor to support parallelization, not currently implemented on the server and
    # thus not currently needed
    return root_client.do_get(flight_info.endpoints[0].ticket)
