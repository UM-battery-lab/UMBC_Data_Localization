"""Module for session and connection related functionality"""
from __future__ import annotations
import typing as t
import os
from functools import partial
from urllib.parse import urljoin

from requests.sessions import Session


class SessionManager:
    """Provide Management of Session and Useful Intercepting to set base url and auth headers"""

    __session: t.Optional[Session] = None

    @classmethod
    def __get_base_url(cls) -> str:
        """Get the Base URL that Requests should route to"""
        url = os.environ.get("VOLTA_URL")
        assert url is not None, "VOLTA_URL Environment Variable is not set"
        return url

    @classmethod
    def __set_interceptor(cls, session: Session) -> None:
        """Set an interceptor to set Base URL for each request"""
        root_url = cls.__get_base_url()

        def new_func(f, method, url, *args, **kwargs):
            new_url = url if root_url in url else urljoin(root_url, url)
            return f(method, new_url, *args, **kwargs)

        session.request = partial(new_func, session.request)  # type: ignore

    @classmethod
    def __get_token(cls) -> str:
        """Obtain the Token to use for Auth"""
        token = os.environ.get("STUDIO_TOKEN")
        assert token is not None, "STUDIO_TOKEN Environment Variable is not set"
        return token

    @classmethod
    def __set_session(cls) -> None:
        """Create a Session and set the Authorization Header"""
        cls.__session = Session()
        cls.__session.headers.update({"Authorization": f"Bearer {cls.__get_token()}"})
        cls.__set_interceptor(cls.__session)

    @classmethod
    def get_session(cls) -> Session:
        """Obtain the Session to use for Requests"""
        if cls.__session is None:
            cls.__set_session()
        assert cls.__session is not None
        return cls.__session


def get_json(url: str) -> t.Union[t.Dict, t.List]:
    """Execute an HTTP Call to the provided URL and return Response data as JSON

    Parameters
    ----------
    url : str
        URL to Request

    Returns
    -------
    t.Union[t.Dict, t.List]
        JSON Deserialized Response
    """
    session = SessionManager.get_session()
    response = session.get(url)
    response.raise_for_status()
    return response.json()

def put_json(url: str, data: dict, json_dump=False) -> t.Union[t.Dict, t.List]:
    """Execute an HTTP Call to the provided URL and return Response data as JSON

    Parameters
    ----------
    url : str
        URL to Request
    data : dict
        data passing in for the API Request
    json_dump: boolean
        True if we need to dump data into json

    Returns
    -------
    t.Union[t.Dict, t.List]
        JSON Deserialized Response
    """
    session = SessionManager.get_session()
    response = session.put(url, json=data) if json_dump else session.put(url, data=data)
    response.raise_for_status()
    return response.json()

def post_json(url: str, data: dict, json_dump=False) -> t.Union[t.Dict, t.List]:
    """Execute an HTTP Call to the provided URL and return Response data as JSON

    Parameters
    ----------
    url : str
        URL to Request
    data : dict
        data passing in for the API Request
    json_dump: boolean
        True if we need to dump data into json

    Returns
    -------
    t.Union[t.Dict, t.List]
        JSON Deserialized Response
    """
    session = SessionManager.get_session()
    response = session.post(url, json=data) if json_dump else session.post(url, data=data)
    response.raise_for_status()
    return response.json()

def delete_json(url: str) -> t.Union[t.Dict, t.List]:
    """Execute an HTTP Call to the provided URL and return Response data as JSON

    Parameters
    ----------
    url : str
        URL to Request

    Returns
    -------
    t.Union[t.Dict, t.List]
        JSON Deserialized Response
    """
    session = SessionManager.get_session()
    response = session.delete(url)
    response.raise_for_status()
    return response