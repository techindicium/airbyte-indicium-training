#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
from requests.auth import AuthBase


import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator, HttpAuthenticator

"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.json file.
"""

URL_BASE = "https://rickandmortyapi.com/api/"

# Basic full refresh stream
class RickymortyHttpStream(HttpStream, ABC):
    # TODO: Fill in the url base. Required.
    url_base = URL_BASE

    def __init__(self, authenticator: Union[AuthBase, HttpAuthenticator] = None, config=None):
        super().__init__(authenticator)

        self.config = config


    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        TODO: Override this method to define a pagination strategy. If you will not be using pagination, no action is required - just return None.

        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        if response.json().get('info').get('next'):
            next_page_num = response.json().get('info').get('next').split('=')[-1]
            next_page = {'page': next_page_num}
        else: 
            next_page = None
        
        return next_page

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        if next_page_token is not None:
            return {'page': next_page_token.get('page')}
        else:
            # First Execution
            return {'page': self.config.get('start_page')}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        for result in response.json().get("results"):
            yield result


class Characters(RickymortyHttpStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "id"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "character"


# Source
class SourceRickymortyHttp(AbstractSource):

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            response = requests.get(URL_BASE)
            status_code = response.status_code

            if status_code != 200:
                # Permission errors starts with 3, e.g. 301
                if str(status_code).startswith("3"):
                    return (False, f"Permission error, status: {status_code}")

                return (False, f"HTTP error, status: {status_code}")

            return True, None
        except Exception as e:
            return self._fail_check_with_message(f"Unknown error, status: {str(e)}")

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        return [Characters(authenticator=None, config=config)]
