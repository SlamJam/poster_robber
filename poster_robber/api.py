import requests
import datetime as dt
from typing import Generic, TypeVar, Iterable

from pydantic import BaseModel, Field
from pydantic.dataclasses import dataclass
from pydantic.tools import parse_obj_as


class PageInfo(BaseModel):
    count: int
    page: int
    per_page: int


DataT = TypeVar("DataT")


class Page(BaseModel, Generic[DataT]):
    data: DataT
    page: PageInfo


class Transaction(BaseModel):
    client_id: int
    id: int = Field(alias="transaction_id")
    closed_at: dt.datetime = Field(alias="date_close")


class ClientInfo(BaseModel):
    id: int = Field(alias="client_id")
    activated_at: dt.datetime = Field(alias="date_activale")


@dataclass
class ApiError(Exception):
    message: str
    code: int = Field(alias="error")

    def __str__(self) -> str:
        return self.__repr__()


def json_response(data: dict) -> dict:
    if "response" in data:
        return data["response"]
    elif "error" in data:
        raise parse_obj_as(ApiError, data)
    else:
        raise Exception("unexpected response")


class API:
    def __init__(self, token: str, *, base_url="https://joinposter.com/api/") -> None:
        self._token = token
        self._base_url = base_url

    def url_for(self, rpc_method: str):
        return self._base_url + rpc_method

    def get_json_request(self, rpc_method: str, **kwargs):
        params = self.params(**kwargs)
        resp = requests.get(self.url_for(rpc_method), params=params)
        resp.raise_for_status()
        return json_response(resp.json())

    def post_put_json_request(self, rpc_method: str, http_method: str, **kwargs):
        params = self.params(**kwargs)
        resp = requests.request(http_method, self.url_for(rpc_method), json=params)
        resp.raise_for_status()
        return json_response(resp.json())

    def post_put_request(self, rpc_method: str, http_method: str, **kwargs):
        params = self.params(**kwargs)
        resp = requests.request(http_method, self.url_for(rpc_method), data=params)
        resp.raise_for_status()
        return resp.text

    def params(self, **kwargs):
        kwargs["token"] = self._token
        return kwargs

    # API calls

    def application_get_info(self):
        return self.get_json_request(
            "application.getInfo",
        )

    def get_transactions_page(
        self, date_from: dt.date, date_to: dt.date, page=1, per_page=100
    ) -> Page[list[Transaction]]:
        data = self.get_json_request(
            "transactions.getTransactions",
            date_from=date_from.isoformat(),
            date_to=date_to.isoformat(),
            page=page,
            per_page=per_page,
        )

        return parse_obj_as(Page[list[Transaction]], data)

    def iter_transactions(
        self, date_from: dt.date, date_to: dt.date, from_page=1, per_page=100
    ) -> Iterable[Transaction]:
        count = per_page
        while per_page == count:
            data = self.get_transactions_page(
                date_from, date_to, page=from_page, per_page=per_page
            )

            yield from data.data

            per_page = data.page.per_page
            count = data.page.count
            from_page = data.page.page + 1

    def get_client(self, client_id: int):
        data = self.get_json_request(
            "clients.getClient",
            client_id=client_id,
        )

        assert len(data) == 1, "unexpected response"

        return parse_obj_as(ClientInfo, data[0])

    def get_clients(self) -> list[ClientInfo]:
        data = self.get_json_request(
            "clients.getClients",
        )

        return parse_obj_as(list[ClientInfo], data)
