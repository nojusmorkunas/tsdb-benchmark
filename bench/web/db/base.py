from abc import ABC, abstractmethod
from typing import TypedDict


class QueryResult(TypedDict):
    columns: list
    rows: list
    total_rows: int
    time_ms: float


class DbAdapter(ABC):
    name: str

    @abstractmethod
    def query(self, sql_or_flux: str, max_rows: int = 500) -> QueryResult: ...

    @abstractmethod
    def ping(self) -> bool: ...

    @abstractmethod
    def row_count(self) -> int | None: ...

    def resolve_placeholders(self, query_text: str) -> str:
        return query_text
