import io
from typing import Any, List, Optional

import pandas as pd
from requests import Response


class DFStoreClient:
    def __init__(self, httpclient: Any, base_url: str) -> None:
        """
        :param httpclient: either `requests` or `fastapi.TestClient`, or any httpclient
            capable of the same interfaces: get, put, etc.
        :param base_url: this string will be pre-pended to any requests, for example
            `http://myservers.blabla.com/`
        """
        self.c = httpclient
        self.url = base_url

    def all_datasets(self) -> List[str]:
        """ :returns: list of all datasets names """
        dataset_list: List[str] = self._get("datasets").json()
        return dataset_list

    def delete(self, name: str) -> None:
        r = self.c.delete(self.url + f"datasets?name={name}")
        _assert_status_code_200(r)

    def upload_dataframe(self, df: pd.DataFrame, name: str) -> None:
        """ Upload a pandas dataframe and save with given name """
        r = self.c.post(
            self.url + "datasets/csv",
            files={"file": (name, io.BytesIO(str.encode(df.to_csv())))},
        )
        _assert_status_code_200(r)

    def get(
        self,
        name: str,
        columns: Optional[List[str]] = None,
        index_col: Optional[str] = None,
        min_index: Optional[int] = None,
        max_index: Optional[int] = None,
    ) -> pd.DataFrame:
        """ Get a slice of a dataset as a pandas DataFrame """
        q = f"datasets/csv?name={name}"
        if columns:
            for col in columns:
                q += f"&columns={col}"
        if index_col:
            q += f"&index_col={index_col}"
        if min_index:
            q += f"&min_index={min_index}"
        if max_index:
            q += f"&max_index={max_index}"

        r = self._get(q)
        return pd.read_csv(io.StringIO(str(r.content, "utf-8")))

    def _get(self, query: str) -> Any:
        r = self.c.get(self.url + query)
        _assert_status_code_200(r)
        return r


def _assert_status_code_200(r: Response) -> None:
    assert r.status_code == 200, r.json()
