import io
from typing import List

import pandas as pd


class FSClient:
    def __init__(self, httpclient, base_url):
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
        return self._get("datasets").json()

    def delete(self, name: str):
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
        self, name: str, columns=None, index_col=None, min_index=None, max_index=None
    ):
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

    def _get(self, query):
        r = self.c.get(self.url + query)
        _assert_status_code_200(r)
        return r


def _assert_status_code_200(r) -> None:
    assert r.status_code == 200, r.json()
