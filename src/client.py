import pandas as pd
from typing import List
import io

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

    def _get(self, query):
        r = self.c.get(self.url + query)
        assert r.status_code == 200, r.json()
        return r

    def all_datasets(self) -> List[str]:
        """ :returns: list of all datasets names """
        return self._get("datasets").json()

    def delete(self, name: str):
        return self.c.delete(self.url + f"datasets?name={name}")

    def upload_dataframe(self, df: pd.DataFrame, name: str):
        """ Upload a pandas dataframe and save with given name """
        return self.c.post(
            self.url + "/datasets/csv", files={"file": (name, io.BytesIO(str.encode(df.to_csv())))}
        )

    def get(self, name: str, columns = None, index_col = None, min_index = None, max_index = None):
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
