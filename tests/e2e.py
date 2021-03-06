from time import sleep

import numpy as np
import pandas as pd

from src.client import DFStoreClient


def e2e(httpclient, url=""):
    """ Runs e2e tests against a httpclient.
    `httpclient` can be `requests` or fastapi's TestClient()
    """
    c = DFStoreClient(httpclient=httpclient, base_url=url)

    # wait 5 seconds for ready
    for attempt in range(20):
        try:
            c.all_datasets()
        except:
            sleep(0.25)

    # delete all
    for dataset in c.all_datasets():
        c.delete(dataset)

    # check no datasets stored
    assert c.all_datasets() == []

    # upload csv
    df = pd.DataFrame(
        {"t": [0, 1, 2, 3], "number": [1, 2, np.nan, 4], "text": ["a", "b", "c", "d"]}
    )
    name = "e2e.csv"
    c.upload_dataframe(name=name, df=df)

    # list all
    assert name in c.all_datasets()

    # get slice
    cols = ["t", "number"]
    sliced_df = c.get(name=name, columns=cols, index_col="t", min_index=2, max_index=4)
    pd.testing.assert_frame_equal(sliced_df, df[cols].iloc[2:4].reset_index(drop=True))

    # delete
    c.delete(name)
    assert name not in c.all_datasets()
