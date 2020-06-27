import io

import pandas as pd
import numpy as np
import requests
from fastapi.testclient import TestClient

from src.main import make_app


def make_awkward_data(nrows=100, ncols=5):
    df = pd.DataFrame(
        {
            "const": [1] * nrows,
            "index": range(nrows),
            "float_with_nans": [np.nan] + [1] * (nrows - 2) + [np.nan],
            "text": (list("abbccc") * nrows)[:nrows],
            "text_with_nans": (["a", np.nan] * nrows)[:nrows],
        }
    ).set_index("index")
    for c in range(df.shape[1], ncols):
        df[f"rnd_{c}"] = np.random.randn(nrows)
    return df


def test_upload_and_download_returns_original_data_with_text_and_nans():
    df = make_awkward_data(nrows=5)
    app = make_app()
    client = TestClient(app)
    filename = "upload.csv"

    # this mimics how typical web sends csv files
    r = client.post(
        "/datasets/csv", files={"file": (filename, io.BytesIO(str.encode(df.to_csv())))}
    )
    assert r.status_code == 200, f"Expected 200. Got {r.json()}"
    r = client.get(f"datasets/csv?name={filename}")
    assert r.status_code == 200, f"Expected 200. Got {r.json()}"

    # mimic download text/csv
    reloaded_df = pd.read_csv(io.StringIO(str(r.content, "utf-8")))
    expected = df.reset_index()
    pd.testing.assert_frame_equal(reloaded_df, expected)



def test_e2e():
    app = make_app()
    client = TestClient(app)
    url = ""

    # upload csv
    r = client.post(url + "datasets/csv",
                      files={"file": ("e2e.csv", io.BytesIO(b't,number,text\r\n0,1,a\r\n1,2,b\r\n2,,c\r\n3,4,d\r\n'))})
    assert r.status_code == 200, r.json()

    # list all
    r = client.get(url + "datasets")
    assert "e2e.csv" in r.json(), r.json()

    # get slice
    r = client.get(url + "datasets/csv?name=e2e.csv&columns=t&columns=number&index_col=t&min_index=2&max_index=4")
    assert r.content == b't,number\r\n2,\r\n3,4.0\r\n'  # stores ints as floats

    # delete
    client.delete(url + "datasets?name=e2e.csv")
    assert "e2e.csv" not in client.get(url + "datasets")