import numpy as np
import pandas as pd
from fastapi.testclient import TestClient

from client import FSClient
from src.main import make_app
from tests.e2e import e2e


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


def test_upload_and_download_awkward_data_returns_original_data_with_text_and_nans():
    app = make_app()
    client = FSClient(httpclient=TestClient(app), base_url="")
    df = make_awkward_data(nrows=5)
    filename = "upload.csv"

    client.upload_dataframe(df=df, name=filename)
    reloaded_df = client.get(name=filename)

    expected = df.reset_index()
    pd.testing.assert_frame_equal(reloaded_df, expected)


def test_e2e():
    app = make_app()
    client = TestClient(app)
    url = ""
    e2e(httpclient=client, url=url)
