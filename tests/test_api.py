import io

import pandas as pd
import numpy as np
from fastapi.testclient import TestClient

from main import make_app


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


def test_upload_and_download_returns_original_data():
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

    # test slice
    min_index = 1
    max_index = 3
    r = client.get(
        f"datasets/csv?name={filename}&columns=index&columns=text&index_col=index&min_index={min_index}&max_index={max_index}"
    )

    assert r.status_code == 200, f"Expected 200. Got {r.json()}"
    reloaded_df = pd.read_csv(io.StringIO(str(r.content, "utf-8")))
    assert reloaded_df.shape == (3, 2)
    assert reloaded_df["index"].tolist() == [1, 2, 3]
    assert reloaded_df["text"].tolist() == ["b", "b", "c"]

    # check it is listed
    assert filename in client.get("datasets").json()

    # test deletion
    client.delete(f"datasets?name={filename}")
    assert filename not in client.get("dataset").json()
