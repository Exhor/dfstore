# DF-Store
Dockerised data storage via REST API

## Usage

### Server
To start a server which stores files in `datasets_folder`.
```bash
$ docker run -d -p 5007:5007 -v datasets_folder:/app/data/ tcorradi/df-store
```

NB: Omitting the volume mount means all data is lost when the container stops.

In a browser navigate to `http://localhost:5007/docs/` to see the OpenAPI spec

### Python Client 
Clone the repo and in python
```python
from src.client import DFStoreClient
import requests
import pandas as pd
client = DFStoreClient(requests, "http://localhost:5007")

client.all_datasets()  # yields []

# upload a dataframe
df = pd.DataFrame({"x": [1,2,3,4,5], "y":["a","b","c","d","e"]})
client.upload_dataframe(df, "my_dataset")

# see it listed
client.all_datasets()  # = ["my_dataset"]

# download the full dataframe
reloaded = client.get("my_dataset") 

# download partial dataframe, filter columns and rows
sliced = client.get("my_dataset", columns=["x"], index_col="x", min_index=2, max_index=4)

# erase from disk
client.delete("my_dataset")
```
