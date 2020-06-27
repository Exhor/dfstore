import io
import os
import requests

# build
# assert not os.system("docker build -t fs .", )

# run
# assert not os.system("docker run --rm -d -p 5007:5007 --name fsserver fs")

url = "http://localhost:5007/"

# upload csv
r = requests.post(url + "datasets/csv", files={"file": ("e2e.csv", io.BytesIO(b',x\r\n0,1\r\n1,2\r\n2,3\r\n3,4\r\n'))})
assert r.status_code == 200, r.json()

# list all
r = requests.get(url + "datasets")
assert "e2e.csv" in r.json(), r.json()

# get slice
r = requests.get(url + "datasets/csv?name=e2e.csv&colums=t&index_col=t&min_index=2&max_index=3")
assert str(r.content) == ""

# delete
requests.delete(url + "datasets?name=e2e.csv")
assert "e2e.csv" not in requests.get(url + "datasets")