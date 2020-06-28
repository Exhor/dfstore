import io
import os
import requests
import traceback

# Surrogate CI script. Build + local deploy + e2e

try:
    # build
    # assert not os.system("docker build -t fs .", )

    # deploy
    # assert not os.system("docker run --rm -d -p 5007:5007 --name fsserver fs")
    from tests.e2e import e2e

    url = "http://localhost:5007/"

    # e2e
    e2e(httpclient=requests, url=url)

except Exception:
    traceback.print_exc()

    # undeploy
    # os.system("docker kill fsserver")