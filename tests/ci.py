import os
import traceback

import requests

from tests.e2e import e2e

# Surrogate CI script. Build + local deploy + e2e

try:
    # build
    assert not os.system("docker build -t fs .."), "BUILD: FAIL :("
    print("BUILD: OK :)")

    # deploy
    # remove previous running
    os.system("docker rm fsserver")
    os.system("docker wait fsserver")
    # run
    assert not os.system(
        "docker run -d --rm -p 5007:5007 --name fsserver fs"
    ), "DEPLOY: FAIL :("
    print("DEPLOY: OK :)")

    url = "http://localhost:5007/"

    # e2e
    e2e(httpclient=requests, url=url)
    print("E2E: OK!")

except Exception:
    traceback.print_exc()

    # undeploy
    os.system("docker kill fsserver")
