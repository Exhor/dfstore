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
    os.system("docker kill fsserver")
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

    # publish (requires manual docker login first)
    assert not os.system("docker tag fs tcorradi/df-store")
    os.system("docker push tcorradi/df-store")

except Exception:
    traceback.print_exc()

    # undeploy
    os.system("docker kill fsserver")
