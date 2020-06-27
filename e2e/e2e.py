import os
import requests

# build
os.system("docker build -t fs .")

# run
os.system("docker run --rm -d --name fsserver fs")

# list all
requests.get("localhost:5007/datasets")