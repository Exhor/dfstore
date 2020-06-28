import os

os.system("isort -rc -y -w 88")
os.system("black .")
os.system(
    "python -m mypy --strict --ignore-missing-imports --allow-untyped-decorators --follow-imports skip src"
)
