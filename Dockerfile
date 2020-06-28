FROM python:3.7-buster as prod
WORKDIR /app/
COPY ./requirements.txt /app/
RUN pip install -r requirements.txt
COPY ./src /app
EXPOSE 5007
ENTRYPOINT ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "--bind=0.0.0.0:5007", "--workers=4", "--timeout=50000", "server:make_app()"]