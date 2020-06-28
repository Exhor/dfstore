import io
import os
import pathlib
from typing import List, Optional

import feather
import pandas as pd
import uvicorn
from fastapi import FastAPI, File, Query, UploadFile
from fastapi.responses import StreamingResponse


def make_app() -> FastAPI:
    app = FastAPI()

    def _dataset_file(filename: str) -> pathlib.Path:
        root = os.getenv("FILESTORE_FOLDER", str(pathlib.Path.home() / "filestore"))
        return pathlib.Path(root) / filename

    @app.get("/")
    def get() -> str:
        return "Hi, Im FeatherStore :)"

    @app.post("/datasets/csv")
    async def upload_dataset_file(file: UploadFile = File(...)) -> None:
        print(f"Received file {file.filename}")
        df = pd.read_csv(file.file)
        dataset_file = _dataset_file(file.filename)
        dataset_file.parent.mkdir(parents=True, exist_ok=True)
        feather.write_dataframe(df=df, dest=str(dataset_file))

    @app.get("/datasets/csv")
    async def dataset_slice(
        name: str,
        columns: List[str] = Query(None),
        index_col: Optional[str] = None,
        min_index: Optional[int] = None,
        max_index: Optional[int] = None,
    ) -> StreamingResponse:
        """ Returns pandas.DataFrame.to_dict() for the given dataset
        """
        dataset_file = _dataset_file(name)
        df = feather.read_dataframe(dataset_file, columns=columns)
        if index_col:
            assert df[index_col].dtype.kind in "iuf", "Index col must be integer/float"
            df = df[df[index_col].between(min_index, max_index)]
        stream = io.StringIO()
        df.to_csv(stream, index=False)
        response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
        response.headers["Content-Disposition"] = f"attachment; filename={name}"
        return response

    @app.delete("/datasets")
    def delete_dataset(name: str) -> None:
        pathlib.Path(_dataset_file(name)).unlink()

    @app.get("/datasets")
    def get_all_datasets() -> List[str]:
        files = pathlib.Path(_dataset_file("")).glob("*")
        return [f.name for f in files]

    return app


if __name__ == "__main__":
    uvicorn.run(make_app(), host="0.0.0.0", port=5007)
