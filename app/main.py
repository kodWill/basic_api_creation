from fastapi import FastAPI

app = FastAPI()


@app.post("/upload/csv")
def upload_csv():
    
    return {"message": "Files Uploaded"}
