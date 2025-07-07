from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from database import SessionLocal, engine, Base
from pydantic import BaseModel

import models
import os
import schemas, upload_csv

app = FastAPI()

def get_db():
    db = SessionLocal()
    try: 
        yield db
    finally: db.close()

class QueryRequest(BaseModel):
    filename: str
    params: dict = {}

@app.post("/queries/from-file")
def run_query(req: QueryRequest):
    try:
        filepath = os.path.join("sql", req.filename)
        if not os.path.exists(filepath):
            raise HTTPException(status_code=404, detail="SQL file not found")

        with open(filepath, "r") as f:
            sql = text(f.read())

        with engine.connect() as conn:
            result = conn.execute(sql, req.params).mappings()
            return [dict(row) for row in result]
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to run SQL file: {str(e)}")

@app.post("/upload/csv")
def upload_csv_to_db(postgre_db: Session = Depends(get_db)):
    try:
        BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # points to project root
        upload_csv.load_csv_to_db(os.path.join(BASE_DIR, "data_folder", "departments.csv"), db_table=models.Department, db=postgre_db)
        upload_csv.load_csv_to_db(os.path.join(BASE_DIR, "data_folder", "jobs.csv"), db_table=models.Job, db=postgre_db)
        upload_csv.load_csv_to_db(os.path.join(BASE_DIR, "data_folder", "hired_employees.csv"), db_table=models.Employee, db=postgre_db)
        
        return {"message": "Files Uploaded"}
    
    except FileNotFoundError as fnf:
        raise HTTPException(status_code=400, detail=str(fnf))

    except SQLAlchemyError as db_err:
        postgre_db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(db_err)}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
    
@app.get("/health/db")
def db_health_check(db: Session = Depends(get_db)):
    try:
        # Simple query to test connection
        db.execute(text('SELECT 1'))
        return {"status": "ok", "message": "Database connected"}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")