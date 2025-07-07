from fastapi import FastAPI, Depends, HTTPException, Path, Body
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from database import SessionLocal, engine, Base
from pydantic import BaseModel
from models import Employee, Job, Department
from schemas import EmployeeCreate, JobCreate, DepartmentCreate  # Pydantic models
from typing import List

import models
import os
import upload_csv



RESOURCE_MODELS = {
    "employees": (Employee, EmployeeCreate),
    "jobs": (Job, JobCreate),
    "departments": (Department, DepartmentCreate)
}

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
    

@app.post("/{resource}/batch")
def batch_insert(
    resource: str = Path(..., description="Resource to insert into"),
    data: List[dict] = Body(...)
):
    if resource not in RESOURCE_MODELS:
        raise HTTPException(status_code=404, detail=f"Resource '{resource}' not supported.")

    model_cls, schema_cls = RESOURCE_MODELS[resource]

    if not (1 <= len(data) <= 1000):
        raise HTTPException(status_code=400, detail="Batch size must be between 1 and 1000.")

    # Validate each record against the corresponding Pydantic model
    try:
        validated = [schema_cls(**row).dict() for row in data]
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Validation error: {e}")

    # Insert into the database
    session = next(get_db())
    try:
        session.bulk_insert_mappings(model_cls, validated)
        session.commit()
        return {"message": f"Inserted {len(validated)} rows into '{resource}'"}
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=f"Insert failed: {e}")
    
@app.get("/health/db")
def db_health_check(db: Session = Depends(get_db)):
    try:
        # Simple query to test connection
        db.execute(text('SELECT 1'))
        return {"status": "ok", "message": "Database connected"}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")