from pydantic import BaseModel

class DepartmentCreate(BaseModel):
    id: int
    department: str

class JobCreate(BaseModel):
    id: int
    job: str

class EmployeeCreate(BaseModel):
    id: int
    name: str
    department_id: int
    job_id: int
