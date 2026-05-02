
import os
from typing import Optional
from fastapi import FastAPI, Header, HTTPException, Query
from pydantic import BaseModel, EmailStr
from .common import *

os.environ.setdefault("SERVICE_NAME", "patient-service")
app = FastAPI(title="Patient Service", version="1.0.0")
app.middleware("http")(add_correlation_and_logging)
conn = db()

class PatientIn(BaseModel):
    name: str
    email: EmailStr
    phone: str
    dob: str
    active: bool = True

class PatientPatch(BaseModel):
    name: Optional[str] = None
    email: Optional[EmailStr] = None
    phone: Optional[str] = None
    dob: Optional[str] = None
    active: Optional[bool] = None

@app.on_event("startup")
def startup():
    conn.execute('''CREATE TABLE IF NOT EXISTS patients(
        patient_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT NOT NULL,
        phone TEXT NOT NULL,
        dob TEXT NOT NULL,
        active INTEGER DEFAULT 1,
        created_at TEXT NOT NULL,
        version INTEGER DEFAULT 1
    )''')
    load_csv_once(conn, "patients", "data/hms_patients_indian(1).csv", ["patient_id","name","email","phone","dob","created_at"])
    conn.execute("UPDATE patients SET active=1 WHERE active IS NULL")
    conn.commit()

@app.get("/health")
def health(): return {"status":"UP", "service":"patient-service"}
@app.get("/metrics")
def metrics(): return metrics_response()

@app.get("/v1/patients")
def list_patients(q: Optional[str] = Query(None, description="Search by name or phone"), limit: int = 20, offset: int = 0):
    limit, offset = paginate(limit, offset)
    where, params = "WHERE 1=1", []
    if q:
        where += " AND (LOWER(name) LIKE ? OR phone LIKE ?)"
        params += [f"%{q.lower()}%", f"%{q}%"]
    rows = conn.execute(f"SELECT patient_id,name,email,phone,dob,active,created_at,version FROM patients {where} ORDER BY patient_id LIMIT ? OFFSET ?", params+[limit,offset]).fetchall()
    return {"items": rows_to_dicts(rows), "limit": limit, "offset": offset}

@app.get("/v1/patients/{patient_id}")
def get_patient(patient_id: int):
    row = conn.execute("SELECT * FROM patients WHERE patient_id=?", (patient_id,)).fetchone()
    if not row: raise HTTPException(404, "Patient not found")
    return row_to_dict(row)

@app.get("/v1/patients/{patient_id}/exists")
def patient_exists(patient_id: int):
    row = conn.execute("SELECT patient_id, active FROM patients WHERE patient_id=?", (patient_id,)).fetchone()
    return {"exists": bool(row), "active": bool(row and row["active"])}

@app.post("/v1/patients", status_code=201)
def create_patient(payload: PatientIn, x_role: Optional[str] = Header(None)):
    require_role(x_role, {"reception", "admin"})
    cur = conn.execute("INSERT INTO patients(name,email,phone,dob,active,created_at,version) VALUES(?,?,?,?,?,?,1)",
                       (payload.name, payload.email, payload.phone, payload.dob, int(payload.active), now_utc()))
    conn.commit()
    logger.info(f"Created patient name={payload.name} email={payload.email} phone={payload.phone}")
    return get_patient(cur.lastrowid)

@app.put("/v1/patients/{patient_id}")
def update_patient(patient_id: int, payload: PatientPatch, x_role: Optional[str] = Header(None)):
    require_role(x_role, {"reception", "admin"})
    current = get_patient(patient_id)
    data = payload.dict(exclude_unset=True)
    if not data: return current
    fields = ", ".join([f"{k}=?" for k in data.keys()]) + ", version=version+1"
    vals = [int(v) if isinstance(v, bool) else str(v) for v in data.values()] + [patient_id]
    conn.execute(f"UPDATE patients SET {fields} WHERE patient_id=?", vals)
    conn.commit()
    return get_patient(patient_id)

@app.delete("/v1/patients/{patient_id}", status_code=204)
def delete_patient(patient_id: int, x_role: Optional[str] = Header(None)):
    require_role(x_role, {"admin"})
    conn.execute("UPDATE patients SET active=0, version=version+1 WHERE patient_id=?", (patient_id,))
    conn.commit()
    return None
