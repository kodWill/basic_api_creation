import pandas as pd
import models
import logging
import os

from typing import Optional
from sqlalchemy.orm import DeclarativeMeta
from sqlalchemy import Integer, Float, String, DateTime
from sqlalchemy.orm import Session

# Ensure the logs directory exists
LOG_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

# Configure logging to file inside app/logs/upload_logs.log
LOG_FILE_PATH = os.path.join(LOG_DIR, 'upload_logs.log')

logging.basicConfig(
    filename=LOG_FILE_PATH,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

def get_sqlalchemy_column_names(db_table):
    """Return list of column names from a SQLAlchemy model (excluding relationships)."""
    return [col.name for col in db_table.__table__.columns]

def coerce_value(value, col_type):
    if pd.isna(value) or str(value).strip() == "":
        return None

    try:
        if isinstance(col_type, Integer):
            return int(float(value))  # safely handle "34.0"
        elif isinstance(col_type, Float):
            return float(value)
        elif isinstance(col_type, DateTime):
            parsed = pd.to_datetime(value, errors="coerce")
            return parsed.to_pydatetime() if not pd.isna(parsed) else None
        elif isinstance(col_type, String):
            return str(value).strip()
    except Exception:
        return None

    return value  # fallback (untyped)

def safe_row(row: pd.Series, model: DeclarativeMeta) -> Optional[dict]:
    result = {}
    for col in model.__table__.columns:
        col_name = col.name
        raw_value = row.get(col_name)

        value = coerce_value(raw_value, col.type)

        if value is None and not col.nullable and not col.primary_key:
            # Required but missing or malformed
            return None

        result[col_name] = value
    return result

def load_csv_to_db(file_path: str, db_table, db: Session):
   
    try:
        # Dynamically extract column names from the SQLAlchemy model
        column_names = get_sqlalchemy_column_names(db_table)
        logging.info(f"Using dynamic column names: {column_names}")
        df = pd.read_csv(file_path, header=None, names=column_names)

        records = []
        for _, row in df.iterrows():
            try:
                record = safe_row(row, db_table)  # could be Job, Department, etc.
                if record:
                    logging.info(f"Inserting: {record}")
                    db_obj = db_table(**row.to_dict())  # Directly create ORM object
                    records.append(db_obj)
            except Exception as e:
                logging.error(f"Error creating DB object from row {row.to_dict()}: {e}")

        logging.info(f"Valid records loaded from {file_path}: {len(records)}")
        db.bulk_save_objects(records)
        db.commit()

    except Exception as e:
        logging.error(f"Failed to process {file_path}: {e}")
        raise
