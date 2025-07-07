import pandas as pd
import models
import logging
import os

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

def load_csv_to_db(file_path: str, db_table, db: Session):
   
    try:
        # Dynamically extract column names from the SQLAlchemy model
        column_names = get_sqlalchemy_column_names(db_table)
        logging.info(f"Using dynamic column names: {column_names}")
        df = pd.read_csv(file_path, header=None, names=column_names)

        records = []
        for _, row in df.iterrows():
            try:
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
