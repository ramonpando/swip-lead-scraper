import sqlite3
import json
import uuid
from datetime import datetime
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class JobDatabase:
    def __init__(self, db_path: str = "/app/jobs.db"):
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        """Inicializar base de datos"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    request_data TEXT,
                    results TEXT,
                    created_at TEXT,
                    updated_at TEXT,
                    estimated_time INTEGER
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("✅ Database initialized")
            
        except Exception as e:
            logger.error(f"❌ Database init error: {e}")
    
    def create_job(self, request_data: Dict) -> str:
        """Crear nuevo job"""
        try:
            job_id = str(uuid.uuid4())
            now = datetime.now().isoformat()
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO jobs (job_id, status, request_data, created_at, updated_at, estimated_time)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (job_id, "started", json.dumps(request_data), now, now, 5))
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ Job created: {job_id}")
            return job_id
            
        except Exception as e:
            logger.error(f"❌ Create job error: {e}")
            return None
    
    def get_job_status(self, job_id: str) -> Optional[Dict]:
        """Obtener status del job"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM jobs WHERE job_id = ?', (job_id,))
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return {
                    "job_id": row[0],
                    "status": row[1],
                    "request_data": json.loads(row[2]) if row[2] else {},
                    "results": json.loads(row[3]) if row[3] else None,
                    "created_at": row[4],
                    "updated_at": row[5],
                    "estimated_time": row[6]
                }
            return None
            
        except Exception as e:
            logger.error(f"❌ Get job error: {e}")
            return None
    
    def update_job(self, job_id: str, status: str, results: List[Dict] = None):
        """Actualizar job"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            now = datetime.now().isoformat()
            results_json = json.dumps(results) if results else None
            
            cursor.execute('''
                UPDATE jobs 
                SET status = ?, results = ?, updated_at = ?
                WHERE job_id = ?
            ''', (status, results_json, now, job_id))
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ Job updated: {job_id} -> {status}")
            
        except Exception as e:
            logger.error(f"❌ Update job error: {e}")

# Instancia global
job_db = JobDatabase()
