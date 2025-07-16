#!/usr/bin/env python3
"""
Swip Lead Scraper API
FastAPI application para scraping de leads PyME
"""

from fastapi import FastAPI, BackgroundTasks, HTTPException, Depends
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import os
import json
import uuid
import asyncio
from datetime import datetime
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/api.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Crear aplicación FastAPI
app = FastAPI(
    title="Swip Lead Scraper API",
    description="API para scraping y procesamiento de leads PyME",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)
@app.post("/test-scraper")
async def test_scraper(source: str = "google_maps"):
    """Probar un scraper específico"""
    try:
        if source == "google_maps":
            from scrapers.google_maps_scraper import GoogleMapsLeadScraper
            scraper = GoogleMapsLeadScraper()
            # Test con un restaurante en Querétaro
            results = await scraper.test_single_search("Restaurantes", "Querétaro", 1)
        else:
            raise HTTPException(status_code=400, detail="Fuente no válida")
        
        return {
            "source": source,
            "test_results": results,
            "status": "success" if results else "no_results"
        }
        
    except Exception as e:
        logger.error(f"Test scraper error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Estado global de jobs
active_jobs: Dict[str, Dict] = {}

# Modelos Pydantic
class ScrapingRequest(BaseModel):
    """Modelo para request de scraping"""
    sectors: List[str] = Field(..., description="Sectores a scrapear", example=["Restaurantes", "Talleres"])
    locations: List[str] = Field(..., description="Ubicaciones objetivo", example=["Querétaro", "Ciudad de México"])
    max_leads_per_sector: int = Field(default=50, ge=1, le=200, description="Máximo leads por sector")
    sources: List[str] = Field(default=["google_maps"], description="Fuentes de scraping")
    filters: Optional[Dict[str, Any]] = Field(default=None, description="Filtros adicionales")
    webhook_url: Optional[str] = Field(default=None, description="URL webhook para notificaciones")
    output_format: str = Field(default="json", description="Formato de salida")

class LeadModel(BaseModel):
    """Modelo para un lead"""
    name: str
    phone: Optional[str] = None
    email: Optional[str] = None
    address: Optional[str] = None
    sector: str
    location: str
    source: str
    rating: Optional[float] = None
    website: Optional[str] = None
    credit_potential: str
    extracted_at: datetime

class ScrapingResponse(BaseModel):
    """Modelo para respuesta de scraping"""
    job_id: str
    status: str
    message: str
    estimated_time: Optional[int] = None

class JobStatus(BaseModel):
    """Modelo para estado del job"""
    job_id: str
    status: str
    progress: float
    total_leads: int
    current_sector: Optional[str] = None
    current_location: Optional[str] = None
    started_at: datetime
    estimated_completion: Optional[datetime] = None
    error_message: Optional[str] = None

# Endpoints

@app.get("/")
async def root():
    """Endpoint raíz"""
    return {
        "service": "Swip Lead Scraper API",
        "version": "1.0.0",
        "status": "active",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/health")
async def health_check():
    """Health check para Docker"""
    try:
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "scrapers": {
                "google_maps": True,
                "mercadolibre": True,
                "directories": True
            }
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "error": str(e)}
        )

@app.post("/scrape", response_model=ScrapingResponse)
async def start_scraping(
    request: ScrapingRequest, 
    background_tasks: BackgroundTasks
):
    """Iniciar proceso de scraping"""
    try:
        job_id = str(uuid.uuid4())
        
        active_jobs[job_id] = {
            "status": "started",
            "progress": 0.0,
            "total_leads": 0,
            "started_at": datetime.now(),
            "request": request.dict(),
            "results": []
        }
        
        background_tasks.add_task(execute_scraping, job_id, request)
        
        estimated_time = len(request.sectors) * len(request.locations) * 3
        
        logger.info(f"Started scraping job {job_id}")
        
        return ScrapingResponse(
            job_id=job_id,
            status="started",
            message="Scraping iniciado exitosamente",
            estimated_time=estimated_time
        )
        
    except Exception as e:
        logger.error(f"Error starting scraping: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/jobs/{job_id}/status", response_model=JobStatus)
async def get_job_status(job_id: str):
    """Obtener estado de un job de scraping"""
    if job_id not in active_jobs:
        raise HTTPException(status_code=404, detail="Job no encontrado")
    
    job = active_jobs[job_id]
    
    return JobStatus(
        job_id=job_id,
        status=job["status"],
        progress=job["progress"],
        total_leads=job["total_leads"],
        current_sector=job.get("current_sector"),
        current_location=job.get("current_location"),
        started_at=job["started_at"],
        estimated_completion=job.get("estimated_completion"),
        error_message=job.get("error_message")
    )

@app.get("/jobs/{job_id}/results")
async def get_job_results(job_id: str, format: str = "json"):
    """Obtener resultados de un job"""
    if job_id not in active_jobs:
        raise HTTPException(status_code=404, detail="Job no encontrado")
    
    job = active_jobs[job_id]
    
    if job["status"] != "completed":
        raise HTTPException(status_code=400, detail="Job aún no completado")
    
    results = job["results"]
    
    if format == "csv":
        csv_path = f"/app/downloads/{job_id}_leads.csv"
        # Aquí iría la lógica para generar CSV
        return {"message": "CSV generation not implemented yet"}
    
    return {"job_id": job_id, "total_leads": len(results), "leads": results}

async def execute_scraping(job_id: str, request: ScrapingRequest):
    """Ejecutar proceso de scraping completo"""
    try:
        job = active_jobs[job_id]
        job["status"] = "running"
        
        # Simulación de scraping por ahora
        await asyncio.sleep(2)
        
        # Datos de ejemplo
        sample_leads = [
            {
                "name": f"Empresa de {request.sectors[0]} en {request.locations[0]}",
                "phone": "442-123-4567",
                "email": "contacto@empresa.com",
                "sector": request.sectors[0],
                "location": request.locations[0],
                "source": "google_maps",
                "credit_potential": "ALTO"
            }
        ]
        
        job["results"] = sample_leads
        job["total_leads"] = len(sample_leads)
        job["status"] = "completed"
        job["progress"] = 100.0
        job["completed_at"] = datetime.now()
        
        logger.info(f"Scraping job {job_id} completed")
        
    except Exception as e:
        logger.error(f"Scraping job {job_id} failed: {e}")
        active_jobs[job_id]["status"] = "failed"
        active_jobs[job_id]["error_message"] = str(e)
