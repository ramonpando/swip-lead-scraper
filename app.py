#!/usr/bin/env python3
"""
Swip Lead Scraper API
API para scraping y procesamiento de leads PyME
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Importar nuestros módulos
from database import job_db
from scrapers.google_maps_scraper import GoogleMapsLeadScraper

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Schemas
class ScrapingRequest(BaseModel):
    sectors: List[str] = Field(..., description="Sectores a scrapear")
    locations: List[str] = Field(..., description="Ubicaciones a scrapear")
    max_leads_per_sector: int = Field(default=10, description="Máximo leads por sector")
    sources: List[str] = Field(default=["google_maps"], description="Fuentes de scraping")

class ScrapingResponse(BaseModel):
    job_id: str
    status: str
    message: str
    estimated_time: int

class JobStatus(BaseModel):
    job_id: str
    status: str
    progress: Optional[str] = None
    estimated_time: Optional[int] = None
    created_at: str
    updated_at: str

class ValidationError(BaseModel):
    detail: str

# Lifecycle
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle de la aplicación"""
    logger.info("🚀 Starting Swip Lead Scraper API")
    
    # Verificar scrapers disponibles
    scrapers_status = await check_scrapers()
    logger.info(f"📊 Scrapers status: {scrapers_status}")
    
    yield
    
    logger.info("🛑 Shutting down Swip Lead Scraper API")

# App
app = FastAPI(
    title="Swip Lead Scraper API",
    description="API para scraping y procesamiento de leads PyME",
    version="1.0.0",
    lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Helper functions
async def check_scrapers() -> Dict[str, bool]:
    """Verificar estado de scrapers"""
    scrapers = {}
    
    try:
        scraper = GoogleMapsLeadScraper()
        scrapers["google_maps"] = scraper.test_connection()
    except Exception as e:
        logger.error(f"Google Maps scraper error: {e}")
        scrapers["google_maps"] = False
    
    return scrapers

async def run_scraping_job(job_id: str, request_data: ScrapingRequest):
    """Ejecutar scraping job en background"""
    try:
        logger.info(f"🎯 Starting scraping job: {job_id}")
        
        all_leads = []
        
        for sector in request_data.sectors:
            for location in request_data.locations:
                logger.info(f"🔍 Scraping: {sector} in {location}")
                
                try:
                    # Usar Google Maps scraper
                    scraper = GoogleMapsLeadScraper()
                    leads = await scraper.scrape_leads(
                        sector=sector,
                        location=location,
                        max_leads=request_data.max_leads_per_sector
                    )
                    
                    all_leads.extend(leads)
                    logger.info(f"✅ Found {len(leads)} leads for {sector} in {location}")
                    
                    # Pausa entre sectores
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    logger.error(f"❌ Scraping error for {sector} in {location}: {e}")
                    continue
        
        # Actualizar job con resultados
        job_db.update_job(job_id, "completed", all_leads)
        
        logger.info(f"🎉 Job completed: {job_id} with {len(all_leads)} total leads")
        
    except Exception as e:
        logger.error(f"❌ Job failed: {job_id} - {e}")
        job_db.update_job(job_id, "failed", [])

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
    """Health check"""
    scrapers = await check_scrapers()
    
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "scrapers": scrapers
    }

@app.post("/scrape", response_model=ScrapingResponse)
async def start_scraping(
    request: ScrapingRequest,
    background_tasks: BackgroundTasks
):
    """Iniciar proceso de scraping"""
    try:
        # Validar sources
        valid_sources = ["google_maps"]
        invalid_sources = [s for s in request.sources if s not in valid_sources]
        if invalid_sources:
            raise HTTPException(
                status_code=400,
                detail=f"Fuentes inválidas: {invalid_sources}. Válidas: {valid_sources}"
            )
        
        # Crear job en database
        job_id = job_db.create_job(request.dict())
        
        if not job_id:
            raise HTTPException(status_code=500, detail="Error creando job")
        
        # Ejecutar scraping en background
        background_tasks.add_task(run_scraping_job, job_id, request)
        
        return ScrapingResponse(
            job_id=job_id,
            status="started",
            message="Scraping iniciado exitosamente",
            estimated_time=len(request.sectors) * len(request.locations) * 2
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Start scraping error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/jobs/{job_id}/status", response_model=JobStatus)
async def get_job_status(job_id: str):
    """Obtener status de un job"""
    try:
        job = job_db.get_job_status(job_id)
        
        if not job:
            raise HTTPException(status_code=404, detail="Job no encontrado")
        
        return JobStatus(
            job_id=job["job_id"],
            status=job["status"],
            estimated_time=job.get("estimated_time"),
            created_at=job["created_at"],
            updated_at=job["updated_at"]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get job status error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/jobs/{job_id}/results")
async def get_job_results(job_id: str):
    """Obtener resultados de un job"""
    try:
        job = job_db.get_job_status(job_id)
        
        if not job:
            raise HTTPException(status_code=404, detail="Job no encontrado")
        
        if job["status"] == "started":
            return {
                "job_id": job_id,
                "status": "processing",
                "message": "Job aún en proceso"
            }
        elif job["status"] == "failed":
            return {
                "job_id": job_id,
                "status": "failed",
                "message": "Job falló"
            }
        elif job["status"] == "completed":
            results = job.get("results", [])
            return {
                "job_id": job_id,
                "status": "completed",
                "total_leads": len(results),
                "leads": results
            }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get job results error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/test-scraper")
async def test_scraper(source: str = "google_maps"):
    """Probar un scraper específico"""
    try:
        if source == "google_maps":
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
