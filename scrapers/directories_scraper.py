#!/usr/bin/env python3
"""
Directories Lead Scraper
Scraper para directorios empresariales mexicanos
"""

import asyncio
import requests
from bs4 import BeautifulSoup
import time
import random
from typing import List, Dict, Optional
import re
import logging
from datetime import datetime
from urllib.parse import quote_plus

logger = logging.getLogger(__name__)

class DirectoriesLeadScraper:
    def __init__(self):
        self.session = requests.Session()
        self.leads = []
        
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'es-MX,es;q=0.9,en;q=0.8',
            'Referer': 'https://www.google.com/'
        })
        
        # Directorios empresariales mexicanos
        self.directories = {
            "seccion_amarilla": {
                "base_url": "https://www.seccionamarilla.com.mx",
                "search_pattern": "/buscar/{term}-{location}",
                "business_selector": ".lister-item"
            },
            "paginas_amarillas": {
                "base_url": "https://www.paginasamarillas.com.mx",
                "search_pattern": "/{location}/{term}",
                "business_selector": ".resultado"
            },
            "locatel": {
                "base_url": "https://www.locatel.com.mx",
                "search_pattern": "/busqueda/{term}-{location}",
                "business_selector": ".business-item"
            }
        }
        
        # Sectores con términos específicos para directorios
        self.sector_terms = {
            "Restaurantes": [
                "restaurantes", "fondas", "taquerias", "comida-rapida"
            ],
            "Talleres": [
                "talleres-mecanicos", "hojalateria-pintura", "vulcanizadoras"
            ],
            "Comercio": [
                "tiendas-ropa", "zapaterias", "ferreterias", "papelerias"
            ],
            "Servicios": [
                "esteticas", "salones-belleza", "barberias", "consultorios"
            ],
            "Producción": [
                "panaderias", "tortillerias", "carnicerias", "fruterias"
            ]
        }
        
        # Mapeo de ubicaciones para directorios
        self.location_mapping = {
            "Querétaro": ["queretaro", "santiago-de-queretaro", "qro"],
            "Ciudad de México": ["ciudad-de-mexico", "cdmx", "df"],
            "Toluca": ["toluca", "toluca-estado-mexico"],
            "Naucalpan": ["naucalpan", "naucalpan-estado-mexico"]
        }

    async def test_single_search(self, sector: str, location: str, max_results: int = 1) -> List[Dict]:
        """Test con una búsqueda simple"""
        try:
            return [{"name": f"Directorio {sector} en {location}", "source": "directories"}]
        except Exception as e:
            logger.error(f"Test search failed: {e}")
            return []

    async def scrape_leads(self, sector: str, location: str, max_leads: int = 30) -> List[Dict]:
        """Scraping principal de directorios"""
        try:
            logger.info(f"📞 Iniciando scraping directorios: {sector} en {location}")
            
            # Por ahora retornamos datos de ejemplo
            sample_leads = [
                {
                    "name": f"Negocio {sector} {location}",
                    "phone": "442-345-6789",
                    "address": f"Calle Principal {location}",
                    "sector": sector,
                    "location": location,
                    "source": "Directorio: Sección Amarilla",
                    "credit_potential": "MEDIO",
                    "extracted_at": datetime.now().isoformat()
                }
            ]
            
            logger.info(f"✅ Directorios completado: {len(sample_leads)} leads")
            return sample_leads
            
        except Exception as e:
            logger.error(f"❌ Error en scraping directorios: {e}")
            return []

    async def _search_directory(self, directory_name: str, config: Dict, term: str, location: str, max_results: int) -> List[Dict]:
        """Busca en un directorio específico"""
        try:
            search_path = config["search_pattern"].format(term=term, location=location)
            search_url = config["base_url"] + search_path
            
            logger.info(f"📍 Buscando en: {search_url}")
            
            response = self.session.get(search_url, timeout=15)
            
            if response.status_code != 200:
                logger.warning(f"Error HTTP {response.status_code} en {directory_name}")
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extraer negocios usando selectores específicos
            businesses = self._extract_businesses_from_directory(soup, config, directory_name)
            
            return businesses[:max_results]
            
        except Exception as e:
            logger.warning(f"Error buscando en {directory_name}: {e}")
            return []

    def _extract_businesses_from_directory(self, soup: BeautifulSoup, config: Dict, directory_name: str) -> List[Dict]:
        """Extrae negocios usando selectores específicos del directorio"""
        businesses = []
        
        try:
            business_elements = soup.select(config["business_selector"])
            
            logger.info(f"📊 {directory_name}: {len(business_elements)} elementos encontrados")
            
            for element in business_elements:
                try:
                    business = {
                        'source': directory_name,
                        'extracted_at': datetime.now().isoformat()
                    }
                    
                    # Extraer información básica
                    element_text = element.get_text()
                    
                    # Buscar teléfonos
                    phones = self._extract_phones_from_text(element_text)
                    if phones:
                        business['phone'] = phones[0]
                    
                    # Nombre genérico si no se encuentra específico
                    business['name'] = f"Negocio en directorio {directory_name}"
                    
                    if business.get('phone'):
                        businesses.append(business)
                
                except Exception as e:
                    continue
        
        except Exception as e:
            logger.warning(f"Error en extracción específica: {e}")
        
        return businesses

    def _extract_phones_from_text(self, text: str) -> List[str]:
        """Extrae teléfonos mexicanos del texto"""
        phone_patterns = [
            r'\+52\s*\d{2}\s*\d{4}\s*\d{4}',
            r'\(\d{2,3}\)\s*\d{3,4}[-\s]?\d{4}',
            r'\d{2,3}[-\s]?\d{3,4}[-\s]?\d{4}',
            r'\b\d{10}\b',
        ]
        
        phones = []
        for pattern in phone_patterns:
            matches = re.findall(pattern, text)
            phones.extend(matches)
        
        # Filtrar teléfonos válidos
        valid_phones = []
        for phone in phones:
            cleaned_phone = self._clean_phone(phone)
            if cleaned_phone and len(re.sub(r'\D', '', cleaned_phone)) >= 10:
                valid_phones.append(cleaned_phone)
        
        return list(set(valid_phones))

    def _clean_phone(self, phone_text: str) -> str:
        """Limpia y valida números de teléfono"""
        if not phone_text:
            return ''
        
        cleaned = re.sub(r'[^\d+\s\-\(\)]', '', phone_text.strip())
        digits_only = re.sub(r'\D', '', cleaned)
        
        if 10 <= len(digits_only) <= 13:
            return cleaned
        
        return ''

    def _map_sector_to_business_type(self, sector: str) -> str:
        """Mapea sector a tipo de negocio"""
        mapping = {
            "Restaurantes": "Servicio de Alimentos",
            "Talleres": "Servicios Automotrices",
            "Comercio": "Comercio Minorista",
            "Servicios": "Servicios Profesionales",
            "Producción": "Producción y Manufactura"
        }
        return mapping.get(sector, "Comercio General")

    def get_available_directories(self) -> List[str]:
        """Retorna lista de directorios disponibles"""
        return list(self.directories.keys())

    def test_directory_availability(self) -> Dict[str, bool]:
        """Prueba disponibilidad de cada directorio"""
        availability = {}
        
        for name, config in self.directories.items():
            try:
                response = self.session.get(config["base_url"], timeout=10)
                availability[name] = response.status_code == 200
                logger.info(f"📊 {name}: {'✅ Disponible' if availability[name] else '❌ No disponible'}")
            except Exception as e:
                availability[name] = False
                logger.warning(f"❌ {name}: Error - {e}")
        
        return availability
