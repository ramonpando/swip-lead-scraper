#!/usr/bin/env python3
"""
Google Maps Lead Scraper - VERSIÓN REQUESTS
Scraper funcional SIN Selenium usando requests + BeautifulSoup
"""

import asyncio
import time
import random
from typing import List, Dict, Optional
import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime
import re

logger = logging.getLogger(__name__)

class GoogleMapsLeadScraper:
    def __init__(self):
        self.session = requests.Session()
        self.leads = []
        
        # Headers para parecer navegador real
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'es-MX,es;q=0.9,en;q=0.8',
        })
        
        # Sectores específicos para créditos PyME
        self.sector_terms = {
            "Restaurantes": [
                "restaurante", "taquería", "fonda", "comida casera"
            ],
            "Talleres": [
                "taller mecánico", "hojalatería", "vulcanizadora"
            ],
            "Comercio": [
                "tienda de ropa", "zapatería", "ferretería"
            ],
            "Servicios": [
                "estética", "salón de belleza", "barbería"
            ],
            "Producción": [
                "panadería", "tortillería", "carnicería"
            ]
        }

    def test_connection(self) -> bool:
        """Testa la conexión"""
        try:
            response = self.session.get("https://www.google.com", timeout=10)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Test connection failed: {e}")
            return False

    async def test_single_search(self, sector: str, location: str, max_results: int = 1) -> List[Dict]:
        """Test con una búsqueda usando Google Search"""
        try:
            query = f"{sector} en {location} teléfono contacto"
            results = await self._search_google(query, max_results)
            return results
            
        except Exception as e:
            logger.error(f"Test search failed: {e}")
            return []

    async def scrape_leads(self, sector: str, location: str, max_leads: int = 10) -> List[Dict]:
        """Scraping principal usando Google Search"""
        try:
            logger.info(f"🎯 Iniciando scraping: {sector} en {location}")
            
            all_leads = []
            terms = self.sector_terms.get(sector, [sector])
            
            # Probar 2 términos máximo
            for term in terms[:2]:
                search_query = f"{term} {location} teléfono contacto"
                
                logger.info(f"🔍 Buscando: {search_query}")
                
                leads_batch = await self._search_google(search_query, max_leads // 2)
                all_leads.extend(leads_batch)
                
                # Pausa entre búsquedas
                await asyncio.sleep(random.uniform(2, 5))
                
                if len(all_leads) >= max_leads:
                    break
            
            # Procesar leads
            processed_leads = self._process_leads(all_leads, sector, location)
            
            logger.info(f"✅ Scraping completado: {len(processed_leads)} leads")
            return processed_leads[:max_leads]
            
        except Exception as e:
            logger.error(f"❌ Error en scraping: {e}")
            return []

    async def _search_google(self, query: str, max_results: int) -> List[Dict]:
        """Busca en Google usando requests"""
        try:
            # URL de búsqueda en Google
            search_url = f"https://www.google.com/search?q={requests.utils.quote(query)}"
            
            response = self.session.get(search_url, timeout=15)
            
            if response.status_code != 200:
                logger.warning(f"Error HTTP {response.status_code}")
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extraer resultados
            results = await self._extract_google_results(soup, max_results)
            
            return results
            
        except Exception as e:
            logger.error(f"Error en búsqueda Google: {e}")
            return []

    async def _extract_google_results(self, soup: BeautifulSoup, max_results: int) -> List[Dict]:
        """Extrae información de los resultados de Google"""
        businesses = []
        
        try:
            # Buscar en diferentes elementos de Google
            result_divs = soup.find_all('div', class_='g')
            
            if not result_divs:
                # Intentar otros selectores
                result_divs = soup.find_all('div', {'data-ved': True})
            
            logger.info(f"📊 Encontrados {len(result_divs)} elementos")
            
            for i, div in enumerate(result_divs[:max_results * 2]):
                try:
                    business_data = await self._extract_business_from_div(div)
                    
                    if business_data:
                        businesses.append(business_data)
                        logger.info(f"✅ Negocio {len(businesses)}: {business_data.get('name', 'Sin nombre')}")
                    
                    if len(businesses) >= max_results:
                        break
                        
                except Exception as e:
                    continue
            
        except Exception as e:
            logger.error(f"Error extrayendo resultados: {e}")
        
        return businesses

    async def _extract_business_from_div(self, div) -> Optional[Dict]:
        """Extrae información de un div de resultado"""
        try:
            text_content = div.get_text()
            
            if len(text_content) < 20:
                return None
            
            business = {
                'extracted_at': datetime.now().isoformat(),
                'source': 'Google Search'
            }
            
            # Extraer nombre (primer enlace o texto relevante)
            link = div.find('a')
            if link:
                title = link.get_text().strip()
                if 5 < len(title) < 100:
                    business['name'] = title
            
            # Buscar teléfonos
            phone_patterns = [
                r'\+?52\s*\d{2}\s*\d{4}\s*\d{4}',
                r'\(\d{2,3}\)\s*\d{3,4}[-\s]?\d{4}',
                r'\d{2,3}[-\s]?\d{3,4}[-\s]?\d{4}'
            ]
            
            for pattern in phone_patterns:
                phone_match = re.search(pattern, text_content)
                if phone_match:
                    business['phone'] = phone_match.group().strip()
                    break
            
            # Buscar dirección
            address_patterns = [
                r'(?:Calle|Av\.|Avenida|Blvd\.)[^,\n]+',
                r'C\.P\.?\s*\d{5}',
                r'#\d+[^,\n]+'
            ]
            
            for pattern in address_patterns:
                addr_match = re.search(pattern, text_content, re.IGNORECASE)
                if addr_match:
                    business['address'] = addr_match.group().strip()
                    break
            
            # Solo retornar si tiene información mínima útil
            if business.get('name') or business.get('phone'):
                return business
                
            return None
            
        except Exception as e:
            return None

    def _process_leads(self, leads: List[Dict], sector: str, location: str) -> List[Dict]:
        """Procesa y enriquece leads"""
        processed = []
        
        for lead in leads:
            try:
                enhanced_lead = {
                    'name': lead.get('name', f'Negocio {sector}'),
                    'phone': lead.get('phone', ''),
                    'email': '',  # No disponible en búsqueda
                    'address': lead.get('address', ''),
                    'sector': sector,
                    'location': location,
                    'source': 'Google Search',
                    'credit_potential': self._calculate_credit_potential(lead, sector),
                    'extracted_at': lead.get('extracted_at', datetime.now().isoformat())
                }
                
                processed.append(enhanced_lead)
                
            except Exception as e:
                continue
        
        return processed

    def _calculate_credit_potential(self, business: Dict, sector: str) -> str:
        """Calcula potencial de crédito"""
        score = 0
        
        # Score por sector
        high_capital_sectors = ['Restaurantes', 'Talleres', 'Producción']
        if sector in high_capital_sectors:
            score += 2
        else:
            score += 1
        
        # Score por información disponible
        if business.get('phone'):
            score += 2
        if business.get('address'):
            score += 1
        
        # Determinar potencial
        if score >= 4:
            return "ALTO"
        elif score >= 2:
            return "MEDIO"
        else:
            return "BAJO"
