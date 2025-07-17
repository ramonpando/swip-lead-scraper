#!/usr/bin/env python3
"""
Scraper completo funcional para Sección Amarilla
CON URL DINÁMICA - Respeta la categoría que le envías
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

# Logger setup
logger = logging.getLogger(__name__)

class GoogleMapsLeadScraper:
    """Scraper funcional para Sección Amarilla con URL dinámica"""
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        self.extracted_leads = set()

    def test_connection(self) -> bool:
        try:
            response = self.session.get("https://www.google.com", timeout=10)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Test connection failed: {e}")
            return False

    async def test_single_search(self, sector: str, location: str, max_results: int = 1) -> List[Dict]:
        return await self.scrape_leads(sector, location, max_results)

    async def scrape_leads(self, sector: str, location: str, max_leads: int = 10):
    # ...
    # CONSTRUIR URL BASADA EN PARÁMETROS
    if "contadores" in sector.lower():
        url = "https://www.seccionamarilla.com.mx/resultados/contadores/distrito-federal/zona-metropolitana/1"
    elif "abogados" in sector.lower():
        url = "https://www.seccionamarilla.com.mx/resultados/abogados/distrito-federal/zona-metropolitana/1"
    else:
        url = "https://www.seccionamarilla.com.mx/resultados/agencias-de-marketing/distrito-federal/zona-metropolitana/1"
    # ...
    return await self.scrape_leads_from_url(url, max_leads)
            
        except Exception as e:
            logger.error(f"❌ Error en scraping: {e}")
            return []

    async def scrape_leads_from_url(self, url: str, max_leads: int = 10) -> List[Dict]:
        """Scrapear desde URL específica - NUEVA FUNCIÓN"""
        try:
            logger.info(f"🔥 Scraping URL específica: {url}")
            
            # USAR LA URL TAL COMO VIENE - NO HARDCODED
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            leads = []
            
            # Determinar sector basado en URL
            sector = self._extract_sector_from_url(url)
            
            # Buscar en filas de tabla
            business_rows = soup.find_all('tr')
            logger.info(f"📋 Filas encontradas: {len(business_rows)}")
            
            for row in business_rows:
                lead = self._extract_from_business_row(row, sector)
                if lead and len(leads) < max_leads:
                    # Evitar duplicados
                    lead_id = f"{lead.get('name', '')}-{lead.get('phone', '')}"
                    if lead_id not in self.extracted_leads:
                        self.extracted_leads.add(lead_id)
                        leads.append(lead)
                        logger.info(f"✅ Lead extraído: {lead.get('name', 'Sin nombre')}")
            
            # También buscar enlaces de teléfono
            phone_links = soup.find_all('a', href=re.compile(r'tel:'))
            logger.info(f"📞 Enlaces de teléfono: {len(phone_links)}")
            
            for link in phone_links:
                if len(leads) >= max_leads:
                    break
                lead = self._extract_from_phone_link(link, soup, sector)
                if lead:
                    lead_id = f"{lead.get('name', '')}-{lead.get('phone', '')}"
                    if lead_id not in self.extracted_leads:
                        self.extracted_leads.add(lead_id)
                        leads.append(lead)
            
            logger.info(f"🎯 Total leads de {sector}: {len(leads)}")
            return leads
            
        except Exception as e:
            logger.error(f"❌ Error scraping {url}: {e}")
            return []

    def _extract_sector_from_url(self, url: str) -> str:
        """Extraer sector de la URL - NUEVA FUNCIÓN"""
        if 'contadores' in url.lower():
            return 'Contadores'
        elif 'abogados' in url.lower():
            return 'Abogados'
        elif 'marketing' in url.lower():
            return 'Marketing/Publicidad'
        elif 'arquitectos' in url.lower():
            return 'Arquitectos'
        elif 'ingenieros' in url.lower():
            return 'Ingenieros'
        elif 'medicos' in url.lower():
            return 'Médicos'
        elif 'dentistas' in url.lower():
            return 'Dentistas'
        elif 'consultores' in url.lower():
            return 'Consultores'
        elif 'publicidad' in url.lower():
            return 'Publicidad'
        else:
            return 'Servicios Profesionales'

    def _extract_from_business_row(self, row, sector: str) -> Optional[Dict]:
        """Extraer información de fila de negocio - ACTUALIZADA"""
        try:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 2:
                return None
            
            row_text = row.get_text(strip=True)
            
            # Skip filas irrelevantes
            skip_keywords = ['nombre', 'estatus', 'acciones', 'encuentra los mejores', 'buscar']
            if any(keyword in row_text.lower() for keyword in skip_keywords):
                return None
            
            # Buscar nombre de empresa
            name = None
            for cell in cells:
                cell_text = cell.get_text(strip=True)
                if (len(cell_text) > 3 and 
                    not cell_text.lower() in ['abierto', 'cerrado'] and
                    not cell_text.startswith('AV.') and
                    not cell_text.startswith('CALLE') and
                    not re.match(r'\(\d+\)', cell_text)):
                    
                    name = cell_text
                    break
            
            # Buscar teléfono
            phone = self._extract_phone(row_text)
            
            # Buscar dirección
            address = self._extract_address_from_row(row)
            
            if name and phone and len(name) > 3:
                return {
                    'name': name,
                    'phone': phone,
                    'email': None,
                    'address': address or "México, DF",
                    'sector': sector,  # <-- USAR SECTOR DINÁMICO
                    'location': 'México, DF',
                    'source': 'seccion_amarilla',
                    'credit_potential': self._assess_credit_potential(sector),
                    'estimated_revenue': self._estimate_revenue(sector),
                    'loan_range': self._estimate_loan_range(sector),
                    'extracted_at': datetime.now().isoformat(),
                    'debug_results_type': f'<class "business_row_{sector}">'  # <-- INCLUIR SECTOR
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error extrayendo de fila: {e}")
            return None

    def _extract_from_phone_link(self, link, soup, sector: str) -> Optional[Dict]:
        """Extraer información del enlace de teléfono - ACTUALIZADA"""
        try:
            phone = link.get('href').replace('tel:', '').strip()
            
            # Buscar el contenedor padre del teléfono
            container = link.find_parent(['tr', 'div', 'td'])
            if container:
                name = self._find_business_name_in_container(container)
                
                if name and phone:
                    return {
                        'name': name,
                        'phone': phone,
                        'email': None,
                        'address': "México, DF",
                        'sector': sector,  # <-- USAR SECTOR DINÁMICO
                        'location': 'México, DF',
                        'source': 'seccion_amarilla',
                        'credit_potential': self._assess_credit_potential(sector),
                        'estimated_revenue': self._estimate_revenue(sector),
                        'loan_range': self._estimate_loan_range(sector),
                        'extracted_at': datetime.now().isoformat(),
                        'debug_results_type': f'<class "phone_link_{sector}">'
                    }
            
            return None
            
        except Exception as e:
            logger.error(f"Error extrayendo de enlace: {e}")
            return None

    def _assess_credit_potential(self, sector: str) -> str:
        """Evaluar potencial crediticio basado en sector"""
        high_potential = ['Contadores', 'Abogados', 'Arquitectos', 'Ingenieros', 'Médicos']
        medium_high = ['Marketing/Publicidad', 'Consultores', 'Dentistas']
        
        if sector in high_potential:
            return 'ALTO'
        elif sector in medium_high:
            return 'MEDIO-ALTO'
        else:
            return 'MEDIO'

    def _estimate_revenue(self, sector: str) -> str:
        """Estimar ingresos basado en sector"""
        if sector in ['Contadores', 'Abogados', 'Médicos']:
            return '$400,000 - $1,200,000'
        elif sector in ['Arquitectos', 'Ingenieros']:
            return '$300,000 - $800,000'
        elif sector in ['Marketing/Publicidad', 'Consultores']:
            return '$200,000 - $600,000'
        else:
            return '$150,000 - $400,000'

    def _estimate_loan_range(self, sector: str) -> str:
        """Estimar rango de préstamo basado en sector"""
        if sector in ['Contadores', 'Abogados', 'Médicos']:
            return '$100,000 - $3,000,000'
        elif sector in ['Arquitectos', 'Ingenieros']:
            return '$75,000 - $2,000,000'
        else:
            return '$50,000 - $1,200,000'

    def _extract_phone(self, text: str) -> Optional[str]:
        """Extraer teléfono del texto"""
        patterns = [
            r'\(\d{2,3}\)\d{3,4}-?\d{4}',  # (55)1234-5678
            r'\d{10}',  # 5512345678
            r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}',  # 555-123-4567
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group()
        
        return None

    def _extract_address_from_row(self, row) -> Optional[str]:
        """Extraer dirección de la fila"""
        try:
            cells = row.find_all(['td', 'th'])
            
            # Buscar celda que contenga dirección
            for cell in cells:
                text = cell.get_text(strip=True)
                if any(indicator in text.upper() for indicator in ['AV.', 'CALLE', 'COL.', 'BENITO', 'JUAREZ']):
                    return text[:100]
            
            return None
            
        except Exception as e:
            return None

    def _find_business_name_in_container(self, container) -> Optional[str]:
        """Encontrar nombre de negocio en contenedor"""
        try:
            # Buscar en elementos de texto prominente
            for tag in ['h1', 'h2', 'h3', 'h4', 'strong', 'b']:
                elem = container.find(tag)
                if elem:
                    text = elem.get_text(strip=True)
                    if len(text) > 3:
                        return text[:50]
            
            # Buscar en texto general
            texts = container.find_all(text=True)
            for text in texts:
                text = text.strip()
                if (len(text) > 3 and 
                    not re.match(r'^\(\d+\)', text) and
                    not text.lower() in ['abierto', 'cerrado'] and
                    not text.startswith('AV.')):
                    
                    return text[:50]
            
            return None
            
        except Exception as e:
            return None

# Función para compatibilidad con el sistema existente - CORREGIDA
def scrape_seccion_amarilla(url):
    """Función compatible - AHORA USA URL REAL"""
    scraper = GoogleMapsLeadScraper()
    
    # Ejecutar scraping síncrono
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        # IMPORTANTE: Usar la URL real que viene del request
        logger.info(f"🎯 URL recibida: {url}")
        
        # NUEVA LÓGICA: Usar la URL real en lugar de ignorarla
        results = loop.run_until_complete(scraper.scrape_leads_from_url(url, 10))
        return results
    finally:
        loop.close()
   
