#!/usr/bin/env python3
"""
Scraper Sección Amarilla - Selectores corregidos para estructura real
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
    """Scraper para Sección Amarilla con selectores corregidos"""
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

    def test_connection(self) -> bool:
        try:
            response = self.session.get("https://www.google.com", timeout=10)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Test connection failed: {e}")
            return False

    async def test_single_search(self, sector: str, location: str, max_results: int = 1) -> List[Dict]:
        return await self.scrape_leads(sector, location, max_results)

    async def scrape_leads(self, sector: str, location: str, max_leads: int = 10) -> List[Dict]:
        try:
            logger.info(f"🔥 Iniciando scraping: {sector} en {location}")
            
            # URL de Sección Amarilla
            if "marketing" in sector.lower():
                url = "https://www.seccionamarilla.com.mx/resultados/agencias-de-marketing/distrito-federal/zona-metropolitana/1"
            else:
                sector_clean = sector.replace(' ', '-').lower()
                location_clean = location.replace(' ', '-').lower()
                url = f"https://www.seccionamarilla.com.mx/resultados/{sector_clean}/{location_clean}/1"
            
            logger.info(f"📍 URL a scrapear: {url}")
            
            # Hacer request
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            leads = []
            
            # ESTRATEGIA CORREGIDA: Buscar elementos de negocio individuales
            
            # Método 1: Buscar por estructura de tabla/filas
            business_rows = soup.find_all('tr')
            logger.info(f"📋 Filas encontradas: {len(business_rows)}")
            
            for row in business_rows:
                lead = self._extract_from_business_row(row)
                if lead and len(leads) < max_leads:
                    leads.append(lead)
                    logger.info(f"✅ Lead extraído: {lead.get('name', 'Sin nombre')}")
            
            # Método 2: Buscar contenedores con nombres de empresas
            if not leads:
                # Buscar elementos que contengan nombres de empresas
                potential_names = soup.find_all(text=True)
                phone_elements = soup.find_all(string=re.compile(r'\(\d{2,3}\)\d{3,4}-?\d{4}|\d{10}'))
                
                logger.info(f"📞 Elementos con teléfonos: {len(phone_elements)}")
                
                # Extraer información de elementos vecinos a teléfonos
                for phone_elem in phone_elements:
                    lead = self._extract_from_phone_context(phone_elem, soup)
                    if lead and len(leads) < max_leads:
                        leads.append(lead)
            
            # Método 3: Buscar por patrones específicos de la página
            if not leads:
                leads = self._extract_by_patterns(soup, max_leads)
            
            logger.info(f"🎯 Total leads extraídos: {len(leads)}")
            return leads
            
        except Exception as e:
            logger.error(f"❌ Error en scraping: {e}")
            return []

    def _extract_from_business_row(self, row) -> Optional[Dict]:
        """Extraer de fila de negocio"""
        try:
            # Buscar celdas de la fila
            cells = row.find_all(['td', 'th'])
            if len(cells) < 2:
                return None
            
            row_text = row.get_text(strip=True)
            
            # Skip filas de encabezado o navegación
            skip_keywords = ['nombre', 'estatus', 'acciones', 'encuentra los mejores', 'buscar']
            if any(keyword in row_text.lower() for keyword in skip_keywords):
                return None
            
            # Buscar nombre de empresa en la primera celda significativa
            name = None
            for cell in cells:
                cell_text = cell.get_text(strip=True)
                # Buscar texto que parezca nombre de empresa
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
                    'address': address,
                    'sector': 'Marketing/Publicidad',
                    'location': 'México, DF',
                    'source': 'seccion_amarilla',
                    'credit_potential': 'ALTO',
                    'estimated_revenue': '$200,000 - $500,000',
                    'loan_range': '$50,000 - $1,200,000',
                    'extracted_at': datetime.now().isoformat(),
                    'debug_results_type': '<class "business_row">'
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error extrayendo de fila: {e}")
            return None

    def _extract_from_phone_context(self, phone_element, soup) -> Optional[Dict]:
        """Extraer información del contexto del teléfono"""
        try:
            # Encontrar el contenedor padre del teléfono
            parent = phone_element.parent
            while parent and parent.name != 'tr':
                parent = parent.parent
                if parent is None:
                    break
            
            if not parent:
                return None
            
            # Extraer información de la fila/contenedor
            container_text = parent.get_text(strip=True)
            phone = self._extract_phone(container_text)
            
            # Buscar nombre en elementos hermanos o padres
            name = self._find_business_name_near_phone(parent)
            
            if name and phone:
                return {
                    'name': name,
                    'phone': phone,
                    'email': None,
                    'address': container_text[:100],
                    'sector': 'Marketing/Publicidad',
                    'location': 'México, DF',
                    'source': 'seccion_amarilla',
                    'credit_potential': 'ALTO',
                    'estimated_revenue': '$200,000 - $500,000',
                    'loan_range': '$50,000 - $1,200,000',
                    'extracted_at': datetime.now().isoformat(),
                    'debug_results_type': '<class "phone_context">'
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error extrayendo de contexto: {e}")
            return None

    def _extract_by_patterns(self, soup, max_leads: int) -> List[Dict]:
        """Extraer por patrones específicos de la página"""
        leads = []
        
        try:
            # Buscar todos los enlaces de teléfono
            phone_links = soup.find_all('a', href=re.compile(r'tel:'))
            
            for link in phone_links[:max_leads]:
                phone = link.get('href').replace('tel:', '').strip()
                
                # Buscar el nombre en el contexto del enlace
                container = link.find_parent(['tr', 'div', 'td'])
                if container:
                    name = self._find_business_name_in_container(container)
                    
                    if name and phone:
                        leads.append({
                            'name': name,
                            'phone': phone,
                            'email': None,
                            'address': container.get_text(strip=True)[:100],
                            'sector': 'Marketing/Publicidad',
                            'location': 'México, DF',
                            'source': 'seccion_amarilla',
                            'credit_potential': 'ALTO',
                            'estimated_revenue': '$200,000 - $500,000',
                            'loan_range': '$50,000 - $1,200,000',
                            'extracted_at': datetime.now().isoformat(),
                            'debug_results_type': '<class "phone_link_pattern">'
                        })
            
        except Exception as e:
            logger.error(f"Error en extracción por patrones: {e}")
        
        return leads

    def _find_business_name_near_phone(self, container) -> Optional[str]:
        """Encontrar nombre de negocio cerca del teléfono"""
        try:
            # Buscar en el mismo contenedor
            text_nodes = container.find_all(text=True)
            
            for text in text_nodes:
                text = text.strip()
                # Buscar texto que parezca nombre de empresa
                if (len(text) > 3 and 
                    not re.match(r'^\(\d+\)', text) and
                    not text.lower() in ['abierto', 'cerrado', 'whatsapp'] and
                    not text.startswith('AV.') and
                    not text.startswith('CALLE')):
                    
                    return text[:50]  # Limitar longitud
            
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

    def _extract_phone(self, text: str) -> Optional[str]:
        """Extraer teléfono del texto"""
        # Patrones de teléfono mexicano
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

# Función para compatibilidad
def scrape_seccion_amarilla(url):
    """Función compatible con el sistema existente"""
    scraper = GoogleMapsLeadScraper()
    
    # Ejecutar scraping síncrono
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        if 'marketing' in url:
            sector = 'agencias de marketing'
            location = 'distrito federal'
        else:
            sector = 'empresas'
            location = 'méxico'
        
        results = loop.run_until_complete(scraper.scrape_leads(sector, location, 10))
        return results
    finally:
        loop.close()
