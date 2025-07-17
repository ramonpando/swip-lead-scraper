#!/usr/bin/env python3
"""
Scraper simple para Sección Amarilla
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
    """Mantener compatibilidad con nombre original"""
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
            
            # Construir URL de Sección Amarilla
            base_url = "https://www.seccionamarilla.com.mx/resultados"
            # Limpiar y formatear términos
            sector_clean = sector.replace(' ', '-').lower()
            location_clean = location.replace(' ', '-').lower()
            
            # URL de prueba por defecto
            if "marketing" in sector.lower():
                url = "https://www.seccionamarilla.com.mx/resultados/agencias-de-marketing/distrito-federal/zona-metropolitana/1"
            else:
                url = f"{base_url}/{sector_clean}/{location_clean}/1"
            
            logger.info(f"📍 URL a scrapear: {url}")
            
            # Hacer request
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            leads = []
            
            # Estrategia 1: Buscar en contenedores div
            containers = soup.find_all('div', class_='container_out')
            logger.info(f"📦 Containers encontrados: {len(containers)}")
            
            if not containers:
                # Estrategia 2: Buscar otros contenedores
                containers = soup.find_all('div', class_=['result-item', 'listing', 'business-info'])
                logger.info(f"📦 Containers alternativos: {len(containers)}")
            
            if not containers:
                # Estrategia 3: Buscar en tabla
                table_rows = soup.find_all('tr')
                logger.info(f"📋 Filas de tabla: {len(table_rows)}")
                
                for row in table_rows[:max_leads]:
                    lead = self._extract_from_table_row(row)
                    if lead:
                        leads.append(lead)
            
            # Procesar containers
            for i, container in enumerate(containers[:max_leads]):
                try:
                    lead = self._extract_lead_from_container(container, i)
                    if lead:
                        leads.append(lead)
                        logger.info(f"✅ Lead extraído: {lead.get('name', 'Sin nombre')}")
                    
                    # Delay entre extracciones
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"❌ Error extrayendo lead {i}: {e}")
                    continue
            
            # Si no hay leads, intentar extracción general
            if not leads:
                leads = self._extract_general_info(soup, max_leads)
            
            logger.info(f"🎯 Total leads extraídos: {len(leads)}")
            return leads
            
        except Exception as e:
            logger.error(f"❌ Error en scraping: {e}")
            return []

    def _extract_lead_from_container(self, container, index: int) -> Optional[Dict]:
        """Extraer información de un contenedor"""
        try:
            text = container.get_text(strip=True)
            
            # Buscar nombre
            name = self._extract_name(container, text)
            
            # Buscar teléfono
            phone = self._extract_phone(text)
            
            # Buscar email
            email = self._extract_email(text)
            
            # Buscar dirección
            address = self._extract_address(container, text)
            
            # Solo devolver si tenemos al menos nombre y contacto
            if name and (phone or email):
                return {
                    'name': name,
                    'phone': phone,
                    'email': email,
                    'address': address,
                    'sector': 'Sección Amarilla',
                    'location': 'México',
                    'source': 'seccion_amarilla',
                    'credit_potential': 'ALTO',
                    'estimated_revenue': '$200,000 - $500,000',
                    'loan_range': '$50,000 - $1,200,000',
                    'extracted_at': datetime.now().isoformat(),
                    'debug_results_type': f'<class "container_{index}">'
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error extrayendo de contenedor: {e}")
            return None

    def _extract_from_table_row(self, row) -> Optional[Dict]:
        """Extraer información de fila de tabla"""
        try:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 2:
                return None
            
            text = row.get_text(strip=True)
            
            # Buscar información
            name = None
            phone = self._extract_phone(text)
            email = self._extract_email(text)
            
            # Buscar nombre en las celdas
            for cell in cells:
                cell_text = cell.get_text(strip=True)
                if len(cell_text) > 5 and not cell_text.lower() in ['abierto', 'cerrado', 'compartir', 'ruta']:
                    name = cell_text[:100]
                    break
            
            if name and (phone or email):
                return {
                    'name': name,
                    'phone': phone,
                    'email': email,
                    'address': text[:200],
                    'sector': 'Sección Amarilla',
                    'location': 'México',
                    'source': 'seccion_amarilla',
                    'credit_potential': 'ALTO',
                    'estimated_revenue': '$200,000 - $500,000',
                    'loan_range': '$50,000 - $1,200,000',
                    'extracted_at': datetime.now().isoformat(),
                    'debug_results_type': '<class "table_row">'
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error extrayendo de tabla: {e}")
            return None

    def _extract_general_info(self, soup, max_leads: int) -> List[Dict]:
        """Extracción general cuando fallan otras estrategias"""
        leads = []
        
        try:
            # Buscar todos los textos que parezcan nombres de empresas
            all_text = soup.get_text()
            
            # Buscar patrones de teléfono
            phone_pattern = r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b|\b\(\d{3}\)\s*\d{3}[-.]?\d{4}\b'
            phones = re.findall(phone_pattern, all_text)
            
            # Buscar patrones de email
            email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            emails = re.findall(email_pattern, all_text)
            
            # Crear leads básicos con la información encontrada
            for i, phone in enumerate(phones[:max_leads]):
                if phone and not phone.startswith('998'):  # Filtrar teléfonos de ejemplo
                    leads.append({
                        'name': f'Empresa {i+1}',
                        'phone': phone,
                        'email': emails[i] if i < len(emails) else None,
                        'address': 'México, DF',
                        'sector': 'Sección Amarilla',
                        'location': 'México',
                        'source': 'seccion_amarilla',
                        'credit_potential': 'MEDIO',
                        'estimated_revenue': '$100,000 - $300,000',
                        'loan_range': '$25,000 - $800,000',
                        'extracted_at': datetime.now().isoformat(),
                        'debug_results_type': '<class "general_extraction">'
                    })
            
        except Exception as e:
            logger.error(f"Error en extracción general: {e}")
        
        return leads

    def _extract_name(self, container, text: str) -> Optional[str]:
        """Extraer nombre de empresa"""
        # Buscar en títulos
        title_elements = container.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'a'])
        for elem in title_elements:
            elem_text = elem.get_text(strip=True)
            if len(elem_text) > 3 and not elem_text.lower() in ['abierto', 'cerrado', 'compartir', 'ruta', 'más información']:
                return elem_text[:100]
        
        # Buscar en primera línea significativa
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        for line in lines[:3]:  # Revisar primeras 3 líneas
            if len(line) > 3 and not line.lower() in ['abierto', 'cerrado', 'compartir', 'ruta']:
                return line[:100]
        
        return None

    def _extract_phone(self, text: str) -> Optional[str]:
        """Extraer teléfono"""
        phone_pattern = r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b|\b\(\d{3}\)\s*\d{3}[-.]?\d{4}\b'
        match = re.search(phone_pattern, text)
        return match.group() if match else None

    def _extract_email(self, text: str) -> Optional[str]:
        """Extraer email"""
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        match = re.search(email_pattern, text)
        return match.group() if match else None

    def _extract_address(self, container, text: str) -> Optional[str]:
        """Extraer dirección"""
        # Buscar patrones de dirección
        address_indicators = ['c.p.', 'cp', 'col.', 'colonia', 'df', 'cdmx', 'méxico']
        lines = text.split('\n')
        
        for line in lines:
            line_lower = line.lower()
            if any(indicator in line_lower for indicator in address_indicators):
                return line.strip()[:200]
        
        return text[:100] if text else None

# Función para compatibilidad
def scrape_seccion_amarilla(url):
    """Función compatible con el sistema existente"""
    scraper = GoogleMapsLeadScraper()
    
    # Ejecutar scraping síncrono
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        # Extraer parámetros de la URL
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
