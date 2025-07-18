#!/usr/bin/env python3
"""
Scraper completo funcional para Sección Amarilla
CON PAGINACIÓN AUTOMÁTICA Y FIX ESPECÍFICO PARA ESTRUCTURA HTML
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
    """Scraper funcional para Sección Amarilla con estructura específica"""
    
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

    async def scrape_leads(self, sector: str, location: str, max_leads: int = 10) -> List[Dict]:
        try:
            logger.info(f"🔥 Iniciando scraping: {sector} en {location}")
            logger.info(f"🎯 Objetivo: {max_leads} leads")
            
            # CALCULAR PÁGINA AUTOMÁTICAMENTE BASADO EN TIEMPO
            now = datetime.now()
            
            # Rotación cada 2 horas: página 1-10
            hours_since_start = now.hour + (now.day * 24)
            page_number = (hours_since_start // 2) % 10 + 1
            
            logger.info(f"🕐 Hora actual: {now.hour}:00")
            logger.info(f"📄 Página calculada automáticamente: {page_number}")
            
            # CONSTRUIR URL CON PÁGINA DINÁMICA
            if "contadores" in sector.lower() or "contador" in sector.lower():
                base_url = "https://www.seccionamarilla.com.mx/resultados/contadores/distrito-federal/zona-metropolitana"
                url = f"{base_url}/{page_number}"
                logger.info("📊 CATEGORÍA: Contadores")
                
            elif "abogados" in sector.lower() or "abogado" in sector.lower():
                base_url = "https://www.seccionamarilla.com.mx/resultados/abogados/distrito-federal/zona-metropolitana"
                url = f"{base_url}/{page_number}"
                logger.info("⚖️ CATEGORÍA: Abogados")
                
            elif "arquitectos" in sector.lower() or "arquitecto" in sector.lower():
                base_url = "https://www.seccionamarilla.com.mx/resultados/arquitectos/distrito-federal/zona-metropolitana"
                url = f"{base_url}/{page_number}"
                logger.info("🏗️ CATEGORÍA: Arquitectos")
                
            elif "medicos" in sector.lower() or "medico" in sector.lower():
                base_url = "https://www.seccionamarilla.com.mx/resultados/medicos/distrito-federal/zona-metropolitana"
                url = f"{base_url}/{page_number}"
                logger.info("👨‍⚕️ CATEGORÍA: Médicos")
                
            elif "dentistas" in sector.lower() or "dentista" in sector.lower():
                base_url = "https://www.seccionamarilla.com.mx/resultados/dentistas/distrito-federal/zona-metropolitana"
                url = f"{base_url}/{page_number}"
                logger.info("🦷 CATEGORÍA: Dentistas")
                
            elif "ingenieros" in sector.lower() or "ingeniero" in sector.lower():
                base_url = "https://www.seccionamarilla.com.mx/resultados/ingenieros/distrito-federal/zona-metropolitana"
                url = f"{base_url}/{page_number}"
                logger.info("🔧 CATEGORÍA: Ingenieros")
                
            elif "consultores" in sector.lower() or "consultor" in sector.lower():
                base_url = "https://www.seccionamarilla.com.mx/resultados/consultores/distrito-federal/zona-metropolitana"
                url = f"{base_url}/{page_number}"
                logger.info("💼 CATEGORÍA: Consultores")
                
            elif "publicidad" in sector.lower():
                base_url = "https://www.seccionamarilla.com.mx/resultados/agencias-de-publicidad/distrito-federal/zona-metropolitana"
                url = f"{base_url}/{page_number}"
                logger.info("📢 CATEGORÍA: Publicidad")
                
            # DEFAULT: Marketing
            else:
                base_url = "https://www.seccionamarilla.com.mx/resultados/agencias-de-marketing/distrito-federal/zona-metropolitana"
                url = f"{base_url}/{page_number}"
                logger.info("🎯 CATEGORÍA: Marketing (Default)")
            
            logger.info(f"📍 URL con paginación: {url}")
            
            # Ejecutar scraping
            leads = await self.scrape_leads_from_url(url, max_leads)
            
            # Log adicional para tracking
            logger.info(f"📊 RESUMEN: Página {page_number} de {sector} = {len(leads)} leads")
            
            return leads
            
        except Exception as e:
            logger.error(f"❌ Error en scraping: {e}")
            return []

    async def scrape_leads_from_url(self, url: str, max_leads: int = 10) -> List[Dict]:
        """Scrapear desde URL específica"""
        try:
            logger.info(f"🔥 Scraping URL específica: {url}")
            
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            leads = []
            
            sector = self._extract_sector_from_url(url)
            
            business_rows = soup.find_all('tr')
            logger.info(f"📋 Filas encontradas: {len(business_rows)}")
            
            for row in business_rows:
                lead = self._extract_from_business_row(row, sector)
                if lead and len(leads) < max_leads:
                    lead_id = f"{lead.get('name', '')}-{lead.get('phone', '')}"
                    if lead_id not in self.extracted_leads:
                        self.extracted_leads.add(lead_id)
                        leads.append(lead)
                        logger.info(f"✅ Lead extraído: {lead.get('name', 'Sin nombre')}")
            
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
        """Extraer sector de la URL"""
        if 'contadores' in url.lower():
            return 'Contadores'
        elif 'abogados' in url.lower():
            return 'Abogados'
        elif 'arquitectos' in url.lower():
            return 'Arquitectos'
        elif 'medicos' in url.lower():
            return 'Médicos'
        elif 'dentistas' in url.lower():
            return 'Dentistas'
        elif 'ingenieros' in url.lower():
            return 'Ingenieros'
        elif 'consultores' in url.lower():
            return 'Consultores'
        elif 'publicidad' in url.lower():
            return 'Publicidad'
        elif 'marketing' in url.lower():
            return 'Marketing/Publicidad'
        else:
            return 'Servicios Profesionales'

    def _extract_from_business_row(self, row, sector: str) -> Optional[Dict]:
        """Extraer información - ESPECÍFICO para estructura Sección Amarilla"""
        try:
            # Buscar estructura específica: título + dirección + teléfono
            name = None
            address = None
            phone = None
            
            # MÉTODO 1: Buscar por jerarquía de elementos HTML
            # Buscar título principal (usualmente H2, H3 o elemento con clase específica)
            title_elements = row.find_all(['h1', 'h2', 'h3', 'h4'])
            for title in title_elements:
                title_text = title.get_text(strip=True)
                if len(title_text) > 3 and not any(skip in title_text.lower() for skip in ['abierto', 'cerrado', 'ubicación']):
                    name = title_text
                    break
            
            # MÉTODO 2: Si no hay títulos, buscar por estructura de celdas
            if not name:
                cells = row.find_all(['td', 'th'])
                
                for i, cell in enumerate(cells):
                    cell_text = cell.get_text(strip=True)
                    
                    # Skip celdas vacías
                    if len(cell_text) < 4:
                        continue
                    
                    # Skip teléfonos directos
                    if re.match(r'^\(\d+\)', cell_text) or re.match(r'^\d{10}', cell_text):
                        continue
                    
                    # DETECTAR ESTRUCTURA: Primera línea = nombre, Segunda línea = dirección
                    lines = cell_text.split('\n')
                    if len(lines) >= 2:
                        # Primera línea = nombre de empresa
                        potential_name = lines[0].strip()
                        potential_address = lines[1].strip()
                        
                        # Validar que la primera línea es nombre (no dirección)
                        if not self._is_address_line(potential_name) and len(potential_name) > 3:
                            name = potential_name
                            if self._is_address_line(potential_address):
                                address = potential_address
                            break
                    
                    # MÉTODO ALTERNATIVO: Si no hay saltos de línea, buscar patrón nombre + dirección
                    elif not name:
                        clean_name = self._extract_name_from_mixed_text(cell_text)
                        if clean_name:
                            name = clean_name
                            # El resto sería dirección
                            remaining = cell_text.replace(clean_name, '').strip()
                            if remaining and self._is_address_line(remaining):
                                address = remaining
            
            # Buscar teléfono en enlaces tel: o en texto
            phone_links = row.find_all('a', href=re.compile(r'tel:'))
            if phone_links:
                phone = phone_links[0].get('href').replace('tel:', '').strip()
            else:
                # Buscar teléfono en texto general
                row_text = row.get_text()
                phone = self._extract_phone(row_text)
            
            # Si no encontramos dirección separada, buscarla en toda la fila
            if not address:
                address = self._extract_address_from_row(row)
            
            # Validar que tenemos datos mínimos
            if name and phone and len(name) > 3:
                return {
                    'name': name,
                    'phone': phone,
                    'email': None,
                    'address': address or "México, DF",
                    'sector': sector,
                    'location': 'México, DF',
                    'source': 'seccion_amarilla',
                    'credit_potential': self._assess_credit_potential(sector),
                    'estimated_revenue': self._estimate_revenue(sector),
                    'loan_range': self._estimate_loan_range(sector),
                    'extracted_at': datetime.now().isoformat(),
                    'debug_results_type': f'<class "business_row_{sector}">'
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error extrayendo de fila: {e}")
            return None

    def _is_address_line(self, text: str) -> bool:
        """Detectar si una línea es dirección"""
        address_indicators = [
            'MZ', 'LT', 'CALLE', 'AV.', 'AVENIDA', 'COL.', 'COLONIA',
            'NO.', 'NUM.', '#', 'BLVD', 'BOULEVARD', 'C.P.',
            'GUADALUPE', 'VICTORIA', 'CUAUHTEMOC', 'IZTLAHUACAN',
            'BARRANCA', 'BELLAVISTA', 'VIVEROS', 'ROSEDAL',
            'CONJUNTO', 'RESIDENCIAL', 'FRACCIONAMIENTO', 'MONTEBELLO'
        ]
        
        text_upper = text.upper()
        
        # Si contiene múltiples indicadores, probablemente es dirección
        indicator_count = sum(1 for indicator in address_indicators if indicator in text_upper)
        
        # Si tiene 2+ indicadores o patrones específicos
        if indicator_count >= 2:
            return True
        
        # Patrones específicos de direcciones mexicanas
        patterns = [
            r'\bMZ\s+\d+',  # MZ 8
            r'\bLT\s+\d+',  # LT 20
            r'\b\d+\s+(NO\.|NUM\.)',  # 661 NO.
            r'\bC\.P\.\s*\d+',  # C.P. 12345
        ]
        
        for pattern in patterns:
            if re.search(pattern, text_upper):
                return True
        
        return False

    def _extract_name_from_mixed_text(self, text: str) -> Optional[str]:
        """Extraer nombre cuando está mezclado con dirección"""
        try:
            # Buscar donde empieza la dirección
            address_start_patterns = [
                r'\b(GUADALUPE|VICTORIA|CUAUHTEMOC|IZTLAHUACAN)',
                r'\b(BARRANCA|BELLAVISTA|VIVEROS|ROSEDAL)',
                r'\b(RIO|TIBER|MONTEBELLO|CONJUNTO)',
                r'\bMZ\s+\d+',
                r'\bLT\s+\d+',
                r'\b\d+\s+(NO\.|NUM\.)',
                r'\bCOL\.',
                r'\bAV\.',
                r'\bCALLE\s+\w+'
            ]
            
            earliest_pos = len(text)
            
            for pattern in address_start_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match and match.start() < earliest_pos:
                    earliest_pos = match.start()
            
            if earliest_pos < len(text):
                name = text[:earliest_pos].strip()
                # Limpiar caracteres finales
                name = re.sub(r'[,\-\s]+$', '', name)
                
                if len(name) > 3:
                    return name
            
            return None
            
        except Exception as e:
            return None

    def _extract_from_phone_link(self, link, soup, sector: str) -> Optional[Dict]:
        """Extraer información del enlace de teléfono"""
        try:
            phone = link.get('href').replace('tel:', '').strip()
            
            container = link.find_parent(['tr', 'div', 'td'])
            if container:
                name = self._find_business_name_in_container(container)
                
                if name and phone:
                    # Limpiar nombre si contiene dirección
                    clean_name = self._extract_name_from_mixed_text(name)
                    if clean_name:
                        name = clean_name
                    
                    return {
                        'name': name,
                        'phone': phone,
                        'email': None,
                        'address': "México, DF",
                        'sector': sector,
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
        high_potential = ['Contadores', 'Abogados', 'Arquitectos', 'Médicos', 'Ingenieros']
        medium_high = ['Dentistas', 'Consultores', 'Publicidad']
        
        if sector in high_potential:
            return 'ALTO'
        elif sector in medium_high:
            return 'MEDIO-ALTO'
        else:
            return 'MEDIO'

    def _estimate_revenue(self, sector: str) -> str:
        """Estimar ingresos basado en sector"""
        if sector in ['Contadores', 'Abogados', 'Médicos']:
            return '$500,000 - $1,500,000'
        elif sector in ['Arquitectos', 'Ingenieros']:
            return '$400,000 - $1,200,000'
        elif sector in ['Dentistas', 'Consultores']:
            return '$300,000 - $800,000'
        else:
            return '$200,000 - $600,000'

    def _estimate_loan_range(self, sector: str) -> str:
        """Estimar rango de préstamo basado en sector"""
        if sector in ['Contadores', 'Abogados', 'Médicos']:
            return '$125,000 - $3,750,000'
        elif sector in ['Arquitectos', 'Ingenieros']:
            return '$100,000 - $3,000,000'
        elif sector in ['Dentistas', 'Consultores']:
            return '$75,000 - $2,000,000'
        else:
            return '$50,000 - $1,500,000'

    def _extract_phone(self, text: str) -> Optional[str]:
        """Extraer teléfono del texto"""
        patterns = [
            r'\(\d{2,3}\)\d{3,4}-?\d{4}',
            r'\d{10}',
            r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group()
        
        return None

    def _extract_address_from_row(self, row) -> Optional[str]:
        """Extraer dirección de la fila - MEJORADO"""
        try:
            # Buscar en celdas individuales
            cells = row.find_all(['td', 'th'])
            
            for cell in cells:
                text = cell.get_text(strip=True)
                
                # Si la celda es claramente una dirección
                if self._is_address_line(text):
                    return text[:100]
                
                # Si es texto mixto, extraer solo la parte de dirección
                lines = text.split('\n')
                for line in lines:
                    if self._is_address_line(line.strip()):
                        return line.strip()[:100]
            
            return None
            
        except Exception as e:
            return None

    def _find_business_name_in_container(self, container) -> Optional[str]:
        """Encontrar nombre de negocio en contenedor"""
        try:
            for tag in ['h1', 'h2', 'h3', 'h4', 'strong', 'b']:
                elem = container.find(tag)
                if elem:
                    text = elem.get_text(strip=True)
                    if len(text) > 3:
                        return text[:50]
            
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

def scrape_seccion_amarilla(url):
    """Función compatible con el sistema existente"""
    scraper = GoogleMapsLeadScraper()
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        logger.info(f"🎯 URL recibida: {url}")
        results = loop.run_until_complete(scraper.scrape_leads_from_url(url, 10))
        return results
    finally:
        loop.close()
