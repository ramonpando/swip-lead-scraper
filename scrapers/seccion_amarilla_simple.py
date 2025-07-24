#!/usr/bin/env python3
"""
Scraper completo funcional para Sección Amarilla
CON ESTRUCTURA HTML CORRECTA: span itemprop="name" + small class="short_address"
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
    """Scraper funcional con estructura HTML correcta"""
    
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
        """Extraer información - CORRECTO: span itemprop='name' = NOMBRE, small short_address = DIRECCIÓN"""
        try:
            name = None
            address = None
            phone = None
            
                      # — MÉTODO 1: Nombre desde el <a> dentro de <p class="bussines_name">
            name_tag = row.select_one('p.bussines_name a')
            if name_tag:
                name = name_tag.get_text(strip=True)
                logger.info(f"🏷️ Nombre extraído de <a>: {name}")

            # — MÉTODO 2: Fallback si no hay <a>
            if not name:
                for tag in ['h1','h2','h3','h4','strong','b']:
                    elem = row.find(tag)
                    if elem:
                        txt = elem.get_text(strip=True)
                        if len(txt) > 3 and not any(skip in txt.lower() for skip in ['abierto','cerrado','acciones']):
                            name = txt
                            logger.info(f"🎯 Nombre fallback: {name}")
                            break

            # — DIRECCIÓN desde el <small.short_address> visible
            address = None
            address_tag = row.select_one('small.short_address:not(.nodisplay)')
            if address_tag:
                address = address_tag.get_text(strip=True)
                logger.info(f"📍 Dirección extraída: {address}")

            # — TELÉFONO (método robusto existente)
            phone = self._extract_phone_robust(row)

            
            # VALIDACIÓN FINAL
            if name and phone and len(name) > 3 and phone != "#ERROR!":
                # Limpiar nombre (muy ligero)
                name = name.strip()
                
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

    def _looks_like_address(self, text: str) -> bool:
        """Determinar si un texto parece dirección"""
        if not text or len(text) < 10:
            return False
        
        # Indicadores que es dirección
        address_indicators = [
            'CALLE', 'AV.', 'AVENIDA', 'BLVD', 'COL.', 'COLONIA',
            'NO.', 'NUM.', '#', 'MZ', 'LT', 'C.P.',
            'GUADALUPE', 'VICTORIA', 'CUAUHTEMOC', 'BENITO',
            'RIO TIBER', 'ZONA METROPOLITANA', 'DISTRITO FEDERAL', 'MEXICO'
        ]
        
        text_upper = text.upper()
        
        # Si contiene 2+ indicadores, es dirección
        indicator_count = sum(1 for indicator in address_indicators if indicator in text_upper)
        
        if indicator_count >= 2:
            return True
        
        # Si tiene números + palabras de ubicación
        if re.search(r'\d+', text) and any(loc in text_upper for loc in ['MEXICO', 'DF', 'CDMX', 'CUAUHTEMOC']):
            return True
        
        # Si empieza con indicadores de dirección
        street_starters = ['GUADALUPE', 'RIO TIBER', 'AV.', 'CALLE', 'BLVD']
        if any(text_upper.startswith(starter) for starter in street_starters):
            return True
        
        return False

    def _extract_phone_robust(self, row) -> Optional[str]:
        """Extraer teléfono de forma robusta"""
        try:
            # MÉTODO 1: Enlaces tel: (más confiable)
            phone_links = row.find_all('a', href=re.compile(r'tel:'))
            for link in phone_links:
                phone_raw = link.get('href').replace('tel:', '').strip()
                if phone_raw and len(phone_raw) >= 10:
                    return self._format_phone(phone_raw)
            
            # MÉTODO 2: Buscar en botones con clase de teléfono
            phone_buttons = row.find_all(['button', 'span', 'div'], class_=re.compile(r'phone|tel', re.I))
            for button in phone_buttons:
                button_text = button.get_text(strip=True)
                phone = self._extract_phone_simple(button_text)
                if phone:
                    return phone
            
            # MÉTODO 3: Buscar en texto general de la fila
            row_text = row.get_text()
            phone = self._extract_phone_simple(row_text)
            if phone:
                return phone
            
            return None
            
        except Exception as e:
            logger.error(f"Error extrayendo teléfono: {e}")
            return None

    def _extract_phone_simple(self, text: str) -> Optional[str]:
        """Extraer teléfono del texto"""
        if not text:
            return None
        
        # Patrones para teléfonos mexicanos
        patterns = [
            r'\(\d{2,3}\)\s*\d{3,4}[-\s]?\d{4}',      # (55)1234-5678
            r'\d{2,3}[-\s]\d{3,4}[-\s]\d{4}',         # 55-1234-5678
            r'\b\d{10}\b',                             # 5512345678
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                # Verificar que no esté cerca de indicadores de dirección
                match_pos = text.find(match)
                context = text[max(0, match_pos-15):match_pos+len(match)+15]
                
                if not re.search(r'(MZ|LT|NO\.|NUM\.|C\.P\.|CALLE|AV\.)', context, re.IGNORECASE):
                    return self._format_phone(match)
        
        return None

    def _format_phone(self, phone: str) -> str:
        """Formatear teléfono"""
        try:
            # Solo números
            numbers_only = re.sub(r'[^\d]', '', phone)
            
            # Formatear según longitud
            if len(numbers_only) == 10:
                return f"({numbers_only[:2]}){numbers_only[2:6]}-{numbers_only[6:]}"
            elif len(numbers_only) == 11:
                return f"({numbers_only[:3]}){numbers_only[3:7]}-{numbers_only[7:]}"
            else:
                return numbers_only if len(numbers_only) >= 10 else phone
            
        except Exception as e:
            return phone

    def _extract_from_phone_link(self, link, soup, sector: str) -> Optional[Dict]:
        """Extraer información del enlace de teléfono"""
        try:
            phone = link.get('href').replace('tel:', '').strip()
            phone = self._format_phone(phone)
            
            container = link.find_parent(['tr', 'div', 'td'])
            if container:
                # Buscar nombre en span itemprop="name" dentro del container
                name_span = container.find('span', {'itemprop': 'name'})
                if name_span:
                    name = name_span.get_text(strip=True)
                else:
                    name = self._find_business_name_in_container(container)
                
                if name and phone:
                    return {
                        'name': name.strip(),
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
