#!/usr/bin/env python3
"""
Google Maps Lead Scraper - VERSIÓN REAL
Scraper funcional para encontrar PyMEs en Google Maps
"""

import asyncio
import time
import random
from typing import List, Dict, Optional
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.service import Service
import logging
from datetime import datetime
import re
import os

logger = logging.getLogger(__name__)

class GoogleMapsLeadScraper:
    def __init__(self):
        self.driver = None
        self.leads = []
        
        # Sectores específicos para créditos PyME
        self.sector_terms = {
            "Restaurantes": [
                "restaurante", "taquería", "fonda", "comida casera",
                "cocina económica", "antojitos mexicanos"
            ],
            "Talleres": [
                "taller mecánico", "hojalatería", "vulcanizadora",
                "taller eléctrico", "refacciones automotrices"
            ],
            "Comercio": [
                "tienda de ropa", "zapatería", "ferretería", "papelería",
                "tienda de abarrotes", "boutique"
            ],
            "Servicios": [
                "estética", "salón de belleza", "barbería", "consultorio",
                "farmacia independiente"
            ],
            "Producción": [
                "panadería", "tortillería", "carnicería", "frutería",
                "pastelería artesanal"
            ]
        }

    def setup_driver(self, headless: bool = True):
        """Configura Chrome driver optimizado"""
        try:
            options = Options()
            
            # Configuraciones básicas
            if headless:
                options.add_argument("--headless")
            
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-plugins")
            options.add_argument("--disable-images")
            options.add_argument("--disable-javascript")
            
            # User agent
            options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            
            # Usar Chrome del sistema
            options.binary_location = "/usr/bin/google-chrome"
            
            # Crear driver
            self.driver = webdriver.Chrome(options=options)
            self.driver.set_page_load_timeout(30)
            
            logger.info("✅ Chrome driver configurado exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error configurando driver: {e}")
            return False

    def test_connection(self) -> bool:
        """Testa la conexión"""
        try:
            if not self.setup_driver(headless=True):
                return False
            
            self.driver.get("https://www.google.com")
            time.sleep(2)
            
            title = self.driver.title
            self.driver.quit()
            
            return "google" in title.lower()
            
        except Exception as e:
            logger.error(f"Test connection failed: {e}")
            if self.driver:
                self.driver.quit()
            return False

    async def test_single_search(self, sector: str, location: str, max_results: int = 1) -> List[Dict]:
        """Test con una búsqueda real"""
        try:
            if not self.setup_driver():
                return []
            
            # Buscar en Google Maps
            query = f"{sector} en {location}"
            results = await self._search_google_maps(query, max_results)
            
            self.driver.quit()
            return results
            
        except Exception as e:
            logger.error(f"Test search failed: {e}")
            if self.driver:
                self.driver.quit()
            return []

    async def scrape_leads(self, sector: str, location: str, max_leads: int = 10) -> List[Dict]:
        """Scraping principal REAL"""
        try:
            logger.info(f"🎯 Iniciando scraping REAL: {sector} en {location}")
            
            if not self.setup_driver():
                return []
            
            all_leads = []
            terms = self.sector_terms.get(sector, [sector])
            
            # Probar 2 términos máximo
            for term in terms[:2]:
                search_query = f"{term} {location}"
                
                logger.info(f"🔍 Buscando: {search_query}")
                
                leads_batch = await self._search_google_maps(search_query, max_leads // 2)
                all_leads.extend(leads_batch)
                
                # Pausa entre búsquedas
                await asyncio.sleep(random.uniform(5, 10))
                
                if len(all_leads) >= max_leads:
                    break
            
            self.driver.quit()
            
            # Filtrar y procesar leads
            filtered_leads = self._process_leads(all_leads, sector, location)
            
            logger.info(f"✅ Scraping completado: {len(filtered_leads)} leads")
            return filtered_leads[:max_leads]
            
        except Exception as e:
            logger.error(f"❌ Error en scraping: {e}")
            if self.driver:
                self.driver.quit()
            return []

    async def _search_google_maps(self, query: str, max_results: int) -> List[Dict]:
        """Busca en Google Maps"""
        try:
            # Ir a Google Maps
            self.driver.get("https://www.google.com/maps")
            await asyncio.sleep(3)
            
            # Buscar caja de búsqueda
            search_box = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "searchboxinput"))
            )
            
            search_box.clear()
            search_box.send_keys(query)
            
            # Hacer búsqueda
            search_button = self.driver.find_element(By.ID, "searchbox-searchbutton")
            search_button.click()
            
            await asyncio.sleep(5)
            
            # Extraer resultados
            results = await self._extract_business_results(max_results)
            
            return results
            
        except Exception as e:
            logger.error(f"Error en búsqueda Maps: {e}")
            return []

    async def _extract_business_results(self, max_results: int) -> List[Dict]:
        """Extrae resultados de negocios"""
        businesses = []
        
        try:
            # Esperar resultados
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '[role="main"]'))
            )
            
            # Scroll para cargar más resultados
            for i in range(3):
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                await asyncio.sleep(2)
            
            # Buscar elementos de negocios (múltiples selectores)
            business_selectors = [
                '[data-result-index]',
                '[role="article"]',
                '.Nv2PK',
                '[jsaction*="mouseover"]'
            ]
            
            business_elements = []
            for selector in business_selectors:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    business_elements = elements
                    break
            
            logger.info(f"📊 Encontrados {len(business_elements)} elementos")
            
            for i, element in enumerate(business_elements[:max_results]):
                try:
                    business_data = await self._extract_business_info(element, i)
                    
                    if business_data:
                        businesses.append(business_data)
                        logger.info(f"✅ Negocio {i+1}: {business_data.get('name', 'Sin nombre')}")
                    
                    await asyncio.sleep(random.uniform(1, 3))
                    
                except Exception as e:
                    logger.warning(f"Error extrayendo negocio {i+1}: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Error general en extracción: {e}")
        
        return businesses

    async def _extract_business_info(self, element, index: int) -> Optional[Dict]:
        """Extrae información de un negocio"""
        try:
            # Obtener texto completo del elemento
            business_text = element.get_text()
            
            if len(business_text) < 10:
                return None
            
            # Extraer información básica
            business = {
                'extracted_at': datetime.now().isoformat(),
                'source': 'Google Maps'
            }
            
            # Extraer nombre (primera línea que no sea muy corta)
            lines = business_text.split('\n')
            for line in lines[:5]:
                line = line.strip()
                if 5 < len(line) < 100 and not line.isdigit():
                    business['name'] = line
                    break
            
            # Buscar teléfono
            phone_match = re.search(r'\+?[\d\s\-\(\)]{10,15}', business_text)
            if phone_match:
                business['phone'] = phone_match.group().strip()
            
            # Buscar rating
            rating_match = re.search(r'(\d+[.,]\d+)\s*★', business_text)
            if rating_match:
                business['rating'] = float(rating_match.group(1).replace(',', '.'))
            
            # Buscar dirección (líneas que contengan números)
            for line in lines:
                if re.search(r'\d+.*(?:calle|av|avenida|blvd|col)', line.lower()):
                    business['address'] = line.strip()
                    break
            
            return business if business.get('name') else None
            
        except Exception as e:
            logger.warning(f"Error extrayendo info del negocio: {e}")
            return None

    def _process_leads(self, leads: List[Dict], sector: str, location: str) -> List[Dict]:
        """Procesa y enriquece leads"""
        processed = []
        
        for lead in leads:
            try:
                # Enriquecer información
                enhanced_lead = {
                    'name': lead.get('name', f'Negocio {sector}'),
                    'phone': lead.get('phone', ''),
                    'email': '',  # No disponible en Maps
                    'address': lead.get('address', ''),
                    'sector': sector,
                    'location': location,
                    'source': 'Google Maps',
                    'rating': lead.get('rating', 0),
                    'credit_potential': self._calculate_credit_potential(lead, sector),
                    'extracted_at': lead.get('extracted_at', datetime.now().isoformat())
                }
                
                processed.append(enhanced_lead)
                
            except Exception as e:
                logger.warning(f"Error procesando lead: {e}")
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
        
        # Score por rating
        rating = business.get('rating', 0)
        if rating >= 4.0:
            score += 2
        elif rating >= 3.5:
            score += 1
        
        # Score por información disponible
        if business.get('phone'):
            score += 1
        if business.get('address'):
            score += 1
        
        # Determinar potencial
        if score >= 5:
            return "ALTO"
        elif score >= 3:
            return "MEDIO"
        else:
            return "BAJO"

    def __del__(self):
        """Cleanup"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
