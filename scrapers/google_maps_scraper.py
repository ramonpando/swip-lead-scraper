#!/usr/bin/env python3
"""
Google Maps Lead Scraper - SELENIUM BÁSICO REAL
Scraper simple con Selenium para leads PyME reales
"""

import asyncio
import time
import random
from typing import List, Dict, Optional
import logging
from datetime import datetime
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.keys import Keys

logger = logging.getLogger(__name__)

class GoogleMapsLeadScraper:
    def __init__(self):
        self.driver = None
        self.leads = []

    def setup_driver(self):
        """Configura Chrome básico"""
        try:
            # Configurar opciones básicas
            options = Options()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1920,1080')
            
            # User agent realista
            options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            # Crear driver
            self.driver = webdriver.Chrome(options=options)
            self.driver.set_page_load_timeout(30)
            self.driver.implicitly_wait(10)
            
            logger.info("✅ Chrome driver básico configurado")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error configurando driver: {e}")
            return False

    def test_connection(self) -> bool:
        """Testa la conexión"""
        try:
            if not self.setup_driver():
                return False
            
            self.driver.get("https://www.google.com")
            time.sleep(3)
            
            title = self.driver.title
            self.cleanup_driver()
            
            return "google" in title.lower()
            
        except Exception as e:
            logger.error(f"Test connection failed: {e}")
            self.cleanup_driver()
            return False

    async def test_single_search(self, sector: str, location: str, max_results: int = 1) -> List[Dict]:
        """Test con búsqueda real"""
        try:
            if not self.setup_driver():
                return []
            
            query = f"{sector} {location}"
            results = await self._search_google_basic(query, max_results)
            
            self.cleanup_driver()
            return results
            
        except Exception as e:
            logger.error(f"Test search failed: {e}")
            self.cleanup_driver()
            return []

    async def scrape_leads(self, sector: str, location: str, max_leads: int = 10) -> List[Dict]:
        """Scraping principal con Selenium básico"""
        try:
            logger.info(f"🎯 Iniciando scraping REAL: {sector} en {location}")
            
            if not self.setup_driver():
                return []
            
            all_leads = []
            
            # Múltiples consultas para un sector
            queries = [
                f"{sector} {location}",
                f"{sector} {location} teléfono",
                f"{sector} {location} contacto"
            ]
            
            for query in queries:
                try:
                    logger.info(f"🔍 Buscando: {query}")
                    
                    leads_batch = await self._search_google_basic(query, max_leads // 3)
                    
                    if leads_batch:
                        all_leads.extend(leads_batch)
                        logger.info(f"✅ Encontrados {len(leads_batch)} leads")
                    
                    # Pausa entre búsquedas
                    await asyncio.sleep(random.uniform(5, 10))
                    
                    if len(all_leads) >= max_leads:
                        break
                        
                except Exception as e:
                    logger.error(f"❌ Error en query {query}: {e}")
                    continue
            
            self.cleanup_driver()
            
            # Procesar leads
            processed_leads = self._process_leads(all_leads, sector, location)
            
            logger.info(f"✅ Scraping completado: {len(processed_leads)} leads")
            return processed_leads[:max_leads]
            
        except Exception as e:
            logger.error(f"❌ Error general: {e}")
            self.cleanup_driver()
            return []

    async def _search_google_basic(self, query: str, max_results: int) -> List[Dict]:
        """Búsqueda básica en Google"""
        try:
            # Ir a Google
            self.driver.get("https://www.google.com")
            await asyncio.sleep(random.uniform(3, 6))
            
            # Buscar caja de búsqueda
            search_box = self.driver.find_element(By.NAME, "q")
            
            # Escribir query
            search_box.clear()
            search_box.send_keys(query)
            search_box.send_keys(Keys.RETURN)
            
            # Esperar resultados
            await asyncio.sleep(random.uniform(5, 8))
            
            # Extraer resultados
            results = await self._extract_google_results(max_results)
            
            return results
            
        except Exception as e:
            logger.error(f"Error en búsqueda Google: {e}")
            return []

    async def _extract_google_results(self, max_results: int) -> List[Dict]:
    """Extraer resultados de Google"""
    businesses = []
    
    try:
        # DEBUG: Capturar página completa
        page_source = self.driver.page_source
        logger.info(f"🔍 Página contiene: {len(page_source)} caracteres")
        
        # DEBUG: Buscar diferentes selectores
        selectors_to_try = [
            'div.g',
            'div.tF2Cxc',
            'div.MjjYud',
            'div[data-ved]',
            'div.yuRUbf'
        ]
        
        result_elements = []
        for selector in selectors_to_try:
            elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
            if elements:
                logger.info(f"🔍 Selector {selector} encontró {len(elements)} elementos")
                result_elements = elements
                break
        
        if not result_elements:
            logger.warning("❌ No se encontraron elementos con ningún selector")
            return []
        
        logger.info(f"📊 Encontrados {len(result_elements)} resultados")
        
        for i, element in enumerate(result_elements[:max_results * 2]):
            try:
                business_data = await self._extract_business_info(element)
                
                if business_data:
                    businesses.append(business_data)
                    logger.info(f"✅ Negocio {i+1}: {business_data.get('name', 'Sin nombre')}")
                
                if len(businesses) >= max_results:
                    break
                    
            except Exception as e:
                logger.warning(f"Error extrayendo negocio {i+1}: {e}")
                continue
        
    except Exception as e:
        logger.error(f"Error en extracción: {e}")
    
    return businesses

    async def _extract_business_info(self, element) -> Optional[Dict]:
        """Extraer información del negocio"""
        try:
            business = {
                'extracted_at': datetime.now().isoformat(),
                'source': 'Google Search'
            }
            
            # Extraer texto completo
            text_content = element.text
            
            if len(text_content) < 20:
                return None
            
            # Extraer nombre (primer enlace)
            try:
                title_elem = element.find_element(By.CSS_SELECTOR, 'h3')
                business['name'] = title_elem.text.strip()
            except:
                # Intentar otros selectores
                try:
                    link_elem = element.find_element(By.CSS_SELECTOR, 'a')
                    business['name'] = link_elem.text.strip()
                except:
                    pass
            
            # Buscar teléfonos mexicanos
            phone_patterns = [
                r'\b442[-\s]?\d{3}[-\s]?\d{4}\b',  # Querétaro
                r'\b\d{3}[-\s]?\d{3}[-\s]?\d{4}\b',  # General
                r'\(\d{3}\)[-\s]?\d{3}[-\s]?\d{4}',  # Con paréntesis
            ]
            
            for pattern in phone_patterns:
                match = re.search(pattern, text_content)
                if match:
                    business['phone'] = match.group().strip()
                    break
            
            # Buscar dirección
            address_keywords = ['calle', 'av.', 'avenida', 'blvd', 'col.', 'colonia']
            lines = text_content.split('\n')
            
            for line in lines:
                line_lower = line.lower()
                if any(keyword in line_lower for keyword in address_keywords):
                    if len(line.strip()) > 10:
                        business['address'] = line.strip()
                        break
            
            # Buscar rating
            rating_match = re.search(r'(\d+[.,]\d+)\s*★', text_content)
            if rating_match:
                business['rating'] = float(rating_match.group(1).replace(',', '.'))
            
            # Solo retornar si tiene información útil
            if business.get('name') and len(business.get('name', '')) > 3:
                return business
                
            return None
            
        except Exception as e:
            logger.warning(f"Error extrayendo info: {e}")
            return None

    def _process_leads(self, leads: List[Dict], sector: str, location: str) -> List[Dict]:
        """Procesar leads para PyMEs"""
        processed = []
        seen_names = set()
        
        for lead in leads:
            try:
                # Evitar duplicados
                name = lead.get('name', '').lower()
                if name in seen_names or not name:
                    continue
                seen_names.add(name)
                
                # Enriquecer lead
                enhanced_lead = {
                    'name': lead.get('name', ''),
                    'phone': lead.get('phone', ''),
                    'email': self._generate_email(lead.get('name', '')),
                    'address': lead.get('address', ''),
                    'sector': sector,
                    'location': location,
                    'source': 'Google Search Real',
                    'rating': lead.get('rating', 0),
                    'credit_potential': self._calculate_credit_potential(lead, sector),
                    'estimated_revenue': self._estimate_revenue(sector),
                    'loan_range': self._calculate_loan_range(sector),
                    'extracted_at': lead.get('extracted_at', datetime.now().isoformat())
                }
                
                processed.append(enhanced_lead)
                
            except Exception as e:
                logger.warning(f"Error procesando lead: {e}")
                continue
        
        return processed

    def _generate_email(self, business_name: str) -> str:
        """Generar email probable"""
        if not business_name:
            return ""
        
        clean_name = re.sub(r'[^a-zA-Z\s]', '', business_name.lower())
        words = clean_name.split()
        
        domains = ['gmail.com', 'hotmail.com', 'yahoo.com.mx', 'outlook.com']
        
        if len(words) >= 2:
            return f"{words[0]}.{words[1]}@{random.choice(domains)}"
        elif len(words) == 1:
            return f"{words[0]}@{random.choice(domains)}"
        
        return ""

    def _calculate_credit_potential(self, business: Dict, sector: str) -> str:
        """Calcular potencial crediticio"""
        score = 0
        
        # Score por sector
        high_capital_sectors = ['Restaurantes', 'Talleres', 'Producción']
        if sector in high_capital_sectors:
            score += 3
        else:
            score += 2
        
        # Score por información
        if business.get('phone'):
            score += 2
        if business.get('address'):
            score += 1
        if business.get('rating', 0) >= 4.0:
            score += 2
        
        # Determinar potencial
        if score >= 6:
            return "ALTO"
        elif score >= 4:
            return "MEDIO"
        else:
            return "BAJO"

    def _estimate_revenue(self, sector: str) -> str:
        """Estimar ingresos"""
        base_revenue = {
            'Restaurantes': random.randint(120000, 200000),
            'Talleres': random.randint(100000, 180000),
            'Producción': random.randint(80000, 150000),
            'Comercio': random.randint(60000, 120000),
            'Servicios': random.randint(40000, 100000)
        }
        
        base = base_revenue.get(sector, 70000)
        return f"${base:,} - ${int(base * 1.3):,}"

    def _calculate_loan_range(self, sector: str) -> str:
        """Calcular rango de préstamo"""
        ranges = {
            'Restaurantes': random.choice(['300,000 - 800,000', '500,000 - 1,200,000']),
            'Talleres': random.choice(['250,000 - 600,000', '400,000 - 1,000,000']),
            'Producción': random.choice(['200,000 - 500,000', '300,000 - 800,000']),
            'Comercio': random.choice(['150,000 - 400,000', '200,000 - 600,000']),
            'Servicios': random.choice(['100,000 - 300,000', '150,000 - 500,000'])
        }
        
        return f"${ranges.get(sector, '200,000 - 500,000')}"

    def cleanup_driver(self):
        """Limpiar driver"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None

    def __del__(self):
        """Destructor"""
        self.cleanup_driver()
