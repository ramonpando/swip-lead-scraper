#!/usr/bin/env python3
"""
Google Maps Lead Scraper - SELENIUM REAL
Scraper profesional para leads PyME reales - 3M pesos mensuales
"""

import asyncio
import time
import random
from typing import List, Dict, Optional
import logging
from datetime import datetime
import re
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import undetected_chromedriver as uc
from webdriver_manager.chrome import ChromeDriverManager

logger = logging.getLogger(__name__)

class GoogleMapsLeadScraper:
    def __init__(self):
        self.driver = None
        self.leads = []
        
        # Sectores específicos para créditos PyME
        self.sector_queries = {
            "Restaurantes": [
                "restaurante Querétaro", 
                "comida Querétaro", 
                "tacos Querétaro",
                "cocina mexicana Querétaro"
            ],
            "Talleres": [
                "taller mecánico Querétaro",
                "reparación autos Querétaro", 
                "servicio automotriz Querétaro",
                "hojalatería Querétaro"
            ],
            "Comercio": [
                "tienda Querétaro",
                "comercio Querétaro",
                "negocio local Querétaro",
                "boutique Querétaro"
            ],
            "Servicios": [
                "servicios Querétaro",
                "estética Querétaro",
                "salón belleza Querétaro",
                "barbería Querétaro"
            ],
            "Producción": [
                "panadería Querétaro",
                "tortillería Querétaro",
                "producción Querétaro",
                "manufactura Querétaro"
            ]
        }
    def setup_driver(self):
    # Configura Chrome con undetected-chromedriver
    try:
        # Configurar opciones básicas
        options = uc.ChromeOptions()
        
        # Configuraciones básicas que SÍ funcionan
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-plugins')
        options.add_argument('--disable-images')
        
        # User agent
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # Crear driver undetected (versión simple)
        self.driver = uc.Chrome(options=options, version_main=120)
        
        # Configurar timeouts
        self.driver.set_page_load_timeout(30)
        self.driver.implicitly_wait(10)
        
        logger.info("✅ Chrome undetected driver configurado exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error configurando driver: {e}")
        return False

    def test_connection(self) -> bool:
        """Testa la conexión"""
        try:
            if not self.setup_driver():
                return False
            
            # Probar acceso a Google
            self.driver.get("https://www.google.com")
            time.sleep(3)
            
            # Verificar que cargó correctamente
            title = self.driver.title
            self.cleanup_driver()
            
            return "google" in title.lower()
            
        except Exception as e:
            logger.error(f"Test connection failed: {e}")
            self.cleanup_driver()
            return False

    async def test_single_search(self, sector: str, location: str, max_results: int = 1) -> List[Dict]:
        """Test con una búsqueda real"""
        try:
            if not self.setup_driver():
                return []
            
            # Buscar en Google Maps
            query = f"{sector} {location}"
            results = await self._search_google_maps_real(query, max_results)
            
            self.cleanup_driver()
            return results
            
        except Exception as e:
            logger.error(f"Test search failed: {e}")
            self.cleanup_driver()
            return []

    async def scrape_leads(self, sector: str, location: str, max_leads: int = 10) -> List[Dict]:
        """Scraping principal para leads REALES"""
        try:
            logger.info(f"🎯 Iniciando scraping REAL: {sector} en {location}")
            
            if not self.setup_driver():
                return []
            
            all_leads = []
            queries = self.sector_queries.get(sector, [f"{sector} {location}"])
            
            # Ejecutar múltiples búsquedas
            for query in queries[:3]:  # Máximo 3 búsquedas por sector
                try:
                    logger.info(f"🔍 Ejecutando búsqueda: {query}")
                    
                    # Buscar en Google Maps
                    leads_batch = await self._search_google_maps_real(query, max_leads // 3)
                    
                    if leads_batch:
                        all_leads.extend(leads_batch)
                        logger.info(f"✅ Encontrados {len(leads_batch)} leads en {query}")
                    
                    # Pausa entre búsquedas
                    await asyncio.sleep(random.uniform(5, 10))
                    
                    if len(all_leads) >= max_leads:
                        break
                        
                except Exception as e:
                    logger.error(f"❌ Error en búsqueda {query}: {e}")
                    continue
            
            self.cleanup_driver()
            
            # Procesar y filtrar leads
            processed_leads = self._process_leads(all_leads, sector, location)
            
            logger.info(f"✅ Scraping completado: {len(processed_leads)} leads procesados")
            return processed_leads[:max_leads]
            
        except Exception as e:
            logger.error(f"❌ Error general en scraping: {e}")
            self.cleanup_driver()
            return []

    async def _search_google_maps_real(self, query: str, max_results: int) -> List[Dict]:
        """Búsqueda REAL en Google Maps"""
        try:
            # Ir a Google Maps
            self.driver.get("https://www.google.com/maps")
            
            # Esperar a que cargue
            await asyncio.sleep(random.uniform(3, 6))
            
            # Buscar caja de búsqueda
            search_box = WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.ID, "searchboxinput"))
            )
            
            # Escribir búsqueda de forma humana
            search_box.clear()
            for char in query:
                search_box.send_keys(char)
                await asyncio.sleep(random.uniform(0.05, 0.15))
            
            # Buscar botón de búsqueda
            search_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "searchbox-searchbutton"))
            )
            search_button.click()
            
            # Esperar resultados
            await asyncio.sleep(random.uniform(5, 8))
            
            # Scroll para cargar más resultados
            await self._scroll_results()
            
            # Extraer resultados
            results = await self._extract_maps_results(max_results)
            
            return results
            
        except Exception as e:
            logger.error(f"Error en búsqueda Maps: {e}")
            return []

    async def _scroll_results(self):
        """Scroll para cargar más resultados"""
        try:
            # Encontrar panel de resultados
            results_panel = self.driver.find_element(By.CSS_SELECTOR, '[role="main"]')
            
            # Scroll múltiple
            for i in range(3):
                self.driver.execute_script("arguments[0].scrollBy(0, 1000);", results_panel)
                await asyncio.sleep(random.uniform(2, 4))
            
        except Exception as e:
            logger.warning(f"Error en scroll: {e}")

    async def _extract_maps_results(self, max_results: int) -> List[Dict]:
        """Extrae resultados de Google Maps"""
        businesses = []
        
        try:
            # Esperar a que aparezcan los resultados
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '[role="main"]'))
            )
            
            # Buscar elementos de negocios
            business_elements = self.driver.find_elements(By.CSS_SELECTOR, 'div[data-result-index]')
            
            if not business_elements:
                # Intentar otros selectores
                business_elements = self.driver.find_elements(By.CSS_SELECTOR, 'div[jsaction*="mouseover"]')
            
            logger.info(f"📊 Encontrados {len(business_elements)} elementos de negocios")
            
            for i, element in enumerate(business_elements[:max_results]):
                try:
                    # Click en el elemento para abrir detalles
                    self.driver.execute_script("arguments[0].click();", element)
                    await asyncio.sleep(random.uniform(2, 4))
                    
                    # Extraer información detallada
                    business_data = await self._extract_business_details()
                    
                    if business_data:
                        businesses.append(business_data)
                        logger.info(f"✅ Negocio extraído: {business_data.get('name', 'Sin nombre')}")
                    
                    # Pausa entre extracciones
                    await asyncio.sleep(random.uniform(1, 3))
                    
                except Exception as e:
                    logger.warning(f"Error extrayendo negocio {i+1}: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Error general en extracción: {e}")
        
        return businesses

    async def _extract_business_details(self) -> Optional[Dict]:
        """Extrae detalles específicos del negocio"""
        try:
            business = {
                'extracted_at': datetime.now().isoformat(),
                'source': 'Google Maps Real'
            }
            
            # Extraer nombre
            try:
                name_element = self.driver.find_element(By.CSS_SELECTOR, 'h1.DUwDvf')
                business['name'] = name_element.text.strip()
            except:
                try:
                    name_element = self.driver.find_element(By.CSS_SELECTOR, '[data-attrid="title"]')
                    business['name'] = name_element.text.strip()
                except:
                    pass
            
            # Extraer teléfono
            try:
                phone_element = self.driver.find_element(By.CSS_SELECTOR, 'button[data-item-id*="phone"]')
                business['phone'] = phone_element.get_attribute('data-item-id').replace('phone:', '')
            except:
                try:
                    phone_element = self.driver.find_element(By.CSS_SELECTOR, '[data-attrid="kc:/collection/knowledge_panels/has_phone:phone"]')
                    business['phone'] = phone_element.text.strip()
                except:
                    pass
            
            # Extraer dirección
            try:
                address_element = self.driver.find_element(By.CSS_SELECTOR, 'button[data-item-id*="address"]')
                business['address'] = address_element.text.strip()
            except:
                try:
                    address_element = self.driver.find_element(By.CSS_SELECTOR, '[data-attrid="kc:/location/location:address"]')
                    business['address'] = address_element.text.strip()
                except:
                    pass
            
            # Extraer rating
            try:
                rating_element = self.driver.find_element(By.CSS_SELECTOR, 'span.ceNzKf')
                rating_text = rating_element.text
                rating_match = re.search(r'(\d+[.,]\d+)', rating_text)
                if rating_match:
                    business['rating'] = float(rating_match.group(1).replace(',', '.'))
            except:
                pass
            
            # Extraer horarios
            try:
                hours_element = self.driver.find_element(By.CSS_SELECTOR, '[data-attrid="kc:/location/location:hours"]')
                business['hours'] = hours_element.text.strip()
            except:
                pass
            
            # Extraer website
            try:
                website_element = self.driver.find_element(By.CSS_SELECTOR, 'a[data-item-id*="authority"]')
                business['website'] = website_element.get_attribute('href')
            except:
                pass
            
            # Solo retornar si tiene información básica
            if business.get('name') and (business.get('phone') or business.get('address')):
                return business
            
            return None
            
        except Exception as e:
            logger.warning(f"Error extrayendo detalles: {e}")
            return None

    def _process_leads(self, leads: List[Dict], sector: str, location: str) -> List[Dict]:
        """Procesa y enriquece leads para análisis crediticio"""
        processed = []
        seen_names = set()
        
        for lead in leads:
            try:
                # Evitar duplicados
                name = lead.get('name', '').lower()
                if name in seen_names or not name:
                    continue
                seen_names.add(name)
                
                # Enriquecer para análisis crediticio
                enhanced_lead = {
                    'name': lead.get('name', ''),
                    'phone': self._clean_phone(lead.get('phone', '')),
                    'email': self._generate_probable_email(lead.get('name', '')),
                    'address': lead.get('address', ''),
                    'website': lead.get('website', ''),
                    'hours': lead.get('hours', ''),
                    'sector': sector,
                    'location': location,
                    'source': 'Google Maps Real',
                    'rating': lead.get('rating', 0),
                    'credit_potential': self._calculate_credit_potential(lead, sector),
                    'estimated_revenue': self._estimate_revenue(lead, sector),
                    'loan_range': self._calculate_loan_range(lead, sector),
                    'extracted_at': lead.get('extracted_at', datetime.now().isoformat())
                }
                
                processed.append(enhanced_lead)
                
            except Exception as e:
                logger.warning(f"Error procesando lead: {e}")
                continue
        
        return processed

    def _clean_phone(self, phone: str) -> str:
        """Limpia y formatea teléfono"""
        if not phone:
            return ""
        
        # Extraer solo números
        cleaned = re.sub(r'[^\d]', '', phone)
        
        # Formatear para México
        if len(cleaned) == 10:
            return f"({cleaned[:3]}) {cleaned[3:6]}-{cleaned[6:]}"
        elif len(cleaned) == 12 and cleaned.startswith('52'):
            return f"({cleaned[2:5]}) {cleaned[5:8]}-{cleaned[8:]}"
        
        return phone

    def _generate_probable_email(self, business_name: str) -> str:
        """Genera email probable"""
        if not business_name:
            return ""
        
        # Limpiar nombre
        clean_name = re.sub(r'[^a-zA-Z\s]', '', business_name.lower())
        words = clean_name.split()
        
        domains = ['gmail.com', 'hotmail.com', 'yahoo.com.mx', 'outlook.com']
        
        if len(words) >= 2:
            return f"{words[0]}.{words[1]}@{random.choice(domains)}"
        elif len(words) == 1:
            return f"{words[0]}@{random.choice(domains)}"
        
        return ""

    def _calculate_credit_potential(self, business: Dict, sector: str) -> str:
        """Calcula potencial crediticio REAL"""
        score = 0
        
        # Score por sector (necesidad de capital)
        high_capital_sectors = ['Restaurantes', 'Talleres', 'Producción']
        if sector in high_capital_sectors:
            score += 4
        else:
            score += 2
        
        # Score por información disponible
        if business.get('phone'):
            score += 2
        if business.get('address'):
            score += 2
        if business.get('website'):
            score += 1
        
        # Score por rating (calidad del negocio)
        rating = business.get('rating', 0)
        if rating >= 4.5:
            score += 3
        elif rating >= 4.0:
            score += 2
        elif rating >= 3.5:
            score += 1
        
        # Score por horarios (negocio activo)
        if business.get('hours'):
            score += 1
        
        # Determinar potencial
        if score >= 8:
            return "ALTO"
        elif score >= 5:
            return "MEDIO"
        else:
            return "BAJO"

    def _estimate_revenue(self, business: Dict, sector: str) -> str:
        """Estima ingresos mensuales"""
        base_revenue = {
            'Restaurantes': 150000,
            'Talleres': 120000,
            'Producción': 100000,
            'Comercio': 80000,
            'Servicios': 60000
        }
        
        base = base_revenue.get(sector, 70000)
        
        # Ajustar por rating
        rating = business.get('rating', 3.5)
        if rating >= 4.5:
            multiplier = 1.5
        elif rating >= 4.0:
            multiplier = 1.2
        elif rating >= 3.5:
            multiplier = 1.0
        else:
            multiplier = 0.8
        
        estimated = int(base * multiplier)
        
        return f"${estimated:,} - ${int(estimated * 1.5):,}"

    def _calculate_loan_range(self, business: Dict, sector: str) -> str:
        """Calcula rango de préstamo recomendado"""
        potential = self._calculate_credit_potential(business, sector)
        
        ranges = {
            'ALTO': "500,000 - 1,500,000",
            'MEDIO': "200,000 - 800,000", 
            'BAJO': "50,000 - 300,000"
        }
        
        return f"${ranges.get(potential, '100,000 - 500,000')}"

    def cleanup_driver(self):
        """Limpia driver"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None

    def __del__(self):
        """Destructor"""
        self.cleanup_driver()
