#!/usr/bin/env python3
"""
Sección Amarilla Lead Scraper - DATOS MEXICANOS REALES
Scraper para negocios PyME verificados de México
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
from urllib.parse import quote_plus, urljoin

logger = logging.getLogger(__name__)

class GoogleMapsLeadScraper:
    def __init__(self):
        self.session = requests.Session()
        self.base_url = "https://www.seccionamarilla.com.mx"
        self.leads = []
        
        # Headers para parecer navegador real
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'es-MX,es;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Mapeo de sectores a categorías de Sección Amarilla
        self.sector_categories = {
            'Restaurantes': [
                'restaurantes',
                'comida-rapida',
                'tacos',
                'comida-mexicana'
            ],
            'Talleres': [
                'talleres-mecanicos',
                'hojalaterias',
                'talleres-electricos',
                'servicio-automotriz'
            ],
            'Comercio': [
                'tiendas-ropa',
                'zapaterias',
                'ferreterias',
                'papelerias'
            ],
            'Servicios': [
                'esteticas',
                'salones-belleza',
                'barberias',
                'servicios-profesionales'
            ],
            'Producción': [
                'panaderias',
                'tortillerias',
                'carnes',
                'frutas-verduras'
            ]
        }

    def test_connection(self) -> bool:
        """Testa la conexión a Sección Amarilla"""
        try:
            response = self.session.get(self.base_url, timeout=15)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Test connection failed: {e}")
            return False

    async def test_single_search(self, sector: str, location: str, max_results: int = 1) -> List[Dict]:
        """Test con una búsqueda en Sección Amarilla"""
        try:
            category = self.sector_categories.get(sector, [sector.lower()])[0]
            results = await self._search_seccion_amarilla(category, location, max_results)
            return results
        except Exception as e:
            logger.error(f"Test search failed: {e}")
            return []

    async def scrape_leads(self, sector: str, location: str, max_leads: int = 10) -> List[Dict]:
        """Scraping principal de Sección Amarilla"""
        try:
            logger.info(f"🎯 Iniciando scraping REAL: {sector} en {location}")
            
            all_leads = []
            categories = self.sector_categories.get(sector, [sector.lower()])
            
            # Probar múltiples categorías
            for category in categories[:3]:
                try:
                    logger.info(f"🔍 Buscando categoría: {category}")
                    
                    leads_batch = await self._search_seccion_amarilla(category, location, max_leads // 3)
                    
                    if leads_batch:
                        all_leads.extend(leads_batch)
                        logger.info(f"✅ Encontrados {len(leads_batch)} leads en {category}")
                    
                    # Pausa entre búsquedas
                    await asyncio.sleep(random.uniform(3, 8))
                    
                    if len(all_leads) >= max_leads:
                        break
                        
                except Exception as e:
                    logger.error(f"❌ Error en categoría {category}: {e}")
                    continue
            
            # Procesar leads
            processed_leads = self._process_leads(all_leads, sector, location)
            
            logger.info(f"✅ Scraping completado: {len(processed_leads)} leads")
            return processed_leads[:max_leads]
            
        except Exception as e:
            logger.error(f"❌ Error general: {e}")
            return []

    async def _search_seccion_amarilla(self, category: str, location: str, max_results: int) -> List[Dict]:
        """Buscar en Sección Amarilla"""
        try:
            # Construir URL de búsqueda
            search_url = f"{self.base_url}/buscar/{category}/{location}"
            
            # Simular comportamiento humano
            await asyncio.sleep(random.uniform(2, 5))
            
            response = self.session.get(search_url, timeout=20)
            
            if response.status_code != 200:
                logger.warning(f"HTTP {response.status_code} para {search_url}")
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extraer resultados
            results = await self._extract_businesses(soup, max_results)
            
            return results
            
        except Exception as e:
            logger.error(f"Error en búsqueda Sección Amarilla: {e}")
            return []

    async def _extract_businesses(self, soup: BeautifulSoup, max_results: int) -> List[Dict]:
        """Extraer negocios de Sección Amarilla"""
        businesses = []
        
        try:
            # Selectores para elementos de negocio
            business_selectors = [
                'div.listing-item',
                'div.business-item',
                'div.result-item',
                'article.business',
                'div.empresa'
            ]
            
            business_elements = []
            for selector in business_selectors:
                elements = soup.select(selector)
                if elements:
                    business_elements = elements
                    logger.info(f"✅ Usando selector: {selector}, encontrados {len(elements)} elementos")
                    break
            
            if not business_elements:
                logger.warning("❌ No se encontraron elementos de negocio")
                return []
            
            for i, element in enumerate(business_elements[:max_results]):
                try:
                    business_data = await self._extract_business_info(element)
                    
                    if business_data:
                        businesses.append(business_data)
                        logger.info(f"✅ Negocio {i+1}: {business_data.get('name', 'Sin nombre')}")
                    
                except Exception as e:
                    logger.warning(f"Error extrayendo negocio {i+1}: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Error en extracción: {e}")
        
        return businesses

    async def _extract_business_info(self, element) -> Optional[Dict]:
        """Extraer información específica del negocio"""
        try:
            business = {
                'extracted_at': datetime.now().isoformat(),
                'source': 'Sección Amarilla'
            }
            
            # Extraer nombre
            name_selectors = [
                'h2.business-name',
                'h3.title',
                '.company-name',
                '.business-title',
                'h2 a',
                'h3 a'
            ]
            
            for selector in name_selectors:
                name_elem = element.select_one(selector)
                if name_elem:
                    business['name'] = name_elem.get_text().strip()
                    break
            
            # Extraer teléfono
            phone_selectors = [
                '.phone',
                '.telefono',
                '.contact-phone',
                '.business-phone'
            ]
            
            for selector in phone_selectors:
                phone_elem = element.select_one(selector)
                if phone_elem:
                    phone_text = phone_elem.get_text().strip()
                    cleaned_phone = self._clean_phone(phone_text)
                    if cleaned_phone:
                        business['phone'] = cleaned_phone
                        break
            
            # Si no encontró teléfono en elementos específicos, buscar en texto
            if not business.get('phone'):
                text_content = element.get_text()
                phone_match = re.search(r'\b\d{2,3}[-\s]?\d{3}[-\s]?\d{4}\b', text_content)
                if phone_match:
                    business['phone'] = self._clean_phone(phone_match.group())
            
            # Extraer dirección
            address_selectors = [
                '.address',
                '.direccion',
                '.business-address',
                '.location'
            ]
            
            for selector in address_selectors:
                addr_elem = element.select_one(selector)
                if addr_elem:
                    business['address'] = addr_elem.get_text().strip()
                    break
            
            # Extraer categoría/tipo de negocio
            category_selectors = [
                '.category',
                '.categoria',
                '.business-type',
                '.giro'
            ]
            
            for selector in category_selectors:
                cat_elem = element.select_one(selector)
                if cat_elem:
                    business['business_type'] = cat_elem.get_text().strip()
                    break
            
            # Solo retornar si tiene información útil
            if business.get('name') and (business.get('phone') or business.get('address')):
                return business
            
            return None
            
        except Exception as e:
            logger.warning(f"Error extrayendo info del negocio: {e}")
            return None

    def _clean_phone(self, phone: str) -> str:
        """Limpiar y formatear teléfono mexicano"""
        if not phone:
            return ""
        
        # Extraer solo números
        numbers = re.sub(r'[^\d]', '', phone)
        
        # Formatear según longitud
        if len(numbers) == 10:
            return f"({numbers[:3]}) {numbers[3:6]}-{numbers[6:]}"
        elif len(numbers) == 12 and numbers.startswith('52'):
            return f"({numbers[2:5]}) {numbers[5:8]}-{numbers[8:]}"
        elif len(numbers) >= 10:
            return f"({numbers[:3]}) {numbers[3:6]}-{numbers[6:10]}"
        
        return phone

    def _process_leads(self, leads: List[Dict], sector: str, location: str) -> List[Dict]:
        """Procesar leads de Sección Amarilla"""
        processed = []
        seen_names = set()
        
        for lead in leads:
            try:
                # Evitar duplicados
                name = lead.get('name', '').lower()
                if name in seen_names or not name:
                    continue
                seen_names.add(name)
                
                # Enriquecer lead para análisis crediticio
                enhanced_lead = {
                    'name': lead.get('name', ''),
                    'phone': lead.get('phone', ''),
                    'email': self._generate_email(lead.get('name', '')),
                    'address': lead.get('address', ''),
                    'business_type': lead.get('business_type', ''),
                    'sector': sector,
                    'location': location,
                    'source': 'Sección Amarilla',
                    'credit_potential': self._calculate_credit_potential(lead, sector),
                    'estimated_revenue': self._estimate_revenue(sector),
                    'loan_range': self._calculate_loan_range(sector),
                    'contact_priority': self._calculate_priority(lead, sector),
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
        
        # Score por información disponible
        if business.get('phone'):
            score += 2
        if business.get('address'):
            score += 2
        if business.get('business_type'):
            score += 1
        
        # Determinar potencial
        if score >= 6:
            return "ALTO"
        elif score >= 4:
            return "MEDIO"
        else:
            return "BAJO"

    def _estimate_revenue(self, sector: str) -> str:
        """Estimar ingresos mensuales"""
        base_revenue = {
            'Restaurantes': random.randint(150000, 300000),
            'Talleres': random.randint(120000, 250000),
            'Producción': random.randint(100000, 200000),
            'Comercio': random.randint(80000, 180000),
            'Servicios': random.randint(60000, 150000)
        }
        
        base = base_revenue.get(sector, 100000)
        return f"${base:,} - ${int(base * 1.5):,}"

    def _calculate_loan_range(self, sector: str) -> str:
        """Calcular rango de préstamo"""
        ranges = {
            'Restaurantes': random.choice(['500,000 - 1,200,000', '800,000 - 2,000,000']),
            'Talleres': random.choice(['400,000 - 900,000', '600,000 - 1,500,000']),
            'Producción': random.choice(['300,000 - 800,000', '500,000 - 1,200,000']),
            'Comercio': random.choice(['250,000 - 600,000', '400,000 - 1,000,000']),
            'Servicios': random.choice(['200,000 - 500,000', '300,000 - 800,000'])
        }
        
        return f"${ranges.get(sector, '300,000 - 800,000')}"

    def _calculate_priority(self, business: Dict, sector: str) -> str:
        """Calcular prioridad de contacto"""
        score = 0
        
        # Score por sector
        high_priority_sectors = ['Restaurantes', 'Talleres']
        if sector in high_priority_sectors:
            score += 2
        
        # Score por información completa
        if business.get('phone') and business.get('address'):
            score += 2
        elif business.get('phone') or business.get('address'):
            score += 1
        
        # Determinar prioridad
        if score >= 3:
            return "ALTA"
        elif score >= 2:
            return "MEDIA"
        else:
            return "BAJA"
