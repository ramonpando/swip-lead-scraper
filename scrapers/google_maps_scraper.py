#!/usr/bin/env python3
"""
Google Maps Lead Scraper - VERSIÓN ANTI-DETECCIÓN
Scraper robusto que burla protecciones de Google
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
from urllib.parse import quote_plus
import json

logger = logging.getLogger(__name__)

class GoogleMapsLeadScraper:
    def __init__(self):
        self.session = requests.Session()
        self.leads = []
        
        # User agents rotativos
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0'
        ]
        
        # Configurar headers iniciales
        self.update_headers()
        
        # Sectores específicos para México
        self.sector_terms = {
            "Restaurantes": [
                "restaurante cerca de mi",
                "comida mexicana",
                "tacos",
                "cocina tradicional"
            ],
            "Talleres": [
                "taller mecánico",
                "reparación autos",
                "servicio automotriz",
                "mecánico"
            ],
            "Comercio": [
                "tienda",
                "comercio local",
                "negocio",
                "establecimiento"
            ],
            "Servicios": [
                "servicios locales",
                "negocio de servicios",
                "empresa de servicios",
                "servicio profesional"
            ],
            "Producción": [
                "panadería",
                "producción local",
                "manufactura",
                "fábrica pequeña"
            ]
        }

    def update_headers(self):
        """Actualiza headers con user agent aleatorio"""
        user_agent = random.choice(self.user_agents)
        self.session.headers.update({
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'es-MX,es;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })

    def test_connection(self) -> bool:
        """Testa la conexión"""
        try:
            self.update_headers()
            response = self.session.get("https://www.google.com", timeout=15)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Test connection failed: {e}")
            return False

    async def test_single_search(self, sector: str, location: str, max_results: int = 1) -> List[Dict]:
        """Test con una búsqueda real"""
        try:
            # Usar búsqueda directa en Google
            query = f"{sector} {location} teléfono"
            results = await self._search_google_directly(query, max_results)
            return results
            
        except Exception as e:
            logger.error(f"Test search failed: {e}")
            return []

    async def scrape_leads(self, sector: str, location: str, max_leads: int = 10) -> List[Dict]:
        """Scraping principal REAL"""
        try:
            logger.info(f"🎯 Iniciando scraping REAL: {sector} en {location}")
            
            all_leads = []
            terms = self.sector_terms.get(sector, [sector])
            
            # Usar múltiples fuentes
            sources = [
                self._search_google_directly,
                self._search_google_maps_alternative,
                self._search_business_listings
            ]
            
            # Probar 2 términos con 2 fuentes cada uno
            for term in terms[:2]:
                for source_func in sources[:2]:
                    try:
                        search_query = f"{term} {location} contacto"
                        
                        logger.info(f"🔍 Buscando: {search_query} con {source_func.__name__}")
                        
                        leads_batch = await source_func(search_query, max_leads // 4)
                        
                        if leads_batch:
                            all_leads.extend(leads_batch)
                            logger.info(f"✅ Encontrados {len(leads_batch)} leads")
                        
                        # Pausa entre búsquedas
                        await asyncio.sleep(random.uniform(3, 8))
                        
                        if len(all_leads) >= max_leads:
                            break
                            
                    except Exception as e:
                        logger.error(f"❌ Error en fuente {source_func.__name__}: {e}")
                        continue
                
                if len(all_leads) >= max_leads:
                    break
            
            # Procesar y filtrar leads
            processed_leads = self._process_leads(all_leads, sector, location)
            
            logger.info(f"✅ Scraping completado: {len(processed_leads)} leads válidos")
            return processed_leads[:max_leads]
            
        except Exception as e:
            logger.error(f"❌ Error en scraping: {e}")
            return []

    async def _search_google_directly(self, query: str, max_results: int) -> List[Dict]:
        """Búsqueda directa en Google"""
        try:
            self.update_headers()
            
            # URL de búsqueda
            search_url = f"https://www.google.com/search?q={quote_plus(query)}&num={max_results * 2}"
            
            # Simular comportamiento humano
            await asyncio.sleep(random.uniform(2, 5))
            
            response = self.session.get(search_url, timeout=20)
            
            if response.status_code != 200:
                logger.warning(f"HTTP {response.status_code} para Google search")
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extraer resultados
            results = await self._extract_from_google_results(soup, max_results)
            
            return results
            
        except Exception as e:
            logger.error(f"Error en búsqueda directa Google: {e}")
            return []

    async def _search_google_maps_alternative(self, query: str, max_results: int) -> List[Dict]:
        """Búsqueda alternativa simulando Google Maps"""
        try:
            self.update_headers()
            
            # Usar Google con parámetros de Maps
            search_url = f"https://www.google.com/search?q={quote_plus(query)}&tbm=lcl&num={max_results * 2}"
            
            await asyncio.sleep(random.uniform(3, 7))
            
            response = self.session.get(search_url, timeout=25)
            
            if response.status_code != 200:
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extraer datos de resultados locales
            results = await self._extract_local_results(soup, max_results)
            
            return results
            
        except Exception as e:
            logger.error(f"Error en búsqueda Maps alternativa: {e}")
            return []

    async def _search_business_listings(self, query: str, max_results: int) -> List[Dict]:
        """Generar listings de ejemplo basados en patrones reales"""
        try:
            # Simular datos realistas para testing
            mock_businesses = [
                {
                    "name": f"Restaurante {random.choice(['La Terraza', 'El Rincón', 'Casa Blanca', 'Del Centro', 'Tradición'])}",
                    "phone": f"442-{random.randint(200, 999)}-{random.randint(1000, 9999)}",
                    "address": f"Calle {random.choice(['Juárez', 'Hidalgo', 'Morelos', 'Allende'])} #{random.randint(100, 999)}",
                    "sector": "Restaurantes",
                    "rating": round(random.uniform(3.5, 4.8), 1)
                },
                {
                    "name": f"Taller {random.choice(['Hernández', 'Mecánico Express', 'AutoServicio', 'Reparaciones López'])}",
                    "phone": f"442-{random.randint(300, 999)}-{random.randint(1000, 9999)}",
                    "address": f"Av. {random.choice(['Constituyentes', 'Universidad', 'Tecnológico'])} #{random.randint(200, 800)}",
                    "sector": "Talleres",
                    "rating": round(random.uniform(3.8, 4.5), 1)
                }
            ]
            
            # Devolver algunos resultados simulados
            return mock_businesses[:max_results]
            
        except Exception as e:
            logger.error(f"Error en business listings: {e}")
            return []

    async def _extract_from_google_results(self, soup: BeautifulSoup, max_results: int) -> List[Dict]:
        """Extraer datos de resultados de Google"""
        businesses = []
        
        try:
            # Buscar en diferentes contenedores
            result_containers = soup.find_all(['div'], class_=['g', 'tF2Cxc', 'MjjYud'])
            
            for container in result_containers[:max_results * 2]:
                try:
                    business_data = await self._extract_business_from_container(container)
                    if business_data:
                        businesses.append(business_data)
                        
                    if len(businesses) >= max_results:
                        break
                        
                except Exception as e:
                    continue
            
        except Exception as e:
            logger.error(f"Error extrayendo de Google: {e}")
        
        return businesses

    async def _extract_local_results(self, soup: BeautifulSoup, max_results: int) -> List[Dict]:
        """Extraer resultados locales"""
        businesses = []
        
        try:
            # Buscar elementos específicos de resultados locales
            local_results = soup.find_all(['div'], class_=['VkpGBb', 'cXedhc'])
            
            for result in local_results[:max_results]:
                try:
                    business_data = await self._extract_local_business(result)
                    if business_data:
                        businesses.append(business_data)
                        
                except Exception as e:
                    continue
            
        except Exception as e:
            logger.error(f"Error extrayendo resultados locales: {e}")
        
        return businesses

    async def _extract_business_from_container(self, container) -> Optional[Dict]:
        """Extraer información de un contenedor de resultado"""
        try:
            text_content = container.get_text()
            
            if len(text_content) < 20:
                return None
            
            business = {
                'extracted_at': datetime.now().isoformat(),
                'source': 'Google Search'
            }
            
            # Extraer nombre (primer enlace)
            link = container.find('a')
            if link:
                title_elem = link.find(['h3', 'h2'])
                if title_elem:
                    business['name'] = title_elem.get_text().strip()
            
            # Buscar teléfonos mexicanos
            phone_patterns = [
                r'\b442[-\s]?\d{3}[-\s]?\d{4}\b',  # Querétaro
                r'\b\d{3}[-\s]?\d{3}[-\s]?\d{4}\b',  # Formato general
                r'\(\d{3}\)\s?\d{3}[-\s]?\d{4}',    # Con paréntesis
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
            
            # Solo retornar si tiene datos útiles
            if business.get('name') and (business.get('phone') or business.get('address')):
                return business
                
            return None
            
        except Exception as e:
            return None

    async def _extract_local_business(self, result) -> Optional[Dict]:
        """Extraer datos de resultado local"""
        try:
            business = {
                'extracted_at': datetime.now().isoformat(),
                'source': 'Google Local'
            }
            
            # Extraer nombre
            name_elem = result.find(['span', 'div'], class_=['OSrXXb', 'dbg0pd'])
            if name_elem:
                business['name'] = name_elem.get_text().strip()
            
            # Buscar teléfono en el texto
            text = result.get_text()
            phone_match = re.search(r'\b\d{3}[-\s]?\d{3}[-\s]?\d{4}\b', text)
            if phone_match:
                business['phone'] = phone_match.group().strip()
            
            # Buscar rating
            rating_elem = result.find(['span'], class_=['yi40Hd'])
            if rating_elem:
                rating_text = rating_elem.get_text()
                rating_match = re.search(r'(\d+[.,]\d+)', rating_text)
                if rating_match:
                    business['rating'] = float(rating_match.group(1).replace(',', '.'))
            
            return business if business.get('name') else None
            
        except Exception as e:
            return None

    def _process_leads(self, leads: List[Dict], sector: str, location: str) -> List[Dict]:
        """Procesa y enriquece leads"""
        processed = []
        seen_names = set()
        
        for lead in leads:
            try:
                # Evitar duplicados
                name = lead.get('name', '').lower()
                if name in seen_names or not name:
                    continue
                seen_names.add(name)
                
                # Enriquecer información
                enhanced_lead = {
                    'name': lead.get('name', f'Negocio {sector}'),
                    'phone': lead.get('phone', ''),
                    'email': self._generate_email(lead.get('name', '')),
                    'address': lead.get('address', ''),
                    'sector': sector,
                    'location': location,
                    'source': lead.get('source', 'Google Search'),
                    'rating': lead.get('rating', 0),
                    'credit_potential': self._calculate_credit_potential(lead, sector),
                    'extracted_at': lead.get('extracted_at', datetime.now().isoformat())
                }
                
                processed.append(enhanced_lead)
                
            except Exception as e:
                logger.warning(f"Error procesando lead: {e}")
                continue
        
        return processed

    def _generate_email(self, business_name: str) -> str:
        """Generar email probable basado en nombre"""
        if not business_name:
            return ""
        
        # Limpiar nombre
        clean_name = re.sub(r'[^a-zA-Z\s]', '', business_name.lower())
        words = clean_name.split()
        
        if len(words) >= 2:
            return f"{words[0]}.{words[1]}@gmail.com"
        elif len(words) == 1:
            return f"{words[0]}@hotmail.com"
        
        return ""

    def _calculate_credit_potential(self, business: Dict, sector: str) -> str:
        """Calcula potencial de crédito"""
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
            score += 1
        if business.get('rating', 0) >= 4.0:
            score += 2
        elif business.get('rating', 0) >= 3.5:
            score += 1
        
        # Determinar potencial
        if score >= 6:
            return "ALTO"
        elif score >= 4:
            return "MEDIO"
        else:
            return "BAJO"
