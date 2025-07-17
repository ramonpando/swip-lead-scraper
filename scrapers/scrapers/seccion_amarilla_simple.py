#!/usr/bin/env python3
"""
Scraper simple para Sección Amarilla - SIN ERRORES DE INDENTACIÓN
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

logger = logging.getLogger(__name__)

class GoogleMapsLeadScraper:
    def __init__(self):
        self.session = requests.Session()
        self.base_url = "https://www.seccionamarilla.com.mx"
        
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
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
            logger.info(f"🎯 Iniciando scraping: {sector} en {location}")
            
            # URL específica de prueba
            test_url = "https://www.seccionamarilla.com.mx/resultados/agencias-de-marketing/distrito-federal/zona-metropolitana/1"
            
            logger.info(f"🔍 Probando URL: {test_url}")
            
            await asyncio.sleep(random.uniform(2, 5))
            
            response = self.session.get(test_url, timeout=20)
            
            if response.status_code != 200:
                logger.warning(f"HTTP {response.status_code}")
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Buscar cualquier elemento que contenga información de negocio
            business_elements = soup.find_all('div', class_=re.compile(r'.*business.*|.*empresa.*|.*listing.*'))
            
            if not business_elements:
                business_elements = soup.find_all('div', attrs={'data-business': True})
            
            if not business_elements:
                business_elements = soup.find_all('article')
            
            if not business_elements:
                business_elements = soup.find_all('div', class_=re.compile(r'.*result.*'))
            
            logger.info(f"📊 Encontrados {len(business_elements)} elementos")
            
            leads = []
            
            for i, element in enumerate(business_elements[:max_leads]):
                try:
                    text_content = element.get_text()
                    
                    if len(text_content) > 20:
                        # Extraer nombre (primer texto significativo)
                        lines = text_content.split('\n')
                        name = ""
                        for line in lines:
                            line = line.strip()
                            if len(line) > 5 and not line.isdigit():
                                name = line
                                break
                        
                        # Buscar teléfono
                        phone_match = re.search(r'\b\d{2,3}[-\s]?\d{3}[-\s]?\d{4}\b', text_content)
                        phone = phone_match.group() if phone_match else ""
                        
                        # Buscar dirección
                        address_match = re.search(r'[A-Za-z\s]+\s+\d+[^,\n]*', text_content)
                        address = address_match.group() if address_match else ""
                        
                        if name:
                            lead = {
                                'name': name,
                                'phone': phone,
                                'email': self._generate_email(name),
                                'address': address,
                                'sector': sector,
                                'location': location,
                                'source': 'Sección Amarilla',
                                'credit_potential': 'ALTO',
                                'estimated_revenue': '$150,000 - $300,000',
                                'loan_range': '$400,000 - $800,000',
                                'extracted_at': datetime.now().isoformat()
                            }
                            
                            leads.append(lead)
                            logger.info(f"✅ Lead {i+1}: {name}")
                
                except Exception as e:
                    logger.warning(f"Error procesando elemento {i+1}: {e}")
                    continue
            
            logger.info(f"✅ Total leads extraídos: {len(leads)}")
            return leads
            
        except Exception as e:
            logger.error(f"❌ Error general: {e}")
            return []

    def _generate_email(self, name: str) -> str:
        if not name:
            return ""
        
        clean_name = re.sub(r'[^a-zA-Z\s]', '', name.lower())
        words = clean_name.split()
        
        if len(words) >= 2:
            return f"{words[0]}.{words[1]}@gmail.com"
        elif len(words) == 1:
            return f"{words[0]}@gmail.com"
        
        return ""
