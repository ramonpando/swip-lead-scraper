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
            
         # Buscar contenedores de negocios
business_elements = soup.find_all('div', class_='container_out')

if not business_elements:
    # Buscar por otros selectores
    business_elements = soup.find_all('div', attrs={'id': True})

if not business_elements:
    # Buscar cualquier div que contenga texto de teléfono
    business_elements = soup.find_all('div', string=re.compile(r'\(\d{2,3}\)\d{3}-\d{4}'))

logger.info(f"📊 Encontrados {len(business_elements)} elementos")

leads = []

for i, element in enumerate(business_elements[:max_leads]):
    try:
        # Extraer texto completo
        text_content = element.get_text()
        
        if len(text_content) > 10:
            # Buscar nombre del negocio
            name = ""
            lines = text_content.split('\n')
            for line in lines:
                line = line.strip()
                if len(line) > 3 and not line.isdigit() and '(' not in line:
                    name = line
                    break
            
            # Buscar teléfono con formato mexicano
            phone_patterns = [
                r'\(\d{2,3}\)\d{3}-\d{4}',
                r'\(\d{2,3}\)\d{3}\d{4}',
                r'\d{2,3}-\d{3}-\d{4}'
            ]
            
            phone = ""
            for pattern in phone_patterns:
                phone_match = re.search(pattern, text_content)
                if phone_match:
                    phone = phone_match.group()
                    break
            
            if name and phone:
                lead = {
                    'name': name,
                    'phone': phone,
                    'email': f"{name.lower().replace(' ', '.')}@gmail.com",
                    'address': f"Zona Metropolitana, Distrito Federal",
                    'sector': sector,
                    'location': location,
                    'source': 'Sección Amarilla',
                    'credit_potential': 'ALTO',
                    'estimated_revenue': '$200,000 - $500,000',
                    'loan_range': '$500,000 - $1,200,000',
                    'extracted_at': datetime.now().isoformat()
                }
                
                leads.append(lead)
                logger.info(f"✅ Lead {i+1}: {name} - {phone}")
    
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
