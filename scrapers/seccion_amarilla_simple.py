#!/usr/bin/env python3
"""
Scraper Inteligente con Rotación Automática
Sistema que rota categorías y páginas automáticamente
"""

import asyncio
import time
import json
import os
from typing import List, Dict, Optional, Tuple
import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime
import re

# Logger setup
logger = logging.getLogger(__name__)

class IntelligentRotationScraper:
    """Scraper con rotación automática inteligente"""
    
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
        
        # Archivo de estado persistente
        self.state_file = '/tmp/scraper_state.json'
        self.state = self.load_state()

    def get_rotation_strategies(self) -> List[Dict]:
        """Estrategias de rotación para CDMX, Edo México, Querétaro"""
        return [
            # CATEGORÍA 1: MARKETING
            {
                "id": "marketing_cdmx",
                "category": "agencias-de-marketing",
                "location": "distrito-federal/zona-metropolitana",
                "name": "Marketing CDMX",
                "max_pages": 10,
                "priority": 1
            },
            {
                "id": "marketing_edomex",
                "category": "agencias-de-marketing", 
                "location": "estado-de-mexico/toluca",
                "name": "Marketing Edo México",
                "max_pages": 8,
                "priority": 1
            },
            {
                "id": "marketing_queretaro",
                "category": "agencias-de-marketing",
                "location": "queretaro/queretaro", 
                "name": "Marketing Querétaro",
                "max_pages": 6,
                "priority": 1
            },
            
            # CATEGORÍA 2: PUBLICIDAD
            {
                "id": "publicidad_cdmx",
                "category": "agencias-de-publicidad",
                "location": "distrito-federal/zona-metropolitana",
                "name": "Publicidad CDMX", 
                "max_pages": 8,
                "priority": 2
            },
            {
                "id": "publicidad_edomex",
                "category": "agencias-de-publicidad",
                "location": "estado-de-mexico/toluca",
                "name": "Publicidad Edo México",
                "max_pages": 6,
                "priority": 2
            },
            {
                "id": "publicidad_queretaro",
                "category": "agencias-de-publicidad",
                "location": "queretaro/queretaro",
                "name": "Publicidad Querétaro", 
                "max_pages": 4,
                "priority": 2
            },
            
            # CATEGORÍA 3: SERVICIOS INTERNET
            {
                "id": "internet_cdmx",
                "category": "servicios-de-internet",
                "location": "distrito-federal/zona-metropolitana",
                "name": "Servicios Internet CDMX",
                "max_pages": 6,
                "priority": 3
            },
            {
                "id": "internet_edomex", 
                "category": "servicios-de-internet",
                "location": "estado-de-mexico/toluca",
                "name": "Servicios Internet Edo México",
                "max_pages": 4,
                "priority": 3
            },
            {
                "id": "internet_queretaro",
                "category": "servicios-de-internet", 
                "location": "queretaro/queretaro",
                "name": "Servicios Internet Querétaro",
                "max_pages": 3,
                "priority": 3
            },
            
            # CATEGORÍA 4: DISEÑO GRÁFICO
            {
                "id": "diseno_cdmx",
                "category": "diseno-grafico",
                "location": "distrito-federal/zona-metropolitana", 
                "name": "Diseño Gráfico CDMX",
                "max_pages": 5,
                "priority": 4
            },
            {
                "id": "diseno_edomex",
                "category": "diseno-grafico",
                "location": "estado-de-mexico/toluca",
                "name": "Diseño Gráfico Edo México", 
                "max_pages": 3,
                "priority": 4
            },
            {
                "id": "diseno_queretaro",
                "category": "diseno-grafico",
                "location": "queretaro/queretaro",
                "name": "Diseño Gráfico Querétaro",
                "max_pages": 2,
                "priority": 4
            },
            
            # CATEGORÍA 5: CONSULTORES
            {
                "id": "consultores_cdmx",
                "category": "consultores",
                "location": "distrito-federal/zona-metropolitana",
                "name": "Consultores CDMX",
                "max_pages": 7,
                "priority": 5
            },
            {
                "id": "consultores_edomex",
                "category": "consultores", 
                "location": "estado-de-mexico/toluca",
                "name": "Consultores Edo México",
                "max_pages": 4,
                "priority": 5
            },
            {
                "id": "consultores_queretaro",
                "category": "consultores",
                "location": "queretaro/queretaro",
                "name": "Consultores Querétaro",
                "max_pages": 3,
                "priority": 5
            }
        ]

    def load_state(self) -> Dict:
        """Cargar estado persistente"""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                logger.info(f"Estado cargado: {state}")
                return state
            else:
                # Estado inicial
                initial_state = {
                    "current_strategy_index": 0,
                    "current_page": 1,
                    "completed_strategies": [],
                    "last_execution": None,
                    "total_leads_obtained": 0,
                    "cycle_number": 1
                }
                self.save_state(initial_state)
                return initial_state
        except Exception as e:
            logger.error(f"Error cargando estado: {e}")
            return {
                "current_strategy_index": 0,
                "current_page": 1, 
                "completed_strategies": [],
                "last_execution": None,
                "total_leads_obtained": 0,
                "cycle_number": 1
            }

    def save_state(self, state: Dict):
        """Guardar estado persistente"""
        try:
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
            logger.info(f"Estado guardado: {state}")
        except Exception as e:
            logger.error(f"Error guardando estado: {e}")

    def get_next_strategy_and_page(self) -> Tuple[Dict, int]:
        """Obtener siguiente estrategia y página a scrapear"""
        strategies = self.get_rotation_strategies()
        
        # Verificar si completamos el ciclo
        if self.state["current_strategy_index"] >= len(strategies):
            logger.info("🔄 CICLO COMPLETADO - Reiniciando desde el principio")
            self.state["current_strategy_index"] = 0
            self.state["current_page"] = 1
            self.state["completed_strategies"] = []
            self.state["cycle_number"] += 1
            self.save_state(self.state)
        
        current_strategy = strategies[self.state["current_strategy_index"]]
        current_page = self.state["current_page"]
        
        logger.info(f"📍 Estrategia actual: {current_strategy['name']} - Página {current_page}")
        
        return current_strategy, current_page

    def advance_to_next_position(self, leads_found: int):
        """Avanzar a siguiente página o estrategia"""
        strategies = self.get_rotation_strategies()
        current_strategy = strategies[self.state["current_strategy_index"]]
        
        if leads_found == 0 or self.state["current_page"] >= current_strategy["max_pages"]:
            # No hay más leads o llegamos al máximo de páginas
            logger.info(f"✅ Completada estrategia: {current_strategy['name']}")
            
            self.state["completed_strategies"].append(current_strategy["id"])
            self.state["current_strategy_index"] += 1
            self.state["current_page"] = 1
        else:
            # Continuar con siguiente página de la misma estrategia
            self.state["current_page"] += 1
        
        self.state["last_execution"] = datetime.now().isoformat()
        self.save_state(self.state)

    async def execute_intelligent_scraping(self, target_leads: int = 20) -> Dict:
        """Ejecutar scraping inteligente con rotación"""
        try:
            logger.info("🚀 INICIANDO SCRAPING INTELIGENTE")
            
            # Obtener estrategia y página actual
            strategy, page = self.get_next_strategy_and_page()
            
            # Construir URL
            url = f"https://www.seccionamarilla.com.mx/resultados/{strategy['category']}/{strategy['location']}/{page}"
            
            logger.info(f"🎯 Scrapeando: {url}")
            
            # Ejecutar scraping
            leads = await self.scrape_page(url, strategy, page)
            
            # Actualizar estado y avanzar
            self.advance_to_next_position(len(leads))
            self.state["total_leads_obtained"] += len(leads)
            
            # Preparar respuesta
            result = {
                "timestamp": datetime.now().isoformat(),
                "strategy_used": strategy["name"], 
                "category": strategy["category"],
                "location": strategy["location"],
                "page_scraped": page,
                "leads_obtained": len(leads),
                "leads": leads,
                "system_state": {
                    "cycle_number": self.state["cycle_number"],
                    "total_leads_in_cycle": self.state["total_leads_obtained"], 
                    "current_strategy_index": self.state["current_strategy_index"],
                    "current_page": self.state["current_page"],
                    "strategies_completed": len(self.state["completed_strategies"]),
                    "next_strategy": self.get_next_strategy_preview()
                },
                "rotation_info": {
                    "intelligent_rotation": True,
                    "geographic_scope": "CDMX + Edo México + Querétaro",
                    "auto_cycle_reset": True,
                    "estimated_cycle_duration": "2-3 semanas"
                }
            }
            
            logger.info(f"✅ SCRAPING COMPLETADO: {len(leads)} leads obtenidos")
            logger.info(f"📊 Estado: Ciclo {self.state['cycle_number']}, Estrategia {self.state['current_strategy_index']}, Página {self.state['current_page']}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error en scraping inteligente: {e}")
            return {
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
                "leads_obtained": 0,
                "leads": []
            }

    def get_next_strategy_preview(self) -> str:
        """Obtener preview de próxima estrategia"""
        strategies = self.get_rotation_strategies()
        
        if self.state["current_strategy_index"] < len(strategies):
            next_strategy = strategies[self.state["current_strategy_index"]]
            return f"{next_strategy['name']} - Página {self.state['current_page']}"
        else:
            return "Inicio de nuevo ciclo - Marketing CDMX Página 1"

    async def scrape_page(self, url: str, strategy: Dict, page: int) -> List[Dict]:
        """Scrapear una página específica"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            leads = []
            
            # Usar selectores probados
            business_rows = soup.find_all('tr')
            
            for row in business_rows:
                lead = self.extract_lead_from_row(row, strategy)
                if lead:
                    leads.append(lead)
            
            # También buscar enlaces de teléfono
            phone_links = soup.find_all('a', href=re.compile(r'tel:'))
            for link in phone_links:
                lead = self.extract_lead_from_phone_link(link, soup, strategy)
                if lead:
                    leads.append(lead)
            
            # Deduplicar por teléfono
            unique_leads = self.deduplicate_leads(leads)
            
            return unique_leads
            
        except Exception as e:
            logger.error(f"Error scrapeando {url}: {e}")
            return []

    def extract_lead_from_row(self, row, strategy: Dict) -> Optional[Dict]:
        """Extraer lead de fila"""
        try:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 2:
                return None
            
            row_text = row.get_text(strip=True)
            
            # Skip filas irrelevantes
            skip_keywords = ['nombre', 'estatus', 'acciones', 'encuentra los mejores', 'buscar']
            if any(keyword in row_text.lower() for keyword in skip_keywords):
                return None
            
            # Extraer datos
            name = self.extract_business_name(cells)
            phone = self.extract_phone(row_text)
            address = self.extract_address(row)
            
            if name and phone and len(name) > 3:
                return {
                    'name': name,
                    'phone': phone,
                    'email': None,  # Se completará después
                    'address': address or strategy['location'],
                    'sector': strategy['name'],
                    'location': strategy['location'].split('/')[0],
                    'source': 'seccion_amarilla_intelligent',
                    'credit_potential': self.assess_credit_potential(strategy),
                    'estimated_revenue': self.estimate_revenue(strategy),
                    'loan_range': self.estimate_loan_range(strategy),
                    'extracted_at': datetime.now().isoformat(),
                    'strategy_used': strategy['name'],
                    'page_found': strategy.get('current_page', 1),
                    'cycle_number': self.state.get('cycle_number', 1)
                }
            
            return None
            
        except Exception as e:
            return None

    def extract_lead_from_phone_link(self, link, soup, strategy: Dict) -> Optional[Dict]:
        """Extraer lead de enlace telefónico"""
        try:
            phone = link.get('href').replace('tel:', '').strip()
            
            container = link.find_parent(['tr', 'div', 'td'])
            if container:
                name = self.find_business_name_in_container(container)
                
                if name and phone:
                    return {
                        'name': name,
                        'phone': phone,
                        'email': None,
                        'address': strategy['location'].split('/')[0],
                        'sector': strategy['name'],
                        'location': strategy['location'].split('/')[0],
                        'source': 'seccion_amarilla_intelligent',
                        'credit_potential': self.assess_credit_potential(strategy),
                        'estimated_revenue': self.estimate_revenue(strategy),
                        'loan_range': self.estimate_loan_range(strategy),
                        'extracted_at': datetime.now().isoformat(),
                        'strategy_used': strategy['name'],
                        'page_found': strategy.get('current_page', 1),
                        'cycle_number': self.state.get('cycle_number', 1)
                    }
            
            return None
            
        except Exception as e:
            return None

    def assess_credit_potential(self, strategy: Dict) -> str:
        """Evaluar potencial crediticio basado en sector"""
        high_potential = ['agencias-de-marketing', 'servicios-de-internet']
        
        if strategy['category'] in high_potential:
            return 'ALTO'
        elif strategy['priorit
