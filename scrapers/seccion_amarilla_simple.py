#!/usr/bin/env python3
"""
Sistema de Producción Constante de Leads
4,000 leads/mes = 133 leads/día distribuidos inteligentemente
"""

import asyncio
import time
import random
from typing import List, Dict, Optional, Tuple
import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime, timedelta
import re
from urllib.parse import urljoin, urlparse
import json
import hashlib

# Logger setup
logger = logging.getLogger(__name__)

class ProductionLeadSystem:
    """Sistema de producción constante de leads"""
    
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
        
        # Control de rotación y deduplicación
        self.processed_urls = set()
        self.daily_lead_count = 0
        self.current_strategy_index = 0
        self.last_reset_date = datetime.now().date()
        
        # Configuración de producción
        self.DAILY_TARGET = 133  # 4000/30 días
        self.BATCH_SIZE = 20     # Leads por request
        self.MIN_DELAY = 300     # 5 minutos entre batches
        self.MAX_DELAY = 900     # 15 minutos entre batches

    def get_production_strategies(self) -> List[Dict]:
        """Estrategias de producción secuencial - Solo CDMX, Edo Méx, Querétaro"""
        return [
            # CATEGORÍA 1: MARKETING Y PUBLICIDAD (Completar primero)
            {
                "name": "Marketing CDMX",
                "category": "agencias-de-marketing",
                "locations": ["distrito-federal", "zona-metropolitana"],
                "priority": 1,
                "expected_leads": 80,
                "status": "active"
            },
            {
                "name": "Marketing Edo México",
                "category": "agencias-de-marketing", 
                "locations": ["estado-de-mexico", "toluca"],
                "priority": 1,
                "expected_leads": 40,
                "status": "pending"
            },
            {
                "name": "Marketing Querétaro",
                "category": "agencias-de-marketing",
                "locations": ["queretaro", "queretaro"],
                "priority": 1,
                "expected_leads": 30,
                "status": "pending"
            },
            
            # CATEGORÍA 2: PUBLICIDAD (Después de marketing)
            {
                "name": "Publicidad CDMX",
                "category": "agencias-de-publicidad",
                "locations": ["distrito-federal", "zona-metropolitana"],
                "priority": 2,
                "expected_leads": 60,
                "status": "pending"
            },
            {
                "name": "Publicidad Edo México",
                "category": "agencias-de-publicidad",
                "locations": ["estado-de-mexico", "toluca"],
                "priority": 2,
                "expected_leads": 30,
                "status": "pending"
            },
            {
                "name": "Publicidad Querétaro",
                "category": "agencias-de-publicidad",
                "locations": ["queretaro", "queretaro"],
                "priority": 2,
                "expected_leads": 20,
                "status": "pending"
            },
            
            # CATEGORÍA 3: SERVICIOS INTERNET/TECNOLOGÍA
            {
                "name": "Servicios Internet CDMX",
                "category": "servicios-de-internet",
                "locations": ["distrito-federal", "zona-metropolitana"],
                "priority": 3,
                "expected_leads": 50,
                "status": "pending"
            },
            {
                "name": "Servicios Internet Edo México",
                "category": "servicios-de-internet",
                "locations": ["estado-de-mexico", "toluca"],
                "priority": 3,
                "expected_leads": 25,
                "status": "pending"
            },
            {
                "name": "Servicios Internet Querétaro",
                "category": "servicios-de-internet",
                "locations": ["queretaro", "queretaro"],
                "priority": 3,
                "expected_leads": 20,
                "status": "pending"
            },
            
            # CATEGORÍA 4: DISEÑO GRÁFICO
            {
                "name": "Diseño Gráfico CDMX",
                "category": "diseno-grafico",
                "locations": ["distrito-federal", "zona-metropolitana"],
                "priority": 4,
                "expected_leads": 40,
                "status": "pending"
            },
            {
                "name": "Diseño Gráfico Edo México",
                "category": "diseno-grafico",
                "locations": ["estado-de-mexico", "toluca"],
                "priority": 4,
                "expected_leads": 20,
                "status": "pending"
            },
            {
                "name": "Diseño Gráfico Querétaro",
                "category": "diseno-grafico",
                "locations": ["queretaro", "queretaro"],
                "priority": 4,
                "expected_leads": 15,
                "status": "pending"
            },
            
            # CATEGORÍA 5: CONSULTORES
            {
                "name": "Consultores CDMX",
                "category": "consultores",
                "locations": ["distrito-federal", "zona-metropolitana"],
                "priority": 5,
                "expected_leads": 50,
                "status": "pending"
            },
            {
                "name": "Consultores Edo México",
                "category": "consultores",
                "locations": ["estado-de-mexico", "toluca"],
                "priority": 5,
                "expected_leads": 25,
                "status": "pending"
            },
            {
                "name": "Consultores Querétaro",
                "category": "consultores",
                "locations": ["queretaro", "queretaro"],
                "priority": 5,
                "expected_leads": 20,
                "status": "pending"
            },
            
            # CATEGORÍA 6: SERVICIOS PROFESIONALES
            {
                "name": "Servicios Profesionales CDMX",
                "category": "servicios-profesionales",
                "locations": ["distrito-federal", "zona-metropolitana"],
                "priority": 6,
                "expected_leads": 45,
                "status": "pending"
            },
            {
                "name": "Servicios Profesionales Edo México",
                "category": "servicios-profesionales",
                "locations": ["estado-de-mexico", "toluca"],
                "priority": 6,
                "expected_leads": 20,
                "status": "pending"
            },
            {
                "name": "Servicios Profesionales Querétaro",
                "category": "servicios-profesionales",
                "locations": ["queretaro", "queretaro"],
                "priority": 6,
                "expected_leads": 15,
                "status": "pending"
            },
            
            # CATEGORÍA 7: MEDIOS DE COMUNICACIÓN
            {
                "name": "Medios Comunicación CDMX",
                "category": "medios-de-comunicacion",
                "locations": ["distrito-federal", "zona-metropolitana"],
                "priority": 7,
                "expected_leads": 35,
                "status": "pending"
            },
            {
                "name": "Medios Comunicación Edo México",
                "category": "medios-de-comunicacion",
                "locations": ["estado-de-mexico", "toluca"],
                "priority": 7,
                "expected_leads": 15,
                "status": "pending"
            },
            {
                "name": "Medios Comunicación Querétaro",
                "category": "medios-de-comunicacion",
                "locations": ["queretaro", "queretaro"],
                "priority": 7,
                "expected_leads": 10,
                "status": "pending"
            },
            
            # CATEGORÍA 8: ORGANIZACIÓN DE EVENTOS
            {
                "name": "Eventos CDMX",
                "category": "organizacion-de-eventos",
                "locations": ["distrito-federal", "zona-metropolitana"],
                "priority": 8,
                "expected_leads": 30,
                "status": "pending"
            },
            {
                "name": "Eventos Edo México",
                "category": "organizacion-de-eventos",
                "locations": ["estado-de-mexico", "toluca"],
                "priority": 8,
                "expected_leads": 15,
                "status": "pending"
            },
            {
                "name": "Eventos Querétaro",
                "category": "organizacion-de-eventos",
                "locations": ["queretaro", "queretaro"],
                "priority": 8,
                "expected_leads": 10,
                "status": "pending"
            }
        ]

    def _is_strategy_exhausted(self, strategy: Dict) -> bool:
        """Verificar si una estrategia está agotada (todas sus páginas procesadas)"""
        max_pages_to_check = 10  # Verificar hasta 10 páginas
        
        for page in range(1, max_pages_to_check + 1):
            url = self._build_strategy_url(strategy, page)
            if url not in self.processed_urls:
                return False  # Aún hay páginas sin procesar
        
        return True  # Todas las páginas están procesadas

    def _reset_all_strategies(self, strategies: List[Dict]):
        """Reiniciar todas las estrategias (limpiar URLs procesadas)"""
        logger.info("🔄 REINICIANDO TODAS LAS ESTRATEGIAS")
        self.processed_urls.clear()
        for strategy in strategies:
            strategy['status'] = 'active' if strategy['priority'] == 1 else 'pending'

    def get_strategy_progress_report(self) -> Dict:
        """Generar reporte de progreso por categoría"""
        strategies = self.get_production_strategies()
        
        # Agrupar por categoría (prioridad)
        categories = {}
        for strategy in strategies:
            priority = strategy['priority']
            if priority not in categories:
                categories[priority] = {
                    'priority': priority,
                    'strategies': [],
                    'total_expected': 0,
                    'completed_strategies': 0
                }
            
            categories[priority]['strategies'].append(strategy)
            categories[priority]['total_expected'] += strategy['expected_leads']
            
            if self._is_strategy_exhausted(strategy):
                categories[priority]['completed_strategies'] += 1
        
        # Calcular progreso
        report = {
            'total_categories': len(categories),
            'categories_detail': [],
            'current_active_category': None,
            'overall_progress': 0
        }
        
        completed_categories = 0
        
        for priority in sorted(categories.keys()):
            category = categories[priority]
            
            completion_rate = category['completed_strategies'] / len(category['strategies'])
            category_detail = {
                'category_number': priority,
                'category_name': self._get_category_name(priority),
                'strategies_count': len(category['strategies']),
                'completed_strategies': category['completed_strategies'],
                'completion_percentage': round(completion_rate * 100, 1),
                'expected_total_leads': category['total_expected'],
                'status': 'completed' if completion_rate == 1.0 else 'in_progress' if completion_rate > 0 else 'pending'
            }
            
            report['categories_detail'].append(category_detail)
            
            if completion_rate == 1.0:
                completed_categories += 1
            elif report['current_active_category'] is None and completion_rate < 1.0:
                report['current_active_category'] = category_detail
        
        report['overall_progress'] = round((completed_categories / len(categories)) * 100, 1)
        
        return report

    def _get_category_name(self, priority: int) -> str:
        """Obtener nombre de categoría por prioridad"""
        category_names = {
            1: "Marketing y Publicidad Digital",
            2: "Agencias de Publicidad", 
            3: "Servicios de Internet/Tecnología",
            4: "Diseño Gráfico",
            5: "Consultores",
            6: "Servicios Profesionales",
            7: "Medios de Comunicación",
            8: "Organización de Eventos"
        }
        return category_names.get(priority, f"Categoría {priority}")

    async def execute_daily_production(self, target_leads: int = None) -> Dict:
        """Ejecutar producción diaria con reporte de progreso de categorías"""
        if target_leads is None:
            target_leads = self.BATCH_SIZE
            
        # Reset contador diario si es necesario
        self._check_daily_reset()
        
        logger.info(f"🎯 INICIANDO PRODUCCIÓN DIARIA")
        logger.info(f"📊 Target: {target_leads} leads")
        logger.info(f"📈 Progreso diario: {self.daily_lead_count}/{self.DAILY_TARGET}")
        
        strategies = self.get_production_strategies()
        
        # Seleccionar estrategia basada en completar categoría actual
        selected_strategy = self._select_optimal_strategy(strategies, target_leads)
        
        # Obtener reporte de progreso
        progress_report = self.get_strategy_progress_report()
        
        logger.info(f"🎪 Estrategia seleccionada: {selected_strategy['name']}")
        logger.info(f"📂 Progreso general: {progress_report['overall_progress']}%")
        
        # Ejecutar scraping
        leads = await self._execute_strategy(selected_strategy, target_leads)
        
        # Actualizar contadores
        self.daily_lead_count += len(leads)
        
        # Preparar respuesta con progreso detallado
        result = {
            "timestamp": datetime.now().isoformat(),
            "strategy_used": selected_strategy['name'],
            "leads_obtained": len(leads),
            "daily_progress": f"{self.daily_lead_count}/{self.DAILY_TARGET}",
            "monthly_projection": self.daily_lead_count * 30,
            "geographic_scope": "CDMX, Estado de México, Querétaro",
            "category_progress": progress_report,
            "leads": leads,
            "next_recommended_delay": self._calculate_next_delay(),
            "production_health": self._assess_production_health()
        }
        
        logger.info(f"✅ PRODUCCIÓN COMPLETADA: {len(leads)} leads obtenidos")
        logger.info(f"📍 Ubicaciones: Solo CDMX, Edo Méx, Querétaro")
        
        return result

    def _select_optimal_strategy(self, strategies: List[Dict], target_leads: int) -> Dict:
        """Seleccionar estrategia - Completar categoría actual antes de pasar a siguiente"""
        
        # PASO 1: Buscar la categoría actual (más baja prioridad activa)
        current_category_priority = None
        for strategy in strategies:
            if strategy.get('status') == 'active' or not self._is_strategy_exhausted(strategy):
                if current_category_priority is None or strategy['priority'] < current_category_priority:
                    current_category_priority = strategy['priority']
                break
        
        if current_category_priority is None:
            # Todas las categorías completadas, volver a empezar
            current_category_priority = 1
            self._reset_all_strategies(strategies)
        
        # PASO 2: Filtrar estrategias de la categoría actual
        current_category_strategies = [
            s for s in strategies 
            if s['priority'] == current_category_priority
        ]
        
        # PASO 3: Seleccionar siguiente estrategia no agotada de la categoría
        for strategy in current_category_strategies:
            if not self._is_strategy_exhausted(strategy):
                logger.info(f"📂 Categoría activa: Prioridad {current_category_priority}")
                logger.info(f"🎯 Estrategia seleccionada: {strategy['name']}")
                return strategy
        
        # PASO 4: Si la categoría está completa, pasar a la siguiente
        logger.info(f"✅ Categoría {current_category_priority} COMPLETADA")
        next_priority = current_category_priority + 1
        
        next_category_strategies = [
            s for s in strategies 
            if s['priority'] == next_priority
        ]
        
        if next_category_strategies:
            selected = next_category_strategies[0]
            logger.info(f"🔄 PASANDO A NUEVA CATEGORÍA: Prioridad {next_priority}")
            logger.info(f"🎯 Nueva estrategia: {selected['name']}")
            return selected
        else:
            # No hay más categorías, reiniciar
            logger.info("🔄 TODAS LAS CATEGORÍAS COMPLETADAS - REINICIANDO CICLO")
            self._reset_all_strategies(strategies)
            return strategies[0]

    def _build_strategy_url(self, strategy: Dict, page: int = 1) -> str:
        """Construir URL para una estrategia"""
        category = strategy['category']
        location = strategy['locations'][0]
        
        return f"https://www.seccionamarilla.com.mx/resultados/{category}/{location}/{page}"

    async def _execute_strategy(self, strategy: Dict, target_leads: int) -> List[Dict]:
        """Ejecutar una estrategia específica"""
        all_leads = []
        
        try:
            # Calcular páginas necesarias
            expected_per_page = 10
            max_pages = min(5, (target_leads // expected_per_page) + 1)
            
            for page in range(1, max_pages + 1):
                url = self._build_strategy_url(strategy, page)
                
                # Marcar como procesada
                self.processed_urls.add(url)
                
                logger.info(f"🔍 Página {page}: {url}")
                
                page_leads = await self._scrape_page(url, strategy)
                
                if page_leads:
                    all_leads.extend(page_leads)
                    logger.info(f"✅ Página {page}: {len(page_leads)} leads")
                else:
                    logger.info(f"⚠️ Página {page}: Sin resultados")
                    break
                
                # Delay entre páginas
                await asyncio.sleep(random.uniform(2, 4))
                
                # Parar si alcanzamos target
                if len(all_leads) >= target_leads:
                    break
            
            # Limpiar y limitar resultados
            unique_leads = self._deduplicate_leads(all_leads)[:target_leads]
            
            return unique_leads
            
        except Exception as e:
            logger.error(f"Error ejecutando estrategia: {e}")
            return all_leads

    async def _scrape_page(self, url: str, strategy: Dict) -> List[Dict]:
        """Scrapear una página específica"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            leads = []
            
            # Usar selectores probados
            business_rows = soup.find_all('tr')
            
            for row in business_rows:
                lead = self._extract_lead_from_row(row, strategy)
                if lead:
                    leads.append(lead)
            
            # También buscar enlaces de teléfono
            phone_links = soup.find_all('a', href=re.compile(r'tel:'))
            for link in phone_links:
                lead = self._extract_lead_from_phone_link(link, soup, strategy)
                if lead:
                    leads.append(lead)
            
            return leads
            
        except Exception as e:
            logger.error(f"Error scrapeando {url}: {e}")
            return []

    def _extract_lead_from_row(self, row, strategy: Dict) -> Optional[Dict]:
        """Extraer lead de fila (misma lógica probada)"""
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
            name = self._extract_business_name(cells)
            phone = self._extract_phone(row_text)
            address = self._extract_address(row)
            
            if name and phone and len(name) > 3:
                return {
                    'name': name,
                    'phone': phone,
                    'email': None,
                    'address': address or strategy['locations'][0],
                    'sector': strategy['name'],
                    'location': strategy['locations'][0],
                    'source': 'seccion_amarilla_production',
                    'credit_potential': self._assess_credit_potential(strategy),
                    'estimated_revenue': self._estimate_revenue(strategy),
                    'loan_range': self._estimate_loan_range(strategy),
                    'extracted_at': datetime.now().isoformat(),
                    'strategy_used': strategy['name'],
                    'production_batch': f"batch_{datetime.now().strftime('%Y%m%d_%H%M')}"
                }
            
            return None
            
        except Exception as e:
            return None

    def _extract_lead_from_phone_link(self, link, soup, strategy: Dict) -> Optional[Dict]:
        """Extraer lead de enlace telefónico"""
        try:
            phone = link.get('href').replace('tel:', '').strip()
            
            container = link.find_parent(['tr', 'div', 'td'])
            if container:
                name = self._find_business_name_in_container(container)
                
                if name and phone:
                    return {
                        'name': name,
                        'phone': phone,
                        'email': None,
                        'address': strategy['locations'][0],
                        'sector': strategy['name'],
                        'location': strategy['locations'][0],
                        'source': 'seccion_amarilla_production',
                        'credit_potential': self._assess_credit_potential(strategy),
                        'estimated_revenue': self._estimate_revenue(strategy),
                        'loan_range': self._estimate_loan_range(strategy),
                        'extracted_at': datetime.now().isoformat(),
                        'strategy_used': strategy['name'],
                        'production_batch': f"batch_{datetime.now().strftime('%Y%m%d_%H%M')}"
                    }
            
            return None
            
        except Exception as e:
            return None

    def _assess_credit_potential(self, strategy: Dict) -> str:
        """Evaluar potencial crediticio basado en sector"""
        high_potential = ['agencias-de-marketing', 'servicios-de-internet', 'desarrollo-web']
        
        if strategy['category'] in high_potential:
            return 'ALTO'
        elif strategy['priority'] <= 2:
            return 'MEDIO-ALTO'
        else:
            return 'MEDIO'

    def _estimate_revenue(self, strategy: Dict) -> str:
        """Estimar ingresos basado en sector y ubicación"""
        if strategy['category'] in ['agencias-de-marketing', 'servicios-de-internet']:
            if 'distrito-federal' in strategy['locations']:
                return '$300,000 - $800,000'
            else:
                return '$200,000 - $500,000'
        else:
            return '$150,000 - $400,000'

    def _estimate_loan_range(self, strategy: Dict) -> str:
        """Estimar rango de préstamo"""
        if strategy['category'] in ['agencias-de-marketing', 'servicios-de-internet']:
            return '$75,000 - $2,000,000'
        else:
            return '$50,000 - $1,200,000'

    def _check_daily_reset(self):
        """Verificar si necesitamos reset diario"""
        today = datetime.now().date()
        if today > self.last_reset_date:
            self.daily_lead_count = 0
            self.last_reset_date = today
            self.processed_urls.clear()  # Reset URLs procesadas
            logger.info("🔄 RESET DIARIO EJECUTADO")

    def _calculate_next_delay(self) -> int:
        """Calcular delay óptimo para próxima ejecución"""
        remaining_today = self.DAILY_TARGET - self.daily_lead_count
        hours_left = 24 - datetime.now().hour
        
        if remaining_today <= 0:
            return 3600  # 1 hora si ya cumplimos
        
        if hours_left <= 0:
            return 1800  # 30 minutos si es muy tarde
        
        # Calcular delay dinámico
        optimal_delay = (hours_left * 3600) // max(1, remaining_today // self.BATCH_SIZE)
        
        return max(self.MIN_DELAY, min(self.MAX_DELAY, optimal_delay))

    def _assess_production_health(self) -> Dict:
        """Evaluar salud del sistema de producción"""
        daily_progress = self.daily_lead_count / self.DAILY_TARGET
        
        if daily_progress >= 1.0:
            status = "EXCELLENT"
            message = "✅ Meta diaria cumplida"
        elif daily_progress >= 0.8:
            status = "GOOD"
            message = "🟢 Ritmo óptimo"
        elif daily_progress >= 0.5:
            status = "FAIR"
            message = "🟡 Necesita aceleración"
        else:
            status = "POOR"
            message = "🔴 Requiere intervención"
        
        return {
            "status": status,
            "message": message,
            "daily_progress_percentage": round(daily_progress * 100, 1),
            "monthly_projection": self.daily_lead_count * 30
        }

    def _deduplicate_leads(self, leads: List[Dict]) -> List[Dict]:
        """Eliminar duplicados"""
        seen = set()
        unique = []
        
        for lead in leads:
            identifier = f"{lead.get('name', '').lower()}-{lead.get('phone', '')}"
            if identifier not in seen:
                seen.add(identifier)
                unique.append(lead)
        
        return unique

    # Métodos auxiliares (mantener existentes)
    def _extract_business_name(self, cells) -> Optional[str]:
        for cell in cells:
            text = cell.get_text(strip=True)
            if (len(text) > 3 and 
                not text.lower() in ['abierto', 'cerrado'] and
                not text.startswith('AV.') and
                not re.match(r'\(\d+\)', text)):
                return text
        return None

    def _extract_phone(self, text: str) -> Optional[str]:
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

    def _extract_address(self, row) -> Optional[str]:
        try:
            cells = row.find_all(['td', 'th'])
            for cell in cells:
                text = cell.get_text(strip=True)
                if any(indicator in text.upper() for indicator in ['AV.', 'CALLE', 'COL.']):
                    return text[:100]
            return None
        except:
            return None

    def _find_business_name_in_container(self, container) -> Optional[str]:
        try:
            for tag in ['h1', 'h2', 'h3', 'strong']:
                elem = container.find(tag)
                if elem:
                    text = elem.get_text(strip=True)
                    if len(text) > 3:
                        return text[:50]
            
            texts = container.find_all(text=True)
            for text in texts:
                text = text.strip()
                if (len(text) > 3 and 
                    not text.lower() in ['abierto', 'cerrado'] and
                    not text.startswith('AV.')):
                    return text[:50]
            
            return None
        except:
            return None

# Compatibilidad con sistema existente
class GoogleMapsLeadScraper:
    """Wrapper para compatibilidad"""
    def __init__(self):
        self.production_system = ProductionLeadSystem()

    def test_connection(self) -> bool:
        return self.production_system.session.get("https://www.google.com", timeout=10).status_code == 200

    async def test_single_search(self, sector: str, location: str, max_results: int = 1) -> List[Dict]:
        return await self.scrape_leads(sector, location, max_results)

    async def scrape_leads(self, sector: str, location: str, max_leads: int = 20) -> List[Dict]:
        """Scraping usando sistema de producción"""
        result = await self.production_system.execute_daily_production(max_leads)
        return result['leads']

def scrape_seccion_amarilla(url):
    """Función de compatibilidad"""
    scraper = GoogleMapsLeadScraper()
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        # Determinar leads a obtener basado en URL
        if 'marketing' in url:
            max_leads = 20  # Batch size estándar
        else:
            max_leads = 20
        
        results = loop.run_until_complete(scraper.scrape_leads('marketing', 'distrito-federal', max_leads))
        return results
    finally:
        loop.close()
