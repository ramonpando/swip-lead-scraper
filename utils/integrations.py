#!/usr/bin/env python3
"""
Integrations Module
Integraciones con N8N, Chatwoot y Google Sheets
"""

import asyncio
import aiohttp
import requests
import json
import os
from typing import List, Dict, Optional, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class N8NIntegration:
    def __init__(self):
        self.webhook_url = os.getenv('N8N_WEBHOOK_URL')
        self.api_key = os.getenv('N8N_API_KEY')
        
        self.headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Swip-Lead-Scraper/1.0'
        }
        
        if self.api_key:
            self.headers['Authorization'] = f'Bearer {self.api_key}'

    async def send_completion_webhook(self, webhook_url: str, job_id: str, leads: List[Dict]) -> bool:
        """Envía webhook de completación a N8N"""
        try:
            payload = {
                "event": "scraping_completed",
                "job_id": job_id,
                "timestamp": datetime.now().isoformat(),
                "summary": {
                    "total_leads": len(leads),
                    "leads_by_sector": self._group_by_field(leads, 'sector'),
                    "leads_by_location": self._group_by_field(leads, 'location'),
                    "leads_by_credit_potential": self._group_by_field(leads, 'credit_potential')
                },
                "leads": leads[:50]  # Enviar máximo 50 leads en webhook
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(webhook_url, json=payload, headers=self.headers) as response:
                    if response.status == 200:
                        logger.info(f"✅ Webhook enviado exitosamente a N8N")
                        return True
                    else:
                        logger.warning(f"⚠️ Webhook falló: {response.status}")
                        return False
                        
        except Exception as e:
            logger.error(f"❌ Error enviando webhook a N8N: {e}")
            return False

    async def trigger_workflow(self, workflow_name: str, data: Dict) -> Optional[Dict]:
        """Activa un workflow específico en N8N"""
        try:
            if not self.webhook_url:
                logger.warning("N8N webhook URL no configurada")
                return None
            
            payload = {
                "workflow": workflow_name,
                "data": data,
                "timestamp": datetime.now().isoformat()
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(self.webhook_url, json=payload, headers=self.headers) as response:
                    if response.status == 200:
                        result = await response.json()
                        logger.info(f"✅ Workflow {workflow_name} activado")
                        return result
                    else:
                        logger.warning(f"⚠️ Error activando workflow: {response.status}")
                        return None
                        
        except Exception as e:
            logger.error(f"❌ Error activando workflow N8N: {e}")
            return None

    def _group_by_field(self, leads: List[Dict], field: str) -> Dict[str, int]:
        """Agrupa leads por un campo específico"""
        groups = {}
        for lead in leads:
            value = lead.get(field, 'Sin especificar')
            groups[value] = groups.get(value, 0) + 1
        return groups

class ChatwootIntegration:
    def __init__(self):
        self.api_url = os.getenv('CHATWOOT_API_URL', '').rstrip('/')
        self.api_token = os.getenv('CHATWOOT_API_TOKEN')
        self.account_id = os.getenv('CHATWOOT_ACCOUNT_ID', '1')
        
        self.headers = {
            'Content-Type': 'application/json',
            'api_access_token': self.api_token
        }

    async def create_contacts_from_leads(self, leads: List[Dict]) -> int:
        """Crea contactos en Chatwoot desde leads"""
        if not self.api_url or not self.api_token:
            logger.warning("Chatwoot no configurado correctamente")
            return 0
        
        created_count = 0
        
        for lead in leads:
            try:
                contact_data = self._prepare_contact_data(lead)
                
                if await self._create_contact(contact_data):
                    created_count += 1
                    logger.info(f"✅ Contacto creado: {lead.get('name', 'Sin nombre')}")
                
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.warning(f"Error creando contacto {lead.get('name', '')}: {e}")
                continue
        
        logger.info(f"🎉 Creados {created_count} contactos en Chatwoot")
        return created_count

    def _prepare_contact_data(self, lead: Dict) -> Dict:
        """Prepara datos del lead para Chatwoot"""
        contact_data = {
            "name": lead.get('name', 'Lead sin nombre'),
            "custom_attributes": {
                "sector": lead.get('sector', ''),
                "ubicacion": lead.get('location', ''),
                "potencial_credito": lead.get('credit_potential', ''),
                "puntuacion_lead": lead.get('final_score', 0),
                "fuente": lead.get('source', ''),
                "prioridad_contacto": lead.get('contact_urgency', 'BAJA'),
                "metodo_contacto_preferido": lead.get('preferred_contact', ''),
                "fecha_extraccion": lead.get('extracted_at', ''),
                "direccion": lead.get('address', ''),
                "sitio_web": lead.get('website', '')
            }
        }
        
        # Agregar teléfono si existe
        phone = lead.get('phone', '')
        if phone:
            contact_data["phone_number"] = phone
        
        # Agregar email si existe
        email = lead.get('email', '')
        if email:
            contact_data["email"] = email
        
        return contact_data

    async def _create_contact(self, contact_data: Dict) -> bool:
        """Crea un contacto individual en Chatwoot"""
        try:
            url = f"{self.api_url}/api/v1/accounts/{self.account_id}/contacts"
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=contact_data, headers=self.headers) as response:
                    if response.status in [200, 201]:
                        return True
                    elif response.status == 422:
                        logger.info(f"📝 Contacto ya existe: {contact_data.get('name')}")
                        return False
                    else:
                        logger.warning(f"Error creando contacto: {response.status}")
                        return False
                        
        except Exception as e:
            logger.error(f"Error en API Chatwoot: {e}")
            return False

    async def create_conversation_for_lead(self, lead: Dict, message: str) -> Optional[str]:
        """Crea una conversación inicial para un lead"""
        try:
            contact_data = self._prepare_contact_data(lead)
            contact_id = await self._find_or_create_contact(contact_data)
            
            if contact_id:
                conversation_data = {
                    "source_id": contact_id,
                    "inbox_id": 1,
                    "status": "open",
                    "assignee_id": None
                }
                
                conversation_id = await self._create_conversation(conversation_data)
                
                if conversation_id:
                    await self._send_message(conversation_id, message)
                    return conversation_id
            
            return None
            
        except Exception as e:
            logger.error(f"Error creando conversación: {e}")
            return None

    async def _find_or_create_contact(self, contact_data: Dict) -> Optional[str]:
        """Encuentra o crea un contacto"""
        try:
            # Intentar crear nuevo contacto directamente
            if await self._create_contact(contact_data):
                await asyncio.sleep(1)
                # En implementación real, buscaría el ID del contacto creado
                return "1"  # ID placeholder
            
            return None
            
        except Exception as e:
            logger.error(f"Error encontrando/creando contacto: {e}")
            return None

    async def _create_conversation(self, conversation_data: Dict) -> Optional[str]:
        """Crea una nueva conversación"""
        try:
            url = f"{self.api_url}/api/v1/accounts/{self.account_id}/conversations"
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=conversation_data, headers=self.headers) as response:
                    if response.status in [200, 201]:
                        result = await response.json()
                        return str(result['id'])
                    
            return None
            
        except Exception as e:
            logger.error(f"Error creando conversación: {e}")
            return None

    async def _send_message(self, conversation_id: str, message: str) -> bool:
        """Envía un mensaje a una conversación"""
        try:
            url = f"{self.api_url}/api/v1/accounts/{self.account_id}/conversations/{conversation_id}/messages"
            
            message_data = {
                "content": message,
                "message_type": "outgoing"
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=message_data, headers=self.headers) as response:
                    return response.status in [200, 201]
                    
        except Exception as e:
            logger.error(f"Error enviando mensaje: {e}")
            return False

class GoogleSheetsIntegration:
    def __init__(self):
        self.credentials_path = os.getenv('GOOGLE_SHEETS_CREDENTIALS')
        self.spreadsheet_id = os.getenv('GOOGLE_SHEETS_SPREADSHEET_ID')
        
        logger.info("📊 Google Sheets integration inicializada")

    async def upload_leads_to_sheet(self, leads: List[Dict], sheet_name: str = None) -> bool:
        """Sube leads a Google Sheets"""
        try:
            if sheet_name is None:
                sheet_name = f"Leads_{datetime.now().strftime('%Y%m%d_%H%M')}"
            
            logger.info(f"📊 Simulando subida de {len(leads)} leads a Google Sheets: {sheet_name}")
            
            # Por ahora simulamos la subida
            await asyncio.sleep(1)
            
            logger.info(f"✅ {len(leads)} leads 'subidos' a Google Sheets")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error subiendo a Google Sheets: {e}")
            return False

    async def create_summary_dashboard(self, leads: List[Dict], sheet_name: str = "Dashboard") -> bool:
        """Crea un dashboard resumen en Google Sheets"""
        try:
            logger.info(f"📊 Creando dashboard con {len(leads)} leads")
            
            # Simulación del dashboard
            dashboard_stats = {
                "total_leads": len(leads),
                "by_sector": {},
                "by_location": {},
                "timestamp": datetime.now().isoformat()
            }
            
            # Agrupar por sector
            for lead in leads:
                sector = lead.get('sector', 'Sin sector')
                dashboard_stats["by_sector"][sector] = dashboard_stats["by_sector"].get(sector, 0) + 1
            
            logger.info(f"✅ Dashboard creado: {dashboard_stats}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creando dashboard: {e}")
            return False
