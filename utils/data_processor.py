#!/usr/bin/env python3
"""
Data Processor para Lead Scraper
Procesa, limpia y enriquece datos de leads
"""

import pandas as pd
import re
import json
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
import logging
from collections import Counter

logger = logging.getLogger(__name__)

class LeadProcessor:
    def __init__(self):
        # Palabras clave para detectar grandes empresas (excluir)
        self.big_company_keywords = [
            'oxxo', 'seven eleven', '7-eleven', 'soriana', 'walmart', 'chedraui',
            'liverpool', 'palacio de hierro', 'suburbia', 'coppel', 'elektra',
            'bancomer', 'banamex', 'santander', 'hsbc', 'scotiabank', 'bbva'
        ]
        
        # Patrones para limpiar datos
        self.phone_pattern = re.compile(r'\b(?:\+?52\s?)?(?:\d{2,3}[-\s]?\d{3,4}[-\s]?\d{4})\b')
        self.email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        
    def process_leads(self, raw_leads: List[Dict], filters: Optional[Dict] = None) -> List[Dict]:
        """Procesa lista de leads crudos"""
        try:
            logger.info(f"🔄 Procesando {len(raw_leads)} leads crudos")
            
            if not raw_leads:
                return []
            
            # 1. Limpiar y normalizar datos
            cleaned_leads = self._clean_leads(raw_leads)
            logger.info(f"🧹 Después de limpieza: {len(cleaned_leads)} leads")
            
            # 2. Eliminar duplicados
            unique_leads = self._remove_duplicates(cleaned_leads)
            logger.info(f"🔧 Después de eliminar duplicados: {len(unique_leads)} leads")
            
            # 3. Filtrar empresas no viables
            viable_leads = self._filter_viable_companies(unique_leads)
            logger.info(f"✅ Leads viables: {len(viable_leads)} leads")
            
            # 4. Aplicar filtros personalizados
            if filters:
                filtered_leads = self._apply_custom_filters(viable_leads, filters)
                logger.info(f"🎯 Después de filtros personalizados: {len(filtered_leads)} leads")
            else:
                filtered_leads = viable_leads
            
            # 5. Enriquecer con datos calculados
            enriched_leads = self._enrich_leads(filtered_leads)
            
            # 6. Calcular scores finales
            scored_leads = self._calculate_final_scores(enriched_leads)
            
            # 7. Ordenar por score
            final_leads = sorted(scored_leads, key=lambda x: x.get('final_score', 0), reverse=True)
            
            logger.info(f"🎉 Procesamiento completado: {len(final_leads)} leads finales")
            
            return final_leads
            
        except Exception as e:
            logger.error(f"❌ Error procesando leads: {e}")
            return raw_leads

    def _clean_leads(self, leads: List[Dict]) -> List[Dict]:
        """Limpia y normaliza datos básicos"""
        cleaned = []
        
        for lead in leads:
            try:
                cleaned_lead = {}
                
                # Limpiar nombre
                name = lead.get('name', '').strip()
                if name:
                    name = re.sub(r'[^\w\s\-\.\&]', '', name)
                    name = self._capitalize_business_name(name)
                    cleaned_lead['name'] = name
                
                # Limpiar teléfono
                phone = self._clean_phone(lead.get('phone', ''))
                if phone:
                    cleaned_lead['phone'] = phone
                
                # Limpiar email
                email = self._clean_email(lead.get('email', ''))
                if email:
                    cleaned_lead['email'] = email
                
                # Copiar otros campos importantes
                for field in ['sector', 'location', 'source', 'credit_potential', 
                             'address', 'website', 'extracted_at']:
                    if field in lead:
                        cleaned_lead[field] = lead[field]
                
                # Solo agregar si tiene información mínima
                if cleaned_lead.get('name') or cleaned_lead.get('phone'):
                    cleaned.append(cleaned_lead)
                
            except Exception as e:
                logger.warning(f"Error limpiando lead individual: {e}")
                continue
        
        return cleaned

    def _clean_phone(self, phone: str) -> str:
        """Limpia y valida números de teléfono"""
        if not phone:
            return ''
        
        cleaned = re.sub(r'[^\d+\s\-\(\)]', '', phone.strip())
        digits_only = re.sub(r'\D', '', cleaned)
        
        if 10 <= len(digits_only) <= 13:
            return cleaned
        
        return ''

    def _clean_email(self, email: str) -> str:
        """Limpia y valida emails"""
        if not email:
            return ''
        
        email = email.strip().lower()
        
        if re.match(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$', email, re.IGNORECASE):
            return email
        
        return ''

    def _capitalize_business_name(self, name: str) -> str:
        """Capitaliza correctamente nombres de negocios"""
        if not name:
            return ''
        
        lowercase_words = ['de', 'del', 'la', 'las', 'el', 'los', 'y', 'e', 'o']
        words = name.lower().split()
        capitalized_words = []
        
        for i, word in enumerate(words):
            if i == 0 or word not in lowercase_words:
                capitalized_words.append(word.capitalize())
            else:
                capitalized_words.append(word)
        
        return ' '.join(capitalized_words)

    def _remove_duplicates(self, leads: List[Dict]) -> List[Dict]:
        """Elimina leads duplicados"""
        unique_leads = []
        seen_phones = set()
        seen_names = set()
        
        for lead in leads:
            phone = lead.get('phone', '')
            name = lead.get('name', '').lower()
            
            is_duplicate = False
            
            if phone and phone in seen_phones:
                is_duplicate = True
            elif name and name in seen_names:
                is_duplicate = True
            
            if not is_duplicate:
                unique_leads.append(lead)
                
                if phone:
                    seen_phones.add(phone)
                if name:
                    seen_names.add(name)
        
        return unique_leads

    def _filter_viable_companies(self, leads: List[Dict]) -> List[Dict]:
        """Filtra empresas viables para crédito PyME"""
        viable = []
        
        for lead in leads:
            if self._is_viable_pyme(lead):
                viable.append(lead)
        
        return viable

    def _is_viable_pyme(self, lead: Dict) -> bool:
        """Determina si una empresa es viable para crédito PyME"""
        name = lead.get('name', '').lower()
        
        if not name or len(name) < 3:
            return False
        
        if any(keyword in name for keyword in self.big_company_keywords):
            return False
        
        has_contact = lead.get('phone') or lead.get('email') or lead.get('address')
        if not has_contact:
            return False
        
        return True

    def _apply_custom_filters(self, leads: List[Dict], filters: Dict) -> List[Dict]:
        """Aplica filtros personalizados"""
        filtered = leads.copy()
        
        try:
            if filters.get('sectors'):
                target_sectors = [s.lower() for s in filters['sectors']]
                filtered = [lead for lead in filtered 
                           if lead.get('sector', '').lower() in target_sectors]
            
            if filters.get('locations'):
                target_locations = [l.lower() for l in filters['locations']]
                filtered = [lead for lead in filtered 
                           if any(loc in lead.get('location', '').lower() 
                                 for loc in target_locations)]
            
        except Exception as e:
            logger.warning(f"Error aplicando filtros: {e}")
        
        return filtered

    def _enrich_leads(self, leads: List[Dict]) -> List[Dict]:
        """Enriquece leads con datos calculados"""
        enriched = []
        
        for lead in leads:
            try:
                enriched_lead = lead.copy()
                
                # Calcular completitud de datos
                enriched_lead['data_completeness'] = self._calculate_data_completeness(lead)
                
                # Determinar mejor método de contacto
                enriched_lead['preferred_contact'] = self._get_preferred_contact_method(lead)
                
                # Calcular urgencia de contacto
                enriched_lead['contact_urgency'] = self._calculate_contact_urgency(lead)
                
                enriched.append(enriched_lead)
                
            except Exception as e:
                logger.warning(f"Error enriqueciendo lead: {e}")
                enriched.append(lead)
        
        return enriched

    def _calculate_data_completeness(self, lead: Dict) -> float:
        """Calcula porcentaje de completitud de datos"""
        required_fields = ['name', 'phone', 'email', 'address', 'sector', 'location']
        completed_fields = sum(1 for field in required_fields if lead.get(field))
        
        return round((completed_fields / len(required_fields)) * 100, 1)

    def _get_preferred_contact_method(self, lead: Dict) -> str:
        """Determina el mejor método de contacto"""
        if lead.get('phone'):
            return 'WhatsApp'
        elif lead.get('email'):
            return 'Email'
        elif lead.get('website'):
            return 'Website'
        else:
            return 'Visita presencial'

    def _calculate_contact_urgency(self, lead: Dict) -> str:
        """Calcula urgencia de contacto"""
        score = 0
        
        credit_potential = lead.get('credit_potential', 'BAJO').upper()
        if credit_potential == 'ALTO':
            score += 3
        elif credit_potential == 'MEDIO':
            score += 2
        
        if lead.get('phone'):
            score += 2
        if lead.get('email'):
            score += 1
        
        if score >= 5:
            return 'ALTA'
        elif score >= 3:
            return 'MEDIA'
        else:
            return 'BAJA'

    def _calculate_final_scores(self, leads: List[Dict]) -> List[Dict]:
        """Calcula scores finales ponderados"""
        for lead in leads:
            try:
                score = 0.0
                
                # Score base por información de contacto
                if lead.get('phone'):
                    score += 40
                if lead.get('email'):
                    score += 20
                if lead.get('address'):
                    score += 10
                
                # Score por potencial de crédito
                credit_potential = lead.get('credit_potential', 'BAJO').upper()
                credit_score = {'ALTO': 30, 'MEDIO': 20, 'BAJO': 10}.get(credit_potential, 10)
                score += credit_score
                
                lead['final_score'] = round(score, 2)
                
            except Exception as e:
                logger.warning(f"Error calculando score final: {e}")
                lead['final_score'] = 0.0
        
        return leads

    def save_to_csv(self, leads: List[Dict], filename: str):
        """Guarda leads en archivo CSV"""
        try:
            if not leads:
                return
            
            df = pd.DataFrame(leads)
            df.to_csv(filename, index=False, encoding='utf-8')
            logger.info(f"✅ Leads guardados en CSV: {filename}")
            
        except Exception as e:
            logger.error(f"❌ Error guardando CSV: {e}")
