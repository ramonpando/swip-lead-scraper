import requests
from bs4 import BeautifulSoup
import time
import re
from urllib.parse import urljoin, urlparse
import json

class SeccionAmarillaScraperV2:
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
    
    def scrape_listings(self, url):
        """Scrape listings from Sección Amarilla"""
        try:
            print(f"Scraping URL: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Debug: Print page structure
            print("=== ESTRUCTURA DE LA PÁGINA ===")
            
            # Buscar diferentes tipos de contenedores
            containers = [
                soup.find_all('div', class_='container_out'),
                soup.find_all('div', class_='result-item'),
                soup.find_all('div', class_='listing'),
                soup.find_all('tr'),  # Tabla
                soup.find_all('div', class_='business-info'),
                soup.find_all('div', class_='empresa'),
                soup.find_all('article'),
                soup.find_all('div', attrs={'data-business': True}),
            ]
            
            print(f"Containers found: {[len(c) for c in containers]}")
            
            # Buscar enlaces que contengan información de negocios
            business_links = soup.find_all('a', href=True)
            print(f"Total links found: {len(business_links)}")
            
            # Filtrar enlaces que parezcan ser de negocios
            relevant_links = []
            for link in business_links:
                href = link.get('href', '')
                text = link.get_text(strip=True)
                if any(keyword in href.lower() for keyword in ['empresa', 'negocio', 'detalle', 'info']):
                    relevant_links.append({
                        'href': href,
                        'text': text
                    })
            
            print(f"Relevant business links: {len(relevant_links)}")
            
            # Buscar patrones de teléfono
            phone_pattern = r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b|\b\(\d{3}\)\s*\d{3}[-.]?\d{4}\b'
            all_text = soup.get_text()
            phones = re.findall(phone_pattern, all_text)
            print(f"Phones found: {phones[:5]}...")  # Mostrar los primeros 5
            
            # Buscar patrones de email
            email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            emails = re.findall(email_pattern, all_text)
            print(f"Emails found: {emails[:5]}...")  # Mostrar los primeros 5
            
            # Intentar múltiples selectores
            leads = []
            
            # Método 1: Buscar en filas de tabla
            table_rows = soup.find_all('tr')
            for row in table_rows:
                lead_data = self.extract_from_table_row(row)
                if lead_data:
                    leads.append(lead_data)
            
            # Método 2: Buscar en divs con clases específicas
            for container in soup.find_all('div'):
                classes = container.get('class', [])
                if any(cls in str(classes) for cls in ['result', 'listing', 'business', 'empresa']):
                    lead_data = self.extract_from_container(container)
                    if lead_data:
                        leads.append(lead_data)
            
            # Método 3: Buscar en enlaces con información relevante
            for link in relevant_links[:10]:  # Procesar los primeros 10
                try:
                    detail_url = urljoin(url, link['href'])
                    lead_data = self.scrape_business_detail(detail_url)
                    if lead_data:
                        leads.append(lead_data)
                except Exception as e:
                    print(f"Error scraping detail: {e}")
                    continue
            
            print(f"Total leads extracted: {len(leads)}")
            return leads
            
        except Exception as e:
            print(f"Error scraping: {e}")
            return []
    
    def extract_from_table_row(self, row):
        """Extract data from table row"""
        try:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 2:
                return None
            
            text = row.get_text(strip=True)
            
            # Buscar teléfono
            phone_match = re.search(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b|\b\(\d{3}\)\s*\d{3}[-.]?\d{4}\b', text)
            phone = phone_match.group() if phone_match else None
            
            # Buscar email
            email_match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
            email = email_match.group() if email_match else None
            
            # Buscar nombre (primer celda con texto significativo)
            name = None
            for cell in cells:
                cell_text = cell.get_text(strip=True)
                if len(cell_text) > 5 and not cell_text.lower() in ['abierto', 'cerrado', 'compartir', 'ruta']:
                    name = cell_text[:100]  # Limitar longitud
                    break
            
            if name and (phone or email):
                return {
                    'name': name,
                    'phone': phone,
                    'email': email,
                    'address': text[:200],  # Usar texto completo como dirección
                    'source': 'seccion_amarilla',
                    'extraction_method': 'table_row'
                }
            
            return None
            
        except Exception as e:
            print(f"Error extracting from table row: {e}")
            return None
    
    def extract_from_container(self, container):
        """Extract data from container div"""
        try:
            text = container.get_text(strip=True)
            
            # Buscar teléfono
            phone_match = re.search(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b|\b\(\d{3}\)\s*\d{3}[-.]?\d{4}\b', text)
            phone = phone_match.group() if phone_match else None
            
            # Buscar email
            email_match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
            email = email_match.group() if email_match else None
            
            # Buscar nombre en título o enlace
            name = None
            title_elements = container.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'a'])
            for elem in title_elements:
                elem_text = elem.get_text(strip=True)
                if len(elem_text) > 5 and not elem_text.lower() in ['abierto', 'cerrado', 'compartir', 'ruta']:
                    name = elem_text[:100]
                    break
            
            if not name:
                # Usar la primera línea significativa
                lines = [line.strip() for line in text.split('\n') if line.strip()]
                for line in lines:
                    if len(line) > 5 and not line.lower() in ['abierto', 'cerrado', 'compartir', 'ruta']:
                        name = line[:100]
                        break
            
            if name and (phone or email):
                return {
                    'name': name,
                    'phone': phone,
                    'email': email,
                    'address': text[:200],
                    'source': 'seccion_amarilla',
                    'extraction_method': 'container'
                }
            
            return None
            
        except Exception as e:
            print(f"Error extracting from container: {e}")
            return None
    
    def scrape_business_detail(self, url):
        """Scrape individual business detail page"""
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extraer información del detalle
            name = None
            phone = None
            email = None
            address = None
            
            # Buscar título
            title_selectors = ['h1', 'h2', '.business-name', '.company-name', 'title']
            for selector in title_selectors:
                elem = soup.select_one(selector)
                if elem:
                    name = elem.get_text(strip=True)
                    break
            
            # Buscar teléfono
            text = soup.get_text()
            phone_match = re.search(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b|\b\(\d{3}\)\s*\d{3}[-.]?\d{4}\b', text)
            phone = phone_match.group() if phone_match else None
            
            # Buscar email
            email_match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
            email = email_match.group() if email_match else None
            
            if name and (phone or email):
                return {
                    'name': name,
                    'phone': phone,
                    'email': email,
                    'address': address,
                    'source': 'seccion_amarilla',
                    'extraction_method': 'detail_page'
                }
            
            return None
            
        except Exception as e:
            print(f"Error scraping business detail: {e}")
            return None

# Función para usar en el endpoint
def scrape_seccion_amarilla(url):
    scraper = SeccionAmarillaScraperV2()
    return scraper.scrape_listings(url)

# Test
if __name__ == "__main__":
    test_url = "https://www.seccionamarilla.com.mx/resultados/agencias-de-marketing/distrito-federal/zona-metropolitana/1"
    results = scrape_seccion_amarilla(test_url)
    print(f"Results: {json.dumps(results, indent=2)}")
