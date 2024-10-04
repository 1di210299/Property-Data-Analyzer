import requests
import zipfile
import os
import csv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import time
import re
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import h3
import overpy
from geopy.distance import geodesic
from geopy.geocoders import Nominatim, GoogleV3
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import random
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from dotenv import load_dotenv

load_dotenv()
as_of_date = "2024-09-29"
username = "xxx"
hash_value = "xxx"

headers = {
    "username": username,
    "hash_value": hash_value
}

def download_broadband_data():
    print("Obteniendo fechas disponibles...")
    url_dates = "https://broadbandmap.fcc.gov/api/public/map/listAsOfDates"
    response_dates = requests.get(url_dates, headers=headers)

    if response_dates.status_code == 200:
        dates_data = response_dates.json()
        latest_date = max([date['as_of_date'] for date in dates_data['data'] if date['data_type'] == "availability"])
        print(f"Fecha más reciente de actualización de datos: {latest_date}")
    else:
        print(f"Error {response_dates.status_code}: {response_dates.text}")
        return None

    print("\nObteniendo lista de archivos para la fecha más reciente...")
    url_availability = f"https://broadbandmap.fcc.gov/api/public/map/downloads/listAvailabilityData/{latest_date}"
    response_files = requests.get(url_availability, headers=headers)

    if response_files.status_code == 200:
        files_data = response_files.json()['data']
        fttp_files_nc = [
            file for file in files_data
            if file['file_type'].lower() == 'csv'
            and file['state_fips'] in ['37', '037']
            and file['technology_code_desc'] is not None
            and 'Fiber to the Premises' in file['technology_code_desc']
        ]
        
        if not fttp_files_nc:
            print("No se encontraron archivos de Fiber to the Premises (FTTP) disponibles para Carolina del Norte (NC).")
            return None
        
        latest_fttp_file = fttp_files_nc[0]
        file_id = latest_fttp_file['file_id']
        file_name = latest_fttp_file['file_name']
        print(f"Archivo de FTTP más reciente encontrado para NC: {file_name}")
    else:
        print(f"Error {response_files.status_code}: {response_files.text}")
        return None

    print(f"\nDescargando el archivo '{file_name}'...")
    data_type = "availability"
    url_download = f"https://broadbandmap.fcc.gov/api/public/map/downloads/downloadFile/{data_type}/{file_id}"

    response_download = requests.get(url_download, headers=headers, stream=True)

    if response_download.status_code == 200:
        zip_filename = f"{file_name}.zip"
        with open(zip_filename, 'wb') as file:
            for chunk in response_download.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"Archivo descargado y guardado como '{zip_filename}'")

        if zipfile.is_zipfile(zip_filename):
            print("Extrayendo el archivo zip...")
            with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
                extract_dir = zip_filename.replace('.zip', '')
                zip_ref.extractall(extract_dir)
            
            extracted_files = os.listdir(extract_dir)
            csv_files = [file for file in extracted_files if file.endswith('.csv')]
            
            if csv_files:
                csv_file_path = os.path.join(extract_dir, csv_files[0])
                print(f"Archivo CSV encontrado: {csv_files[0]}")
                return csv_file_path
            else:
                print("No se encontraron archivos CSV dentro del archivo zip extraído.")
                return None
        else:
            print("El archivo descargado no es un archivo zip.")
            return None
    else:
        print(f"Error {response_download.status_code}: {response_download.text}")
        return None

def extract_property_data():
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)

    urls = [
        "https://www.homes.com/cary-nc/?ls-min=7000sf&property_type=1,32,64&price-max=550000",
    "https://www.homes.com/apex-nc/?ls-min=7000sf&property_type=1,32,64&price-max=550000",
    "https://www.homes.com/durham-nc/?bb=m47qv47szHiv16-pwD&ls-min=7000sf&property_type=1,32,64&price-max=550000",
    "https://www.homes.com/fuquay-varina-nc/?ls-min=7000sf&property_type=1,32,64&price-max=550000",
    "https://www.homes.com/garner-nc/?ls-min=7000sf&property_type=1,32,64&price-max=550000",
    "https://www.homes.com/holly-springs-nc/?ls-min=7000sf&property_type=1,32,64&price-max=550000",
    'https://www.homes.com/knightdale-nc/?ls-min=7000sf&property_type=1,32,64&price-max=550000',
    "https://www.homes.com/morrisville-nc/?ls-min=7000sf&property_type=1,32,64&price-max=550000",
    "https://www.homes.com/raleigh-nc/?ls-min=7000sf&property_type=1,32,64&price-max=550000",
    "https://www.homes.com/rolesville-nc/?ls-min=7000sf&property_type=1,32,64&price-max=550000",
    "https://www.homes.com/wake-forest-nc/?ls-min=7000sf&property_type=1,32,64&price-max=550000",
    "https://www.homes.com/wendell-nc/?ls-min=7000sf&property_type=1,32,64&price-max=550000",
    "https://www.homes.com/willow-spring-nc/?ls-min=7000sf&property_type=1,32,64&price-max=550000",
    "https://www.homes.com/zebulon-nc/?ls-min=7000sf&property_type=1,32,64&price-max=550000",
    "https://www.homes.com/apex-nc/new-hill-neighborhood/?ls-min=7000sf&property_type=1,32,64&price-max=550000"
]


    csv_filename = 'properties.csv'

    with open(csv_filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["City", "Price", "Beds", "Baths", "Size", "Address", "Acres", "Description", "Agent"])

    def extract_house_data(house_element, city):
        listing_details = house_element.text
        print(f"Detalles encontrados en la lista: {listing_details}")
        
        price = "N/A"
        beds = "N/A"
        baths = "N/A"
        size = "N/A"
        address = "N/A"
        acres_value = "N/A"
        description = "N/A"
        agent = "N/A"

        lines = listing_details.split("\n")
        for line in lines:
            if re.search(r'\$[\d,]+', line):
                price_match = re.search(r'\$[\d,]+', line)
                if price_match:
                    price = price_match.group(0)
            elif "bed" in line.lower():
                beds = line.split()[0]
            elif "bath" in line.lower():
                baths = line.split()[0]
            elif "sqft" in line.lower():
                size = line.split()[0]
        
        try:
            address = lines[4] if len(lines) > 4 else "N/A"
            description = " ".join(lines[5:-1]) if len(lines) > 6 else "N/A"
            agent = lines[-1] if len(lines) > 0 else "N/A"
        except IndexError:
            pass

        original_window = driver.current_window_handle
        house_element.click()
        time.sleep(2)

        for handle in driver.window_handles:
            if handle != original_window:
                driver.switch_to.window(handle)
                break

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "amenities-container"))
        )

        amenities_container = driver.find_element(By.ID, "amenities-container")
        all_text = amenities_container.text.split("\n")

        for line in all_text:
            if "Acre Lot" in line and acres_value == "N/A":
                acres_value = line.split()[0]
            if "Bedrooms" in line and beds == "N/A":
                beds = line.split(' ')[0]
            if "Bathrooms" in line and baths == "N/A":
                baths = line.split(' ')[0]
            if "Sq Ft" in line and size == "N/A":
                size = line.split(' ')[0]
        
        if price == "N/A":
            try:
                price_text = driver.find_element(By.CLASS_NAME, 'price-container').text
                price_match = re.search(r'\$[\d,]+', price_text)
                if price_match:
                    price = price_match.group(0)
            except NoSuchElementException:
                pass

        driver.close()
        driver.switch_to.window(original_window)

        return [price, beds, baths, size, address, acres_value, description, agent]

    def process_page(city):
        index = 1
        consecutive_failures = 0
        print(f"Procesando página actual en {city}...")

        while True:
            try:
                if consecutive_failures >= 2:
                    print(f"No se encontraron casas en 2 intentos consecutivos en {city}. Pasando al siguiente enlace.")
                    return False

                xpath = f'//*[@id="placardContainer"]/div[2]/ul/li[{index}]/article/div[3]'
                print(f"Buscando la casa {index} con XPath: {xpath}")
                
                house_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, xpath))
                )
                
                house_data = extract_house_data(house_element, city)
                
                with open(csv_filename, 'a', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    writer.writerow([city] + house_data)
                
                print(f"Casa {index} en {city} - Datos guardados")

                time.sleep(3)
                index += 1
                consecutive_failures = 0

            except TimeoutException:
                print(f"No se encontró la casa {index} en {city}.")
                consecutive_failures += 1
                index += 1
            except Exception as e:
                print(f"Error al procesar la casa {index} en {city}: {e}")
                consecutive_failures += 1
                index += 1

        return True

    def has_next_page():
        try:
            next_button = driver.find_element(By.XPATH, '//*[@id="paging"]/ol/li[last()]/button')
            return not "disabled" in next_button.get_attribute("class")
        except NoSuchElementException:
            return False

    def go_to_next_page():
        try:
            next_button = driver.find_element(By.XPATH, '//*[@id="paging"]/ol/li[last()]/button')
            next_button.click()
            time.sleep(3)
            return True
        except NoSuchElementException:
            return False

    for url in urls:
        print(f"Abriendo la página web: {url}")
        driver.get(url)
        driver.implicitly_wait(10)

        city = re.search(r'/([^/]+)-nc/', url).group(1).capitalize()

        page_number = 1
        while True:
            print(f"Procesando página {page_number} en {city}...")
            if not process_page(city):
                break
            
            if has_next_page():
                if go_to_next_page():
                    page_number += 1
                else:
                    print(f"No se pudo avanzar a la siguiente página en {city}.")
                    break
            else:
                print(f"No hay más páginas en {city}. Pasando a la siguiente ciudad.")
                break

    print(f"Extracción completada y datos guardados en '{csv_filename}'.")
    driver.quit()
    return csv_filename

def geocode_addresses(input_csv):
    nominatim = Nominatim(user_agent="my_geocoder_script", timeout=10)
    google_api_key = 'AIzaSyD1GczOL5XT_sE24Yj6RnIOMAxxw0OhXCc'
    google = GoogleV3(api_key=google_api_key)

    output_csv = 'properties_with_geo.csv'

    def geocode_with_retry(address, geocoder, max_attempts=5):
        for attempt in range(max_attempts):
            try:
                location = geocoder.geocode(address)
                return location
            except (GeocoderTimedOut, GeocoderServiceError) as e:
                if attempt == max_attempts - 1:
                    print(f"Error al geocodificar después de {max_attempts} intentos: {e}")
                    return None
                else:
                    wait = 2 ** (attempt + 1) + random.random() * 5
                    print(f"Intento {attempt + 1} falló. Esperando {wait:.2f} segundos...")
                    time.sleep(wait)

    with open(input_csv, 'r', newline='', encoding='utf-8') as infile, \
         open(output_csv, 'w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ['Latitude', 'Longitude', 'Source']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            address = row['Address']
            print(f"Geocodificando la dirección: {address}")
            
            location = geocode_with_retry(address, nominatim)
            
            if location:
                row['Latitude'] = location.latitude
                row['Longitude'] = location.longitude
                row['Source'] = 'Nominatim'
                print(f"Geolocalización encontrada con Nominatim - Latitud: {location.latitude}, Longitud: {location.longitude}")
            else:
                print("Nominatim falló. Intentando con Google Maps API...")
                location = geocode_with_retry(address, google)
                if location:
                    row['Latitude'] = location.latitude
                    row['Longitude'] = location.longitude
                    row['Source'] = 'Google'
                    print(f"Geolocalización encontrada con Google - Latitud: {location.latitude}, Longitud: {location.longitude}")
                else:
                    row['Latitude'] = 'N/A'
                    row['Longitude'] = 'N/A'
                    row['Source'] = 'N/A'
                    print("No se pudo geocodificar la dirección con ningún servicio.")
            
            writer.writerow(row)
            time.sleep(2 + random.random() * 3)

    print(f"Proceso completado. Resultados guardados en '{output_csv}'.")
    return output_csv

def find_internet_providers(properties_file, providers_file):
    def h3_to_point(h3_address):
        lat, lon = h3.h3_to_geo(h3_address)
        return Point(lon, lat)

    print("Cargando datos de proveedores de internet...")
    providers_df = pd.read_csv(providers_file)
    providers_df = providers_df[providers_df['technology'] == 50]

    print("Creando GeoDataFrame para proveedores...")
    providers_df['geometry'] = providers_df['h3_res8_id'].apply(h3_to_point)
    providers_gdf = gpd.GeoDataFrame(providers_df, geometry='geometry', crs="EPSG:4326")

    print("Cargando datos de propiedades...")
    properties_df = pd.read_csv(properties_file, encoding='utf-8', engine='python')
    print(f"Número total de propiedades cargadas: {len(properties_df)}")

    print("Creando GeoDataFrame para propiedades...")
    properties_geometry = [Point(xy) for xy in zip(properties_df['Longitude'], properties_df['Latitude'])]
    properties_gdf = gpd.GeoDataFrame(properties_df, geometry=properties_geometry, crs="EPSG:4326")

    providers_gdf = providers_gdf.to_crs(epsg=26917)
    properties_gdf = properties_gdf.to_crs(epsg=26917)

    print("Encontrando los proveedores más cercanos para cada propiedad...")
    def find_nearest_providers(property_point, max_distance=1000, n=3):
        distances = providers_gdf.distance(property_point)
        valid_distances = distances[distances <= max_distance].dropna()
        if valid_distances.empty:
            return 'No providers found'
        nearest_indices = valid_distances.nsmallest(n).index
        return ', '.join(providers_gdf.loc[nearest_indices, 'brand_name'].unique())

    properties_gdf['nearest_providers'] = properties_gdf['geometry'].apply(find_nearest_providers)

    def get_max_speeds(property_point, max_distance=1000):
        distances = providers_gdf.distance(property_point)
        valid_distances = distances[distances <= max_distance].dropna()
        if valid_distances.empty:
            return pd.Series({'max_download_speed': 'No data', 'max_upload_speed': 'No data'})
        
        nearest_index = valid_distances.idxmin()
        download_speed = providers_gdf.loc[nearest_index, 'max_advertised_download_speed']
        upload_speed = providers_gdf.loc[nearest_index, 'max_advertised_upload_speed']
        return pd.Series({'max_download_speed': download_speed, 'max_upload_speed': upload_speed})

    properties_gdf[['max_download_speed', 'max_upload_speed']] = properties_gdf['geometry'].apply(get_max_speeds)

    properties_gdf = properties_gdf.to_crs(epsg=4326)

    output_file = 'properties_with_fiber_internet_providers.csv'
    print(f"Guardando resultados en {output_file}...")
    columns_to_save = ['Price', 'Beds', 'Baths', 'Size', 'Address', 'Acres', 'Description', 'Agent', 'Latitude', 'Longitude', 'nearest_providers', 'max_download_speed', 'max_upload_speed']
    properties_gdf[columns_to_save].to_csv(output_file, index=False, encoding='utf-8')

    print(f"\nAnálisis completado. Resultados guardados en '{output_file}'")
    return output_file

def find_nearby_amenities(input_file):
    api = overpy.Overpass()
    df = pd.read_csv(input_file)
    df = df.dropna(subset=['Latitude', 'Longitude'])

    search_radius = 5000

    starbucks_data = []
    schools_data = []

    for index, row in df.iterrows():
        latitude = row['Latitude']
        longitude = row['Longitude']
        
        starbucks_query = f"""
        [out:json][timeout:120];
        (
          node["amenity"="cafe"]["name"="Starbucks"](around:{search_radius},{latitude},{longitude});
          way["amenity"="cafe"]["name"="Starbucks"](around:{search_radius},{latitude},{longitude});
          relation["amenity"="cafe"]["name"="Starbucks"](around:{search_radius},{latitude},{longitude});
        );
        out center;
        """
        
        schools_query = f"""
        [out:json][timeout:120];
        (
          node["amenity"="school"](around:{search_radius},{latitude},{longitude});
          way["amenity"="school"](around:{search_radius},{latitude},{longitude});
          relation["amenity"="school"](around:{search_radius},{latitude},{longitude});
        );
        out center;
        """
        
        try:
            starbucks_result = api.query(starbucks_query)
            
            nearest_starbucks = None
            min_distance = float('inf')
            
            for element in starbucks_result.nodes + starbucks_result.ways + starbucks_result.relations:
                starbucks_coords = (element.lat, element.lon) if hasattr(element, 'lat') else (element.center_lat, element.center_lon)
                property_coords = (latitude, longitude)
                distance = geodesic(property_coords, starbucks_coords).km
                
                if distance < min_distance:
                    min_distance = distance
                    nearest_starbucks = {
                        'Starbucks_Latitude': starbucks_coords[0],
                        'Starbucks_Longitude': starbucks_coords[1]
                    }
            
            if nearest_starbucks:
                estimated_time = (min_distance / 40) * 60
                starbucks_info = {
                    'Address': row['Address'],
                    'Starbucks Nearby': 'Sí',
                    'Starbucks_Latitude': nearest_starbucks['Starbucks_Latitude'],
                    'Starbucks_Longitude': nearest_starbucks['Starbucks_Longitude'],
                    'Distance (km)': round(min_distance, 2),
                    'Estimated Time (min)': round(estimated_time, 2)
                }
            else:
                starbucks_info = {
                    'Address': row['Address'],
                    'Starbucks Nearby': 'No',
                    'Starbucks_Latitude': None,
                    'Starbucks_Longitude': None,
                    'Distance (km)': None,
                    'Estimated Time (min)': None
                }
            
            starbucks_data.append(starbucks_info)

            schools_result = api.query(schools_query)
            
            nearby_schools = []
            for element in schools_result.nodes + schools_result.ways + schools_result.relations:
                school_name = element.tags.get('name', 'Unnamed School')
                nearby_schools.append(school_name)
            
            schools_info = {
                'Address': row['Address'],
                'Nearby_Schools': nearby_schools
            }
            schools_data.append(schools_info)

        except overpy.exception.OverpassTooManyRequests as e:
            print(f"Rate limit exceeded. Waiting 10 seconds before retrying...")
            time.sleep(10)
            continue
        except Exception as e:
            print(f"An error occurred for {row['Address']}: {e}")
        
        time.sleep(2)

    starbucks_df = pd.DataFrame(starbucks_data)
    schools_df = pd.DataFrame(schools_data)

    merged_df = df.merge(starbucks_df, on='Address', how='left')
    merged_df = merged_df.merge(schools_df, on='Address', how='left')

    output_file = 'properties_with_starbucks_and_schools.csv'
    merged_df.to_csv(output_file, index=False)
    print(f"Results saved to '{output_file}'")
    return output_file

def send_email(file_path):
    sender_email = "sjajajdaja2102@gmail.com"
    password = "jqox xrmz krba wlod"
    smtp_server = "smtp.gmail.com"
    smtp_port = 587  

    receiver_email = "marypatterson@duck.com"
    subject = "Results"
    body = "Here is the attached file with the results of the property analysis."

    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject

    message.attach(MIMEText(body, "plain"))

    try:
        with open(file_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename={os.path.basename(file_path)}",
            )
            message.attach(part)
    except FileNotFoundError:
        print(f"El archivo {file_path} no se encontró. No se enviará adjunto.")

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, message.as_string())
        print("Correo enviado exitosamente")
    except Exception as e:
        print(f"Error al enviar el correo: {e}")

def main():
    # Paso 1: Descargar datos de banda ancha
    broadband_file = download_broadband_data()
    if not broadband_file:
        print("No se pudo obtener el archivo de datos de banda ancha. Terminando el programa.")
        return

    # Paso 2: Extraer datos de propiedades
    properties_file = extract_property_data()

    # Paso 3: Geocodificar direcciones
    geocoded_file = geocode_addresses(properties_file)

    # Paso 4: Encontrar proveedores de internet cercanos
    properties_with_providers = find_internet_providers(geocoded_file, broadband_file)

    # Paso 5: Encontrar amenidades cercanas (Starbucks y escuelas)
    final_file = find_nearby_amenities(properties_with_providers)

    # Paso 6: Enviar el archivo por correo electrónico
    send_email(final_file)

    print("Proceso completo finalizado.")

if __name__ == "__main__":
    main()
