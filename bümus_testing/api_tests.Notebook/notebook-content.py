# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   }
# META }

# CELL ********************


import requests
import json
from datetime import datetime

def print_response(api_name, response_data, status_code):
    """Helper function to print API responses nicely"""
    print(f"\n{'='*50}")
    print(f"API: {api_name}")
    print(f"Status Code: {status_code}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")
    
    if isinstance(response_data, dict):
        print(json.dumps(response_data, indent=2, ensure_ascii=False))
    else:
        print(response_data)
    print(f"{'='*50}\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# DTN WEATHER API
def test_dtn_weather():
    """DTN Weather API Test - Einzeltest"""

    #--- Obtain an API access token ---
    import requests

    # --- Obtain an API access token ---
    auth_url = 'https://api.auth.dtn.com/v1/tokens/authorize'
    body = {
        'grant_type': 'client_credentials',
        'client_id': 'robert.kowitz@buesum.de', 
        'client_secret': 'Buesum2025#',
        'audience': 'https://weather.api.dtn.com'
    }

    response = requests.post(auth_url, json=body)
    print(response)
    #access_token = response.json()['data']['access_token']

    
    test_api_key = "oViXqthDGFNVw3IvAsnKrFG1AacfPyA6b"
    
    # Koordinaten für Hürth, NRW
    lat = 50.8719
    lon = 6.8828
    
    # DTN Content Services API
    url = "https://api.dtn.com/weather/observations/latest"
    
    params = {
        'lat': lat,
        'lon': lon,
        'units': 'si',
        'apikey': test_api_key
    }
    
    headers = {
        'Accept': 'application/json'
    }
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=30)
        
        if response.status_code == 200:
            print_response("DTN Weather API", response.json(), response.status_code)
        else:
            print_response("DTN Weather API", 
                         f"HTTP {response.status_code}: {response.text[:300]}", 
                         response.status_code)
            
    except requests.exceptions.RequestException as e:
        print_response("DTN Weather API", f"Verbindungsfehler: {e}", "ERROR")
    
    print("DTN API Key Beschaffung:")
    print("1. Website besuchen: https://www.dtn.com/resources/api-data-integrations/")
    print("2. Kontakt zu DTN Sales Representative aufnehmen")
    print("3. Trial/Contract vereinbaren")
    print("4. Client Credentials von DTN Customer Service erhalten")
    print("5. OAuth2 Access Token mit Client Credentials anfordern")

test_dtn_weather()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# TVSH SCHLESWIG-HOLSTEIN API
def test_tvsh_opendata():
    """TVSH Open Data API Test - Einzeltest"""
    base_url = "https://opendata.schleswig-holstein.de/api/3/action"
    
    print("Test 1: Package List")
    try:
        response = requests.get(f"{base_url}/help_show?name=package_search", timeout=30)
        data = response.json()
        
        if data.get('success'):
            limited_data = {
                'success': data['success'],
                'result_count': len(data['result']),
                'first_5_datasets': data['result'][:5]
            }
            print_response("TVSH - Package List", limited_data, response.status_code)
        else:
            print_response("TVSH - Package List", data, response.status_code)
            
    except requests.exceptions.RequestException as e:
        print_response("TVSH - Package List", f"Fehler: {e}", "ERROR")
    
    print("\nTest 2: Tourism Search")
    try:
        search_params = {'q': 'tourismus', 'rows': 3}
        response = requests.get(f"{base_url}/package_search", params=search_params, timeout=30)
        print_response("TVSH - Tourism Search", response.json(), response.status_code)
        
    except requests.exceptions.RequestException as e:
        print_response("TVSH - Tourism Search", f"Fehler: {e}", "ERROR")
        
test_tvsh_opendata()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# BSH GEOSEAPORTAL EINZELTESTS
def test_bsh_bathymetry():
    """BSH Bathymetrie WMS Test"""
    wms_url = "https://www.geoseaportal.de/geoserver/ELC_INSPIRE/ows"
    
    params = {
        'SERVICE': 'WMS',
        'VERSION': '1.3.0',
        'REQUEST': 'GetCapabilities'
    }
    
    try:
        response = requests.get(wms_url, params=params, timeout=30)
        
        if response.status_code == 200:
            # XML Antwort analysieren (einfach)
            xml_content = response.text
            
            # Einfache String-Suche nach wichtigen Informationen
            result = {
                'service_available': True,
                'content_length': len(xml_content),
                'contains_bathymetry': 'bathymetry' in xml_content.lower(),
                'contains_layers': '<Layer>' in xml_content,
                'service_title': 'INSPIRE theme Elevation (bathymetry)' if 'bathymetry' in xml_content else 'Unknown'
            }
            
            print_response("BSH Bathymetrie WMS", result, response.status_code)
        else:
            print_response("BSH Bathymetrie WMS", 
                         f"HTTP Error: {response.status_code}", 
                         response.status_code)
            
    except requests.exceptions.RequestException as e:
        print_response("BSH Bathymetrie WMS", f"Fehler: {e}", "ERROR")

def test_bsh_getmap():
    """BSH GetMap Test - Lade tatsächliche Karte"""
    wms_url = "https://www.geoseaportal.de/geoserver/ELC_INSPIRE/ows"
    
    params = {
        'SERVICE': 'WMS',
        'VERSION': '1.3.0',
        'REQUEST': 'GetMap',
        'LAYERS': 'ELC_INSPIRE:bathymetry',
        'CRS': 'EPSG:4326',
        'BBOX': '7.0,53.5,9.0,55.0',  # Bereich um Hamburg/Deutsche Bucht
        'WIDTH': 512,
        'HEIGHT': 512,
        'FORMAT': 'image/png',
        'TRANSPARENT': 'true'
    }
    
    try:
        response = requests.get(wms_url, params=params, timeout=30)
        
        if response.status_code == 200:
            result = {
                'map_retrieved': True,
                'content_type': response.headers.get('content-type', 'unknown'),
                'image_size_bytes': len(response.content),
                'bbox_coordinates': '7.0,53.5,9.0,55.0 (Hamburg/Deutsche Bucht)',
                'layer': 'Bathymetrie (Meerestiefe)',
                'image_format': 'PNG'
            }
            print_response("BSH GetMap - Bathymetrie Karte", result, response.status_code)
            
            # Optional: Bild speichern
            print("Hinweis: Kartenbilddaten wurden erfolgreich abgerufen.")
            print("Zum Speichern: with open('bathymetry_map.png', 'wb') as f: f.write(response.content)")
            
        else:
            print_response("BSH GetMap", 
                         f"HTTP Error: {response.status_code}", 
                         response.status_code)
            
    except requests.exceptions.RequestException as e:
        print_response("BSH GetMap", f"Fehler: {e}", "ERROR")

def test_bsh_portal_access():
    """BSH GeoSeaPortal Zugang testen"""
    portal_url = "https://www.geoseaportal.de"
    
    try:
        response = requests.get(portal_url, timeout=30)
        
        if response.status_code == 200:
            result = {
                'portal_accessible': True,
                'response_size': len(response.content),
                'content_type': response.headers.get('content-type', 'unknown'),
                'portal_url': portal_url,
                'status': 'BSH GeoSeaPortal ist online und erreichbar'
            }
            print_response("BSH GeoSeaPortal Portal", result, response.status_code)
        else:
            print_response("BSH GeoSeaPortal Portal", 
                         f"HTTP Error: {response.status_code}", 
                         response.status_code)
            
    except requests.exceptions.RequestException as e:
        print_response("BSH GeoSeaPortal Portal", f"Fehler: {e}", "ERROR")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Accessing datasets using TVSH API


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Bathymetrie (Meerestiefe)
test_bsh_bathymetry()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Karte als PNG abrufen
test_bsh_getmap()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Portal-Zugang testen
test_bsh_portal_access()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# PEGELONLINE API
def test_pegelonline():
    """Pegelonline API Test - Einzeltest"""
    base_url = "https://www.pegelonline.wsv.de/webservices/rest-api/v2"
    
    print("Test 1: Alle Stationen (Limit 3)")
    try:
        params = {
            'includeTimeseries': 'true',
            'includeCurrentMeasurement': 'true',
            'limit': 3
        }
        response = requests.get(f"{base_url}/stations.json", params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            simplified_stations = []
            for station in data[:3]:
                simplified_stations.append({
                    'uuid': station.get('uuid'),
                    'shortname': station.get('shortname'),
                    'longname': station.get('longname'),
                    'water': station.get('water', {}).get('shortname'),
                    'current_value': station.get('timeseries', [{}])[0].get('currentMeasurement', {}).get('value') if station.get('timeseries') else None
                })
            
            print_response("Pegelonline - Stations", 
                         {'total_found': len(data), 'stations': simplified_stations}, 
                         response.status_code)
        else:
            print_response("Pegelonline - Stations", 
                         f"HTTP Error: {response.status_code}", 
                         response.status_code)
        
    except requests.exceptions.RequestException as e:
        print_response("Pegelonline - Stations", f"Fehler: {e}", "ERROR")
    
    print("\nTest 2: Rhein-Stationen")
    try:
        params = {
            'waters': 'RHEIN',
            'includeCurrentMeasurement': 'true',
            'limit': 2
        }
        response = requests.get(f"{base_url}/stations.json", params=params, timeout=30)
        print_response("Pegelonline - Rhine Stations", response.json(), response.status_code)
        
    except requests.exceptions.RequestException as e:
        print_response("Pegelonline - Rhine Stations", f"Fehler: {e}", "ERROR")
    
    print("\nTest 3: Aktuelle Messung Köln")
    try:
        station_uuid = "593647aa-9fea-43ec-a7d6-6476a76ae868"
        measurement_url = f"{base_url}/stations/{station_uuid}/W/currentmeasurement.json"
        
        response = requests.get(measurement_url, timeout=30)
        print_response("Pegelonline - Current Measurement Köln", response.json(), response.status_code)
        
    except requests.exceptions.RequestException as e:
        print_response("Pegelonline - Current Measurement", f"Fehler: {e}", "ERROR")

test_pegelonline()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
