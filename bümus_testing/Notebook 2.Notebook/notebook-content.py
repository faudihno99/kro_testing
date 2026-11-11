# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import requests
import json
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# TVSH SCHLESWIG-HOLSTEIN API
def test_tvsh_opendata():
    """TVSH Open Data API Test - Einzeltest"""
    base_url = "https://opendata.schleswig-holstein.de/api/3/action"
    
    print("Test 1: Package List")
    try:
        response = requests.get(f"{base_url}/package_list", timeout=30)
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
# META   "language_group": "synapse_pyspark"
# META }
