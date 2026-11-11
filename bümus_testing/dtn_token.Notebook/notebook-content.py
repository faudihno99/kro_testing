# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

#--- Obtain an API access token ---
import requests

# --- Obtain an API access token ---
auth_url = 'https://api.auth.dtn.com/v1/tokens/authorize'
body = {
    'grant_type': 'client_credentials',
    'client_id': 'z2FD2pHAgfiKqOMAzmtX5My3yrcWg0tW', 
    'client_secret': '4n_fOomMRUKLrCN4ibKN5DAnDFrKuSZ9ojTb07mvMDGxLB5qWfI0dlUMH9oGkG_Y',
    'audience': 'https://weather.api.dtn.com/conditions'
}

response = requests.post(auth_url, json=body)
print(response)
access_token = response.json()['data']['access_token']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


  #--- Invoke the API ---

  api_base_url = 'https://weather.api.dtn.com/v2'
  query_parameters = urllib.parse.urlencode({
      "lat": 35.47,
      "lon": -97.51,
      "startTime": "2023-10-01T00:06:10Z",
      "endTime": "2023-10-14T00:06:10Z",
  })
  endpoint = '/conditions?' + query_parameters

  headers = {'Authorization': 'Bearer ' + access_token}
  response = requests.get(api_base_url + endpoint, headers=headers)

  #--- Print the response ---

  print(response.json())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
