import requests 



url = 'https://api.openweathermap.org/geo/1.0/direct'
params = {'q':'Baku','appid':'82a1ddbac7c82bbf9db7c4afa01c2ab6'}


resp = requests.get(url, params=params)

data = resp.json()[0]


print(f'lat: {data['lat']}')
print(f'lon: {data['lon']}')