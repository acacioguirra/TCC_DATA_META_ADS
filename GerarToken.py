import requests

# Substitua com as credenciais do painel do seu app 'tcc'
app_id = '1439374281276548'
app_secret = 'b98cdc0fa963cfba9ee3c8b6ac7b5fb7'

# Cole o token copiado do Explorador da Graph API
short_lived_token = 'EAAUdGnC7ZCIQBRmognEcM8L5aSyegfPBKAjpa8zoL5W9tYSQI1wDRgFLFj5B9B3r0wpwPD8FSZCoiPsnZCW0gGHln5td3iswPnCZCEfM8b2SbYgiazpMnR0woeZATtBsZCDfbTaZCIX4Xoo0ZBT1W6ZBlJIBJo9JxKcqeOZC1qYh8o9xuZClR4BGDTRlsSvE84DUE7jni19LfZAQf3K3szMX5VIMBAotsV1nLRtCKiomN0yKntCbDeUwZBUcYychO6wFkFHOOKxKjF4dN5ZBM0MWZAf'

url = 'https://graph.facebook.com/v20.0/oauth/access_token'

params = {
    'grant_type': 'fb_exchange_token',
    'client_id': app_id,
    'client_secret': app_secret,
    'fb_exchange_token': short_lived_token
}

response = requests.get(url, params=params)
dados = response.json()

print("Seu token de 60 dias é:")
print(dados.get('access_token'))