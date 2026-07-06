import requests

# Substitua com as credenciais do painel do seu app 'tcc'
app_id = 'app'
app_secret = 'app_secret'

# Cole o token copiado do Explorador da Graph API
short_lived_token = 'token_gerado_no_META'

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