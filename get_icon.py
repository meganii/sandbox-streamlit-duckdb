import requests
import json

USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'

header = { 'User-Agent': USER_AGENT }
url = 'https://scrapbox.io/api/projects/villagepump/'
res = requests.get(url, headers=header).json()

print(res)

print(res['users'])

