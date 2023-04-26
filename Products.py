import json
import requests

with open("AppList.json", 'r') as f:
    data = json.load(f)

for game in data:
    print(game["appid"], game["name"])



