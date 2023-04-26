import json
from CONSTANTS import TAGS


def get_tags(product_info):
    return product_info["store_tags"]


with open("Stellaris.json", 'r') as f:
    res = json.load(f)

tags = get_tags(res)
for tag in tags.values():
    if tag in TAGS:
        print(TAGS[tag])

