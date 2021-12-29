import requests
import json
from web3 import Web3

offset = 0
data = {'assets': []}

while True:
    params = {
        'collection': 'propertysofficial',
        'order_by': 'pk',
        'order_direction': 'asc',
        'offset': offset,
        'limit': 50
    }

    response = requests.get('https://api.opensea.io/api/v1/assets', params=params)
    response_json = response.json()

    data['assets'].extend(response_json['assets'])

    if len(response_json['assets']) < 50:
        break

    offset += 50

properties = []

for asset in data['assets']:
    property = {
        'tokenId': asset['token_id'],
        'numSales': asset['num_sales'],
        'imageUrl': asset['image_url'],
        'imagePreviewUrl': asset['image_preview_url'],
        'imageThumbnailUrl': asset['image_thumbnail_url'],
        'name': asset['name'],
        'osLink': asset['permalink'],
        'lastSale': asset['last_sale'],
        'ownerAddress': asset['owner']['address']
    }

    if asset['owner']['user'] is not None:
        property['ownerName'] = asset['owner']['user']['username']

    if asset['last_sale'] is not None:
        property['lastSale'] = str(Web3.fromWei(int(asset['last_sale']['total_price']), 'ether'))

    for trait in asset['traits']:
        if trait['trait_type'] == 'City Name':
            property['city'] = trait['value']
        if trait['trait_type'] == 'District Name':
            property['district'] = trait['value']
        if trait['trait_type'] == 'Street Name':
            property['street'] = trait['value']
        if trait['trait_type'] == 'Unit':
            property['unit'] = trait['value']
    
    properties.append(property)

with open('properties.json', 'w') as prop_file:
    prop_file.write(json.dumps(properties))
