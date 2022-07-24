import json, requests, time, os
from web3 import Web3
from google.cloud import storage
from requests.adapters import HTTPAdapter, Retry

def run(event, context):
    data = {'assets': []}
    properties = []

    def main():
        OS_BASE_URL = 'https://api.opensea.io/api/v1/assets'
        API_KEY = os.environ.get('OPEN_SEA_API_KEY', 'Specified environment variable is not set.')

        params = {
            'collection': 'propertysofficial',
            'include_orders': 'true',
            'limit': '50'
        }

        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
            'X-API-KEY': API_KEY
        }

        storage_client = storage.Client()
        bucket = storage_client.bucket('propertys-opensea')
        blob = bucket.blob('properties.json')

        with requests.Session() as session:
            retry = Retry(
                total=5,
                status_forcelist=[495, 500, 502, 503, 504],
                backoff_factor=0.1
            )

            session.mount(OS_BASE_URL, HTTPAdapter(max_retries=retry))

            while True:
                print(f"Making request with params: {params}")
                (r := session.get(OS_BASE_URL, params=params, headers=headers)).raise_for_status()
                print(r.status_code)
                response_json = r.json()
                data['assets'].extend(response_json['assets'])

                if (cursor := response_json['next']):
                    print(f"Next cursor: {cursor}")
                    params['cursor'] = cursor
                else:
                    break

        now = time.time()

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

            if asset['seaport_sell_orders'] is not None:
                property['salePrice'] = str(Web3.fromWei(int(asset['seaport_sell_orders'][0]['base_price']), 'ether'))
                property['paymentToken'] = asset['seaport_sell_orders'][0]['payment_token']

            for trait in asset['traits']:
                if trait['trait_type'] == 'City Name':
                    property['city'] = trait['value'].strip()
                if trait['trait_type'] == 'District Name':
                    property['district'] = trait['value'].strip()
                if trait['trait_type'] == 'Street Name':
                    property['street'] = trait['value'].strip()
                if trait['trait_type'] == 'Unit':
                    property['unit'] = trait['value']
                if trait['trait_type'] == 'Special':
                    property['city'] = 'Special'
                    property['district'] = 'Special'
                    property['street'] = trait['value'].strip()
            
            properties.append(property)
        
        time_taken = time.time() - now

        blob.upload_from_string(json.dumps(properties))

        print(time_taken)

    main()