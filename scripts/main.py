import json, asyncio, aiohttp, time, os
from web3 import Web3
from google.cloud import storage

def run(event, context):
    data = {'assets': []}
    properties = []

    async def gather_with_concurrency(n, *tasks):
        semaphore = asyncio.Semaphore(n)

        async def sem_task(task):
            async with semaphore:
                return await task

        return await asyncio.gather(*(sem_task(task) for task in tasks))

    async def get_async(url, session, results):
        print(f"Making request with url: {url}")
        async with session.get(url) as response:
            response_json = await response.json()
            
            data['assets'].extend(response_json['assets'])

    async def main():
        storage_client = storage.Client()
        bucket = storage_client.bucket('propertys-opensea')
        blob = bucket.blob('properties.json')

        API_KEY = os.environ.get('OPEN_SEA_API_KEY', 'Specified environment variable is not set.')

        conn = aiohttp.TCPConnector(limit=None, ttl_dns_cache=300, ssl=False)
        session = aiohttp.ClientSession(connector=conn, headers={
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
            "X-API-KEY": API_KEY
        })
        urls = [f"https://api.opensea.io/api/v1/assets?collection=propertysofficial&order_by=pk&order_direction=asc&offset={i * 50}&limit=50" for i in range(120)]

        results = {}

        conc_req = 1
        now = time.time()
        await gather_with_concurrency(conc_req, *[get_async(i, session, results) for i in urls])

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

            if asset['sell_orders'] is not None:
                property['salePrice'] = str(Web3.fromWei(int(asset['sell_orders'][0]['base_price']), 'ether'))
                property['paymentToken'] = asset['sell_orders'][0]['payment_token']

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
        await session.close()

    # Uncomment this line when running on Windows
    #asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())