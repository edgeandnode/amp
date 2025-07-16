import requests

if __name__ == "__main__":
    url = "http://localhost:1603"
    headers = {
        'Accept-Encoding': 'gzip',
    }

    # Streaming query
    with requests.post(url, data="SELECT block_num FROM mainnet.blocks SETTINGS stream=true", headers=headers, stream=True) as response:
    
    # Non-streaming query
    # with requests.post(url, data="SELECT block_num FROM mainnet.blocks order by block_num", headers=headers, stream=True) as response:

        print(response.headers)
        for line in response.iter_lines():
            print(line)
