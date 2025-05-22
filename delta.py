import requests,pandas as pd,hashlib,hmac,json,time

def product_api():
    response = requests.get('https://cdn.india.deltaex.org/v2/products').json()
    data = response.get("result", [])
    filtered = [item for item in data if item.get("contract_type") == "perpetual_futures"]
    if filtered:
        return pd.DataFrame(filtered)

base_url='https://api.india.delta.exchange'
api_key = 'oDqXukCLyXAA8I5zgsA6VU4Pltrjpc'
api_secret = 'yKW4lU5lMBencLblpDeLJnW1uaLU0wbWM0Zf6TtZKrJwdWMnOfN3xqajt33f'

def generate_signature(secret, message):
    message = bytes(message, 'utf-8')
    secret = bytes(secret, 'utf-8')
    hash = hmac.new(secret, message, hashlib.sha256)
    return hash.hexdigest()

def open_orders(api_key, api_secret):
    method = 'GET'
    timestamp = str(int(time.time()))
    path = '/v2/orders'
    url = f'{base_url}{path}'
    query_string = '?product_id=1&state=open'
    payload = ''
    signature_data = method + timestamp + path + query_string + payload
    signature = generate_signature(api_secret, signature_data)

    req_headers = {
        'api-key': api_key,
        'timestamp': timestamp,
        'signature': signature,
        'User-Agent': 'python-rest-client',
        'Content-Type': 'application/json'
    }

    query = {"product_id": 1, "state": 'open'}

    response = requests.request(
        method, url, data=payload, params=query, timeout=(3, 27), headers=req_headers
    )
    print(response)

def place_order(api_key, api_secret):
    method = 'POST'
    timestamp = str(int(time.time()))
    path = '/v2/orders'
    url = f'{base_url}{path}'
    query_string = ''
    payload = "{\"order_type\":\"limit_order\",\"size\":3,\"side\":\"buy\",\"limit_price\":\"0.0005\",\"product_id\":27}"
    signature_data = method + timestamp + path + query_string + payload
    signature = generate_signature(api_secret, signature_data)

    req_headers = {
        'api-key': api_key,
        'timestamp': timestamp,
        'signature': signature,
        'User-Agent': 'rest-client',
        'Content-Type': 'application/json'
    }

    response = requests.request(
        method, url, data=payload, params={}, timeout=(3, 27), headers=req_headers
    )

    print("Status Code:", response.status_code)

def historical_data_api(timeframe, symbol, starttime, endtime):
    params = {
        'resolution': timeframe,
        'symbol': symbol,
        'start': starttime,
        'end': endtime
    }
    response = (requests.get("https://cdn.india.deltaex.org/v2/history/candles", params=params)).json()
    df = pd.DataFrame(response.get("result", []))
    if not df.empty and 'time' in df.columns:df["time"] = df["time"].apply(lambda ts: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts)))
    return df
def run(period):
    df=product_api()
    timeframe="1m"
    endtime=int(time.time())
    starttime=endtime-period
    data_per_symbol={}
    for index, row in df.iterrows():
        symbol = row["symbol"]
        df_hist=historical_data_api(timeframe,symbol,starttime,endtime)
        data_per_symbol[symbol]=df_hist
    return data_per_symbol