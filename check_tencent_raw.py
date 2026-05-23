import requests
import random
from akshare.utils import demjson

symbol = "sz002421"
year = 2026
params = {
    "_var": f"kline_day{year}",
    "param": f"{symbol},day,{year}-01-01,{year}-12-31,640,",
    "r": f"0.{random.randint(1000000000, 9999999999)}",
}
url = "https://web.ifzqgtimg.cn/appstock/app/fqkline/get"
resp = requests.get(url, params=params)
data_text = resp.text
idx = data_text.find("={")
data_json = demjson.decode(data_text[idx + 1:])["data"][symbol]
if "day" in data_json:
    day_data = data_json["day"]
    print("Raw Tencent columns for first 5 days:")
    for row in day_data[:5]:
        print(row)
