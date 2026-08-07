from src.sources.eastmoney.mirrors import request_mirror_urls


def test_numbered_push2_host_normalized_to_main_domain():
    urls = request_mirror_urls("https://82.push2.eastmoney.com/api/qt/stock/trends2/get")
    assert urls == ["https://push2.eastmoney.com/api/qt/stock/trends2/get"]


def test_numbered_push2his_host_keeps_own_cluster():
    urls = request_mirror_urls("https://17.push2his.eastmoney.com/api/qt/stock/kline/get?lmt=1")
    assert urls == ["https://push2his.eastmoney.com/api/qt/stock/kline/get?lmt=1"]


def test_unnumbered_eastmoney_url_returned_as_single_element():
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    assert request_mirror_urls(url) == [url]


def test_non_eastmoney_url_untouched():
    url = "https://qt.gtimg.cn/q=sz000001"
    assert request_mirror_urls(url) == [url]
