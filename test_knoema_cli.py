from knoema_cli import extract_country_name, create_simple_request

def test_extract_country_name():
    assert extract_country_name('Super Germany Statistics') == 'Germany'

def test_create_simple_request():
    assert create_simple_request('superdataset',123) == [{
        "TimeseriesKey": 123,
        "DatasetId": 'superdataset'}]