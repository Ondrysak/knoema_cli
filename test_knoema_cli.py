from knoema_cli import extract_country_name, create_simple_request, create_raw_request, add_filters

# TODO better tests please


def test_extract_country_name():
    assert extract_country_name('Super Germany Statistics') == 'Germany'

def test_create_simple_request():
    assert create_simple_request('superdataset',123) == [{
        "TimeseriesKey": 123,
        "DatasetId": 'superdataset'}]

def test_create_raw_request():
    assert create_raw_request('datasetus', ['M'], []) == {'Filter': [], 'Frequencies': ['M'], 'Dataset': 'datasetus'}

def test_add_filters():
        assert add_filters('somedimension',[0xdeadbeef],'someDimensionName') == [{"DimensionId": 'somedimension', 'Members': [0xdeadbeef], 'DimensionName': 'someDimensionName'}]