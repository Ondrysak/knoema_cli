import requests
from requests.exceptions import HTTPError
import pandas as pd
import pycountry
import click
import logging
from typing import List, Dict, Any, Union

API_BASE = 'http://knoema.com/api/1.0'
SIMPLE_API_URL = f'{API_BASE}/data/get'

logging.basicConfig(level=logging.DEBUG)


def regions_request(dataset_name: str) -> Union[None, bool, int, float, str, List[Any], Dict[str, Any]]:
    '''
    Get region metadata about a dataset.

    https://knoema.com/rzspxeb/regions-1-dataset

    :param dataset_name: name indentifying the dataset
    :return: Deserialized json object
    '''
    url = f"{API_BASE}/meta/dataset/{dataset_name}/regions"
    res = get_request(url)
    if type(res) is str:
        logging.warning(f"Region info for dataset {dataset_name} missing.")
        res = None
    return res


def ts_metadata(dataset_name: str) -> Union[None, bool, int, float, str, List[Any], Dict[str, Any]]:
    '''
    Get metadata about a dataset.

    https://knoema.com/dev/docs/meta/datasetdetails

    :param dataset_name: name indentifying the dataset
    :return: Deserialized json object
    '''
    url = f"{API_BASE}/meta/dataset/{dataset_name}"
    res = get_request(url)
    return res


def extract_country_name(name: str) -> Union[str,None]:
    '''
    Extract country name from the name of the timeseries.
    If there is multiple the first one is returned.

    :param name: Name of the timeseries.
    :return: Name of the country extracted from the name.
    '''
    logging.info(f'Trying to extract country name from "{name}"')
    names = [c.name for c in list(pycountry.countries)]
    for c_name in names:
        if c_name in name:
            return c_name


def get_request(url: str) -> Union[None, bool, int, float, str, List[Any], Dict[str, Any]]:
    '''
    Make a GET request and try to deserialize the response to as JSON.

    :param url: target url
    :return: Deserialized JSON object.
    '''
    logging.info(f'Get request to url: {url}')
    try:
        response = requests.get(url)

        # If the response was successful, no Exception will be raised
        response.raise_for_status()
        jsonResponse = response.json()
        return jsonResponse
    except HTTPError as http_err:
        logging.error(f'HTTP error occurred: {http_err}')  # Python 3.6
    except Exception as err:
        logging.error(f'Other error occurred: {err}')  # Python 3.6


def post_request(payload: str, url: str) -> Union[None, bool, int, float, str, List[Any], Dict[str, Any]]:
    '''
    Make a POST request to target URL with the specified payload.

    :param payload: POST request data
    :param url: target URL
    :return: Deserialized JSON object.
    '''
    logging.info(f'Post request to url: {url}')
    logging.debug(f'Post request payload: {payload}')

    try:
        response = requests.post(url, json=payload)

        # If the response was successful, no Exception will be raised
        response.raise_for_status()
        # print(response.text)
        jsonResponse = response.json()
        return jsonResponse
    except HTTPError as http_err:
        logging.debug(f'HTTP error occurred: {http_err}')  # Python 3.6
    except Exception as err:
        logging.debug(f'Other error occurred: {err}')  # Python 3.6


def create_simple_request(dataset_id: str, timeseries_key: int) -> str:
    '''
    Function creating a payload for using the simple API.

    https://knoema.com/dev/docs/data/observation

    :param dataset_id:
    :param timeseries_key:
    :return:
    '''
    logging.debug(f'Creating a simple request payload dataset {dataset_id} and tskey {timeseries_key}')
    payload = [{
        "TimeseriesKey": timeseries_key,
        "DatasetId": dataset_id
    }]
    logging.debug(f'Created payload {payload}')
    return payload


def transform_to_df(input_ts: Dict, input_metadata: Dict , input_region: Dict, extracted_country: str =None) -> pd.DataFrame:
    '''
    Function that transforms the data received from the simple API, dataset details API, dataset region API and the country guess into a pandas dataframe.

    :param input_ts: The simple API response.
    :param input_metadata: The dataset details API response.
    :param input_region:  The dataset region API response.
    :param extracted_country: The country name extracted from the name of the timeseries.
    :return: A pandas dataframe representing a timeseries.
    '''
    logging.info('Transformation to a pandas dataframe starting.')
    area = 'Unknown'
    if extracted_country:
        logging.debug(f'Using the info about area extracted from name.')
        area = extracted_country

    elif input_region:
        logging.info('Using the geoDimensionId for area info.')
        geo_dimension_id = input_region['geoDimensionId']
        logging.debug(f'Geodimension id is {geo_dimension_id}')
        geo_dimension = [d['name'] for d in input_ts['metadata'] if d['dim'] == geo_dimension_id]
        if len(geo_dimension) == 0:
            area = 'Unknown'
            logging.warning(f'Geodimension not found setting area to {area}')
        elif len(geo_dimension) == 1:
            area = geo_dimension[0]
            logging.info(f'Geodimension found setting area to {area}')

    logging.debug('Converting to pandas dataframe')
    df = pd.DataFrame(input_ts['data'], columns=['value'])
    # TODO other frequencies exist
    logging.debug(f'Creating period range index {input_ts["frequency"]}')
    if input_ts['frequency'] == 'monthly':
        df.index = pd.period_range(input_ts['startDate'], input_ts['endDate'], freq='M')
        df['startDate'] = df.index.to_timestamp() - pd.offsets.MonthBegin(0)
        df['endDate'] = df.index.to_timestamp() + pd.offsets.MonthEnd(1)
    elif input_ts['frequency'] == 'annual':
        df.index = pd.period_range(input_ts['startDate'], input_ts['endDate'], freq='A')
        df['startDate'] = df.index.to_timestamp() - pd.offsets.YearBegin(0)
        df['endDate'] = df.index.to_timestamp() + pd.offsets.YearEnd(1)
    else:
        raise Exception("Sorry, only annual and monthly frequencies are supported at the moment.")

    if input_metadata['name']:
        logging.debug(f'Setting timeseries name to name from metadata {input_metadata["name"]}')
        df['seriesName'] = input_metadata['name']
    else:
        logging.debug(f'Setting timeseries name to name of dataset {input_ts["datasetName"]}')
        df['seriesName'] = input_ts['datasetName']
    df['area'] = area
    return df


def dataset_region(dataset_name: str) -> Dict:
    try:
        region_response = regions_request(dataset_name)[0]
    except TypeError:
        region_response = None
    return region_response


@click.group()
def knoema_cli() -> None:
    """
    Knoema API wrapper CLI.
    """
    pass


@click.option(
    "--dataset",
    required=True, type=str,
    help="Name of the dataset",
)
@click.option(
    "--guess-country/--no-guess-country",
    default=True,
    help="Use --no-guess-country if you do not want to try to guess the missing area data from the dataset details.",
)
@click.option(
    "--timeseries-key",
    required=True, type=int,
    help="Key indentifying the timeseries",
)
@click.option(
    "--csv-file",
    required=True, type=str,
    help="Name of the output csv file",
)
@knoema_cli.command()
def simple(dataset: str, guess_country: bool, timeseries_key: int, csv_file: str) -> None:
    metadata_response = ts_metadata(dataset)
    region_response = dataset_region(dataset)
    if guess_country:
        extract_country_result = extract_country_name(metadata_response['name'])
    else:
        extract_country_result = None
    data_response = post_request(create_simple_request(dataset, timeseries_key), SIMPLE_API_URL)[0]
    output_dataframe = transform_to_df(data_response, metadata_response, region_response, extract_country_result)
    logging.info(f'Succesfully created the pandas dataframe')
    output_dataframe.to_csv(csv_file, index=False)
    logging.info(f'Dumped the dataframe to a csv file: {csv_file}')


if __name__ == "__main__":
    knoema_cli()
