import requests
from requests.exceptions import HTTPError
import pandas as pd  # type: ignore
import pycountry  # type: ignore
import click
import logging
from itertools import zip_longest
from typing import List, Dict, Any, Union

API_BASE = "http://knoema.com/api/1.0"
SIMPLE_API_URL = f"{API_BASE}/data/get"
RAW_API_URL = f"{API_BASE}/data/details"

logging.basicConfig(level=logging.DEBUG)


def grouper(iterable, n: int, fillvalue=None):
    """
    Collect data into fixed-length chunks or blocks
    grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"

    :param iterable: iterable we want to group
    :param n:  size of chunks
    :param fillvalue: fill value in case the iterable length is not divisible by chunk size
    :return: grouped values
    """
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


def create_raw_request(dataset_id: str, frequencies: List[str], dim_filter: List[Dict]):
    """
    Create raw request payload.

    :param dataset_id: name identifying the dataset
    :param frequencies: list of included frequencies
    :param dim_filter: dimension filter
    :return: deserialized JSON object
    """

    logging.debug("Creating new raw request payload")
    payload = {"Filter": dim_filter, "Frequencies": frequencies, "Dataset": dataset_id}
    logging.debug(f"Created payload {payload}")
    return payload


def add_filters(
    dimension_id: str, members: List, dimension_name: str, dim_filter: List = []
):
    """
    Function to create or modify a list of filters.
    :param dimension_id: Id of the dimension we want to filter on
    :param members: List of members we want to include
    :param dimension_name: Name of the dimension we want to filter on.
    :param dim_filter: List of previous filters we want to modify if left empty new one is created.
    :return: Updated list of filters
    """
    logging.debug(f"Filter added")
    dim_filter.append(
        {
            "DimensionId": dimension_id,
            "Members": members,
            "DimensionName": dimension_name,
        }
    )
    return dim_filter


def transform_to_df_raw(
    input_ts, input_metadata, input_region, response_extract_country, frequency
):
    """
    Transform raw data API response with other metadata into a pandas dataframe.
    :param input_ts: raw data API response
    :param input_metadata: metadata API response
    :param input_region: region API response
    :param response_extract_country: country extraction
    :param frequency:
    :return:
    """
    logging.debug("Creating pandas dataframe from raw data api response.")
    columns = []

    for c in input_ts["columns"]:
        if c["name"] and c["name"] not in columns:
            columns.append(c["name"])
        elif c["dimensionId"] and c["dimensionId"] not in columns:
            columns.append(c["dimensionId"])
    logging.debug(f"Column names {columns}")
    if input_region:
        geo_dimension_id = input_region["geoDimensionId"]
        logging.info(f"GeodimensionId found: {geo_dimension_id}")
        columns = ["area" if c == geo_dimension_id else c for c in columns]
    # the datapoints form one huge list, so we have to group them into individual datapoints
    grouped_list = list(grouper(input_ts["data"], len(columns)))
    df = pd.DataFrame(grouped_list, columns=columns)
    df["Date"] = pd.json_normalize(data=df["Date"])
    # TODO other frequencies exist
    if frequency == "M":
        df["startDate"] = pd.to_datetime(df["Date"]) - pd.offsets.MonthBegin(0)
        df["endDate"] = pd.to_datetime(df["Date"]) + pd.offsets.MonthEnd(1)
    if frequency == "A":
        df["startDate"] = pd.to_datetime(df["Date"]) - pd.offsets.YearBegin(0)
        df["endDate"] = pd.to_datetime(df["Date"]) + pd.offsets.YearEnd(1)
    # TODO this is not the actual name

    if input_metadata["name"]:
        logging.debug(
            f'Setting timeseries name to name from metadata {input_metadata["name"]}'
        )
        df["seriesName"] = input_metadata["name"]
    else:
        logging.debug(
            f'Setting timeseries name to name of dataset {input_ts["datasetName"]}'
        )
        df["seriesName"] = input_ts["datasetName"]
    if not input_region and response_extract_country:
        df["area"] = response_extract_country
    elif not input_region:
        df["area"] = "Unknown"

    df = df.drop(["Date"], axis=1)
    return df


def regions_request(
    dataset_name: str,
) -> List[Any]:
    """
    Get region metadata about a dataset.

    https://knoema.com/rzspxeb/regions-1-dataset

    :param dataset_name: name indentifying the dataset
    :return: Deserialized json object
    """
    url = f"{API_BASE}/meta/dataset/{dataset_name}/regions"
    res = get_request(url)
    if type(res) is str:
        logging.warning(f"Region info for dataset {dataset_name} missing.")
        res = None
    return res


def ts_metadata(
    dataset_name: str,
) -> Dict[Any, Any]:
    """
    Get metadata about a dataset.

    https://knoema.com/dev/docs/meta/datasetdetails

    :param dataset_name: name indentifying the dataset
    :return: Deserialized json object
    """
    url = f"{API_BASE}/meta/dataset/{dataset_name}"
    res = get_request(url)
    return res


def extract_country_name(name: str) -> Union[str, None]:
    """
    Extract country name from the name of the timeseries.
    If there is multiple the first one is returned.

    :param name: Name of the timeseries.
    :return: Name of the country extracted from the name.
    """
    logging.info(f'Trying to extract country name from "{name}"')
    names = [c.name for c in list(pycountry.countries)]
    for c_name in names:
        if c_name in name:
            return c_name
    return None


def get_request(
    url: str,
):
    """
    Make a GET request and try to deserialize the response to as JSON.

    :param url: target url
    :return: Deserialized JSON object.
    """
    logging.info(f"Get request to url: {url}")
    try:
        response = requests.get(url)

        # If the response was successful, no Exception will be raised
        response.raise_for_status()
        jsonResponse = response.json()
        return jsonResponse
    except HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")  # Python 3.6
    except Exception as err:
        logging.error(f"Other error occurred: {err}")  # Python 3.6
    return None


def post_request(payload, url: str):
    """
    Make a POST request to target URL with the specified payload.

    :param payload: POST request data
    :param url: target URL
    :return: Deserialized JSON object.
    """
    logging.info(f"Post request to url: {url}")
    logging.debug(f"Post request payload: {payload}")

    try:
        response = requests.post(url, json=payload)

        # If the response was successful, no Exception will be raised
        response.raise_for_status()
        jsonResponse = response.json()
        return jsonResponse
    except HTTPError as http_err:
        logging.debug(f"HTTP error occurred: {http_err}")  # Python 3.6
    except Exception as err:
        logging.debug(f"Other error occurred: {err}")  # Python 3.6
    return None


def create_simple_request(dataset_id: str, timeseries_key: int) -> List[Any]:
    """
    Function creating a payload for using the simple API.

    https://knoema.com/dev/docs/data/observation

    :param dataset_id:
    :param timeseries_key:
    :return:
    """
    logging.debug(
        f"Creating a simple request payload dataset {dataset_id} and tskey {timeseries_key}"
    )
    payload = [{"TimeseriesKey": timeseries_key, "DatasetId": dataset_id}]
    logging.debug(f"Created payload {payload}")
    return payload


def transform_to_df(
    input_ts: Dict,
    input_metadata: Dict,
    input_region: Dict,
    extracted_country: str = None,
) -> pd.DataFrame:
    """
    Function that transforms the data received from the simple API, dataset details API, dataset region API and the country guess into a pandas dataframe.

    :param input_ts: The simple API response.
    :param input_metadata: The dataset details API response.
    :param input_region:  The dataset region API response.
    :param extracted_country: The country name extracted from the name of the timeseries.
    :return: A pandas dataframe representing a timeseries.
    """
    logging.info("Transformation to a pandas dataframe starting.")
    area = "Unknown"
    if extracted_country:
        logging.debug("Using the info about area extracted from name.")
        area = extracted_country

    elif input_region:
        logging.info("Using the geoDimensionId for area info.")
        geo_dimension_id = input_region["geoDimensionId"]
        logging.debug(f"Geodimension id is {geo_dimension_id}")
        geo_dimension = [
            d["name"] for d in input_ts["metadata"] if d["dim"] == geo_dimension_id
        ]
        if len(geo_dimension) == 0:
            area = "Unknown"
            logging.warning(f"Geodimension not found setting area to {area}")
        elif len(geo_dimension) == 1:
            area = geo_dimension[0]
            logging.info(f"Geodimension found setting area to {area}")

    logging.debug("Converting to pandas dataframe")
    df = pd.DataFrame(input_ts["data"], columns=["value"])
    # TODO other frequencies exist
    logging.debug(f'Creating period range index {input_ts["frequency"]}')
    if input_ts["frequency"] == "monthly":
        df.index = pd.period_range(input_ts["startDate"], input_ts["endDate"], freq="M")
        df["startDate"] = df.index.to_timestamp() - pd.offsets.MonthBegin(0)
        df["endDate"] = df.index.to_timestamp() + pd.offsets.MonthEnd(1)
    elif input_ts["frequency"] == "annual":
        df.index = pd.period_range(input_ts["startDate"], input_ts["endDate"], freq="A")
        df["startDate"] = df.index.to_timestamp() - pd.offsets.YearBegin(0)
        df["endDate"] = df.index.to_timestamp() + pd.offsets.YearEnd(1)
    else:
        raise Exception(
            "Sorry, only annual and monthly frequencies are supported at the moment."
        )

    if input_metadata["name"]:
        logging.debug(
            f'Setting timeseries name to name from metadata {input_metadata["name"]}'
        )
        df["seriesName"] = input_metadata["name"]
    else:
        logging.debug(
            f'Setting timeseries name to name of dataset {input_ts["datasetName"]}'
        )
        df["seriesName"] = input_ts["datasetName"]
    df["area"] = area
    return df


def dataset_region(dataset_name: str) -> Dict:
    """
    Get dataset region metadata

    :param dataset_name: Name identifying the dataset
    :return:
    """
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
    required=True,
    type=str,
    help="Name of the dataset",
)
@click.option(
    "--guess-country/--no-guess-country",
    default=True,
    help="Use --no-guess-country if you do not want to try to guess the missing area data from the dataset details.",
)
@click.option(
    "--timeseries-key",
    required=True,
    type=int,
    help="Key indentifying the timeseries",
)
@click.option(
    "--csv-file",
    required=True,
    type=str,
    help="Name of the output csv file",
)
@knoema_cli.command()
def simple(
    dataset: str, guess_country: bool, timeseries_key: int, csv_file: str
) -> None:
    """
    Subcommand for using the simple data API.
    """
    metadata_response = ts_metadata(dataset)
    region_response = dataset_region(dataset)
    if guess_country:
        extract_country_result = extract_country_name(metadata_response["name"])
    else:
        extract_country_result = None
    data_response = post_request(
        create_simple_request(dataset, timeseries_key), SIMPLE_API_URL
    )[0]
    output_dataframe = transform_to_df(
        data_response, metadata_response, region_response, extract_country_result
    )
    logging.info("Succesfully created the pandas dataframe")
    output_dataframe.to_csv(csv_file, index=False)
    logging.info(f"Dumped the dataframe to a csv file: {csv_file}")


@knoema_cli.command()
@click.option(
    "--dataset",
    prompt=True,
    help="Name of the dataset",
)
@click.option(
    "--guess-country/--no-guess-country",
    default=True,
    help="Use --no-guess-country if you do not want to try to guess the missing area data from the dataset details.",
)
@click.option("--csv-file", required=True, help="Name of the output csv file", type=str)
@click.option(
    "--frequency",
    type=click.Choice(["A", "M"]),
    required=True,
    help="A= Annual; M=Monthly;",
)
@click.option(
    "--filter",
    "-f",
    multiple=True,
    type=str,
    help="<dimension_id>;<dimension_name>;<member>",
)
def raw(dataset, guess_country, csv_file, frequency, filter):
    """
    Subcommand for using the raw data API.
    """
    dim_filter = None
    logging.debug("Parsing the filters")
    for f in filter:
        dimension_id, dimension_name, member = f.split(";")
        logging.info(f"{dimension_id}, {dimension_name}, {member}")
        if not dim_filter:
            dim_filter = add_filters(
                dimension_id=dimension_id,
                members=[member],
                dimension_name=dimension_name,
            )
        else:

            dim_filter = add_filters(
                dimension_id=dimension_id,
                members=[member],
                dimension_name=dimension_name,
                dim_filter=dim_filter,
            )
    metadata_response = ts_metadata(dataset)
    region_response = dataset_region(dataset)
    if guess_country:
        extract_country_result = extract_country_name(metadata_response["name"])
    else:
        extract_country_result = None
    raw_data_response = post_request(
        create_raw_request(dataset, frequencies=[frequency], dim_filter=dim_filter),
        RAW_API_URL,
    )

    df = transform_to_df_raw(
        raw_data_response,
        metadata_response,
        region_response,
        extract_country_result,
        frequency,
    )
    logging.info(f"Succesfully created the pandas dataframe")
    df.to_csv(csv_file, index=False)
    logging.info(f"Dumped the dataframe to a csv file: {csv_file}")


if __name__ == "__main__":
    knoema_cli()
