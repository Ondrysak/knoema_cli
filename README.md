# knoema_cli

Tool wrapping knoema API using a CLI.


## Quickstart

### Install dependencies
```bash
python3 -m pip install -r requirements.txt
```

### Run the tests

```bash
python3 -m pytest
```
### Simple API


#### See the help 

Use the following command `python3 knoema_cli.py simple --help` to see the help below.

```
Usage: knoema_cli.py simple [OPTIONS]

Options:
  --csv-file TEXT                 Name of the output csv file  [required]
  --timeseries-key INTEGER        Key indentifying the timeseries  [required]
  --guess-country / --no-guess-country
                                  Use --no-guess-country if you do not want to
                                  try to guess the missing area data from the
                                  dataset details.
  --dataset TEXT                  Name of the dataset  [required]
  --help                          Show this message and exit.
```

#### Download some data
To get timeseries identified by the timeseries-key `1000540` in dataset named `GERW2020` and dump the results in the file named `output1.csv`

```bash
python3 knoema_cli.py simple --csv-file output1.csv --timeseries-key 1000540 --dataset GERW2020
```


To get timeseries identified by the timeseries-key `1010920` in dataset named `IS_DCSC_COMMDET_1` and dump the results in the file named `output2.csv`

```bash
python3 knoema_cli.py simple --csv-file output2.csv --timeseries-key 1010920 --dataset IS_DCSC_COMMDET_1
```

To get timeseries identified by the timeseries-key `1004570` in dataset named `OEAWPI2017` and dump the results in the file named `output2.csv`

```bash
python main.py simple --csv-file india.csv --timeseries-key 1004570 --dataset OEAWPI2017
```
### Raw API 

#### See help
Use the following command `python3 knoema_cli.py raw --help` to see the help below.


```
Usage: knoema_cli.py raw [OPTIONS]

  Subcommand for using the raw data API.

Options:
  --dataset TEXT                  Name of the dataset
  --guess-country / --no-guess-country
                                  Use --no-guess-country if you do not want to
                                  try to guess the missing area data from the
                                  dataset details.
  --csv-file TEXT                 Name of the output csv file  [required]
  --frequency [A|M]               A= Annual; M=Monthly;  [required]
  -f, --filter TEXT               <dimension_id>;<dimension_name>;<member>
  --help                          Show this message and exit.

```

#### Download some data
To get data from the dataset sts_inpr_m-20180413 corresponding to some dimension filtering.

```
python3 knoema_cli.py raw --csv-file raw.csv --dataset sts_inpr_m-20180413 --frequency M --filter "geo;geo;1016100" --filter "nace-r2;nace_r2;1003900" --filter "s-adj;s_adj;1000000" --filter "measure;Measure;1001980"
```

It is a good idea to verify that your filter corresponds to a single timeseries by using this API

https://knoema.com/dev/docs/data/timeseries