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

### See the help 

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

### Finally download some data
To get timeseries identified by the timeseries-key `1000540` in dataset named `GERW2020` and dump the results in the file named `output1.csv`

```bash
python3 knoema_cli.py simple --csv-file output1.csv --timeseries-key 1000540 --dataset GERW2020
```


To get timeseries identified by the timeseries-key `1010920` in dataset named `IS_DCSC_COMMDET_1` and dump the results in the file named `output2.csv`

```bash
python3 knoema_cli.py simple --csv-file output2.csv --timeseries-key 1010920 --dataset IS_DCSC_COMMDET_1
```
