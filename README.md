[![PyPI version](https://badge.fury.io/py/parquet-py.svg)](https://badge.fury.io/py/parquet-py)

# Parquet-Py

Parquet-Py is a simple command-line interface & Python API designed to facilitate the interaction with Parquet files. It allows users to convert Parquet files into CSV, JSON, lists, and iterators for easy manipulation and access in Python applications.

Using Rust bindings under the hood, Parquet-Py provides a fast and efficient way to work with Parquet files, making it ideal for converting or processing large datasets.

## Features

- **Convert Parquet to CSV**: Convert your Parquet files into CSV format for easy viewing and processing in spreadsheet applications.
- **Convert Parquet to JSON / JSON Lines**: Easily convert your Parquet files into a JSON Array or JSON Lines format for quick inspection or processing.
- **Iterable Parquet Rows**: Access Parquet file rows through an iterator, allowing for efficient row-by-row processing without loading the entire file into memory.
- **Convert Parquet to Python List**: Transform your Parquet files into Python lists, where each row is represented as a dictionary within the list.

## Installation

### PyPI
`pip install parquet-py`

## Usage
### Command-Line Interface

> [!WARNING]
> 
> The CLI is still under development and may not be fully functional.
> 
> Breaking changes may occur in future releases.

> [!TIP]
> 
> Multiple input files can be specified with `--input` option. For example, `--input file1.parquet --input file2.parquet`.

#### Converting Parquet to CSV

To convert a Parquet file into a CSV file, use the `parq convert` command.

```bash
parq convert --input path/to/your/file.parquet --format csv --output example.csv
```

#### Converting Parquet to JSON Array

To convert a Parquet file into a JSON Array, use the `parq convert` command.

```bash
parq convert --input path/to/your/file.parquet --format json --output example.json
```

#### Converting Parquet to JSON Lines

To convert a Parquet file into a JSON Lines, use the `parq convert` command.

```bash
parq convert --input path/to/your/file.parquet --format jsonl --output example.jsonl
```


### Python

#### Iterating Over Parquet Rows

To iterate over the rows of a Parquet file, use the `iter_rows` function. This allows for efficient row-by-row processing without loading the entire file into memory.

```python
from parq import to_iter

# Path to your Parquet file
file_path = "path/to/your/file.parquet"

# Iterate over Parquet rows
for row in to_iter(file_path):
    print(row)
```

#### Converting Parquet to CSV String

To convert a Parquet file into a CSV string, use the `to_csv_str` function.

```python
from parq import to_csv_str

# Path to your Parquet file
file_path = "path/to/your/file.parquet"

# Convert to CSV string
csv_str = to_csv_str(file_path)
print(csv_str)
```

#### Converting Parquet to JSON String

To convert a Parquet file into a JSON string, use the `to_json_str` function.

```python
from parq import to_json_str

# Path to your Parquet file
file_path = "path/to/your/file.parquet"

# Convert to JSON string
json_str = to_json_str(file_path)
print(json_str)
```

#### Converting Parquet to Python List

To convert a Parquet file into a Python list, where each row is represented as a dictionary within the list, use the `to_list` function.

```python
from parq import to_list

# Path to your Parquet file
file_path = "path/to/your/file.parquet"

# Convert to Python list
data_list = to_list(file_path)
print(len(data_list))
```
