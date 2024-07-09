[![PyPI version](https://badge.fury.io/py/parquet-py.svg)](https://badge.fury.io/py/parquet-py)

# Parquet-Py

Parquet-Py is a simple Python API & CLI designed to facilitate the interaction with Parquet files. It allows users to convert Parquet files into JSON strings, lists, or iterators for easy manipulation and access in Python applications.

Using Rust bindings under the hood, Parquet-Py provides a fast and efficient way to work with Parquet files, making it ideal for processing large datasets.

## Features

- **Convert Parquet to JSON String**: Easily convert your Parquet files into a JSON string format for quick inspection or processing.
- **Convert Parquet to Python List**: Transform your Parquet files into Python lists, where each row is represented as a dictionary within the list.
- **Iterable Parquet Rows**: Access Parquet file rows through an iterator, allowing for efficient row-by-row processing without loading the entire file into memory.

## Installation

### PyPI
`pip install parquet-py`

## Usage
### Command-Line Interface

#### Converting Parquet to JSON

To convert a Parquet file into a JSON string, use the `parq convert` command.

```bash
parq convert --input path/to/your/file.parquet --format json --output example.json
```

### Python

#### Converting Parquet to JSON String

To convert a Parquet file into a JSON string, use the `to_json_str` function. This is useful for quick inspection or processing of the data.

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
