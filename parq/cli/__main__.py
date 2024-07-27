#!/usr/bin/env python3
from csv import DictWriter
from json import dumps
from io import StringIO
from itertools import chain

import click
from parq import to_iter


@click.group()
def parq_cli():
    pass


@parq_cli.command()
@click.option(
    "--input",
    "-i",
    "parquet_file_paths",
    help="Input parquet files",
    type=click.Path(exists=True),
    required=True,
    multiple=True,
)
@click.option(
    "--format",
    "-f",
    "output_format",
    type=click.Choice(
        [
            "json",
            "jsonl",
            "csv",
        ],
        case_sensitive=False,
    ),
    required=True,
    help="Output format",
)
@click.option("--output", "-o", "output_file_path", help="Output file", type=click.Path(), required=False)
def convert(parquet_file_paths, output_format, output_file_path):
    """
    Convert a list of parquet files to a specified output format.
    :param parquet_file_paths: List of parquet file paths
    :param output_format: Output format
    :param output_file_path: Output file path
    :return:
    """

    iterchain = chain.from_iterable([to_iter(parquet_file_path) for parquet_file_path in parquet_file_paths])

    if output_format == "jsonl":
        if output_file_path:

            def _iter_jsonl(iterchain_):
                for item_ in iterchain_:
                    yield dumps(item_) + "\n"

            with open(output_file_path, "w") as f:
                f.writelines(_iter_jsonl(iterchain))
        else:
            for item in iterchain:
                click.echo(dumps(item))

    elif output_format == "json":
        # FIXME: Optimize this
        if output_file_path:
            with open(output_file_path, "w") as f:
                f.write(dumps(list(iterchain)))
        else:
            click.echo(dumps(list(iterchain)))

    elif output_format == "csv":
        # FIXME: Optimize this
        buffer = list(iterchain)
        if output_file_path:
            with open(output_file_path, "w") as f:
                writer = DictWriter(f, fieldnames=buffer[0].keys())
                writer.writeheader()
                writer.writerows(buffer)
        else:
            f = StringIO()
            writer = DictWriter(f, fieldnames=buffer[0].keys())
            writer.writeheader()
            writer.writerows(buffer)
            click.echo(f.getvalue())

    else:
        raise ValueError(f"Unsupported output format: {output_format}")


if __name__ == "__main__":
    parq_cli()
