#!/usr/bin/env python3
import click
from parq import to_json_str, to_csv_str


@click.group()
def parq_cli():
    pass


@parq_cli.command()
@click.option(
    "--input", "-i", "parquet_file_path", help="Input parquet file", type=click.Path(exists=True), required=True
)
@click.option(
    "--format",
    "-f",
    "output_format",
    type=click.Choice(
        [
            "json",
            "csv",
        ],
        case_sensitive=False,
    ),
    required=True,
    help="Output format",
)
@click.option("--output", "-o", "output_file_path", help="Output file", type=click.Path(), required=False)
def convert(parquet_file_path, output_format, output_file_path):
    if output_format == "json":
        output_string = to_json_str(str(parquet_file_path))
    elif output_format == "csv":
        output_string = to_csv_str(str(parquet_file_path))
    else:
        raise ValueError(f"Unsupported output format {output_format}")

    if output_file_path:
        with open(output_file_path, "w") as f:
            f.write(output_string)
    else:
        click.echo(output_string)


if __name__ == "__main__":
    parq_cli()
