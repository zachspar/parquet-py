use pyo3::prelude::*;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::{fs::File, path::Path};

// convert parquet file to json string
#[pyfunction]
fn to_json_str(path: &str) -> PyResult<String> {
    let file_path = Path::new(path);
    if let Ok(file) = File::open(&file_path) {
        let reader = SerializedFileReader::new(file).unwrap();

        // iterate through reader and add to json list string
        let mut json_str = "[".to_string();
        for row in reader.get_row_iter(None).unwrap() {
            json_str.push_str(&row.unwrap().to_json_value().to_string());
            json_str.push_str(",");
        }

        json_str.pop();
        json_str.push_str("]");

        // return json string
        return Ok(json_str);
    }

    // return ValueError if file not found
    Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Unable to open parquet file"))
}

// python module
#[pymodule]
fn parq(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(to_json_str, m)?)?;
    Ok(())
}
