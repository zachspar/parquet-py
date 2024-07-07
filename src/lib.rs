use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyBool};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Row;
use std::{fs::File, path::Path};
use serde_json::Value;

struct PyValue(Value);

impl ToPyObject for PyValue {
    fn to_object(&self, py: Python) -> PyObject {
        match value_to_py_object(py, &self.0) {
            Ok(obj) => obj,
            Err(_) => py.None(), // Fallback to None in case of error, adjust as needed
        }
    }
}

/// Converts a serde_json `Value` to a PyObject.
fn value_to_py_object(py: Python, value: &Value) -> PyResult<PyObject> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Bool(b) => Ok(PyBool::new_bound(py, *b).into_py(py)), // Adjusted for PyBool
        Value::Number(num) => {
            if let Some(i) = num.as_i64() {
                Ok(i.into_py(py))
            } else if let Some(f) = num.as_f64() {
                Ok(f.into_py(py))
            } else {
                Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Unsupported number type",
                ))
            }
        },
        Value::String(s) => Ok(s.into_py(py)),
        Value::Array(arr) => {
            let py_list = PyList::empty_bound(py);
            for item in arr {
                py_list.append(value_to_py_object(py, item)?)?;
            }
            Ok(py_list.into_py(py))
        },
        Value::Object(obj) => {
            let py_dict = PyDict::new_bound(py); // Correct usage of PyDict
            for (k, v) in obj {
                py_dict.set_item(k, value_to_py_object(py, v)?)?;
            }
            Ok(py_dict.into_py(py))
        },
    }
}

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
    } else {
        // return ValueError if file not found
        Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Unable to open parquet file"))
    }
}

#[pyclass]
struct ParquetRowIterator {
    iter: Box<dyn Iterator<Item = PyResult<PyObject>> + Send>,
}

#[pymethods]
impl ParquetRowIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyResult<PyObject>> {
        slf.iter.next()
    }
}

// Separate function to create a ParquetRowIterator instance
fn create_parquet_row_iterator(path: String, py: Python) -> PyResult<ParquetRowIterator> {
    let file = File::open(Path::new(&path)).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
    let reader = SerializedFileReader::new(file).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))?;
    let row_iter = reader.get_row_iter(None).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))?;
    let iter = Box::new(row_iter.map(|row_result| {
        row_result
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))
            .and_then(|row| row_to_py_dict(Python::acquire_gil().python(), &row))
    }));
    Ok(ParquetRowIterator { iter })
}

#[pyfunction]
fn to_iter(path: &str, py: Python) -> PyResult<PyObject> {
    let iterator = create_parquet_row_iterator(path.to_string(), py)?;
    let iterator_obj = Py::new(py, iterator)?;
    Ok(iterator_obj.to_object(py))
}

// Remember to add the to_iter function to your module
#[pymodule]
fn parq(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(to_iter, m)?)?;
    Ok(())
}

#[pyfunction]
fn to_list(path: &str, py: Python) -> PyResult<PyObject> {
    let file_path = Path::new(path);
    if let Ok(file) = File::open(&file_path) {
        let reader = SerializedFileReader::new(file).unwrap();

        let list = PyList::empty_bound(py);
        for row in reader.get_row_iter(None).unwrap() {
            let row_dict = row.unwrap().to_json_value();
            let dict = PyDict::new_bound(py);
            for (key, value) in row_dict.as_object().unwrap() {
                dict.set_item(key, PyValue(value.clone()))?;
                list.append(&dict)?;
            }
        }

        Ok(list.into())
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Unable to open parquet file"))
    }
}

// python module
#[pymodule]
fn parq(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(to_json_str, m)?)?;
    m.add_function(wrap_pyfunction!(to_list, m)?)?;
    m.add_function(wrap_pyfunction!(to_iter, m)?)?;
    Ok(())
}
