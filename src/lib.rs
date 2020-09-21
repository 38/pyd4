use d4::ptab::{DecodeResult, PTablePartitionReader, UncompressedReader};
use d4::stab::{RangeRecord, STablePartitionReader, SimpleKeyValueReader};
use d4::D4FileReader;
use d4::task::{Histogram, Mean, Task, TaskContext, TaskPartition};
use pyo3::prelude::*;
use pyo3::types::{PyString, PyTuple, PyInt};
use pyo3::class::iter::{IterNextOutput, PyIterProtocol};
use std::io::Result;

type D4Reader = D4FileReader<UncompressedReader, SimpleKeyValueReader<RangeRecord>>;

#[pyclass]
pub struct D4File {
    path: String,
}

#[pyclass]
pub struct D4Iter {
    inner: D4Reader,
    iter: Box<dyn Iterator<Item = i32> + Send + 'static >,
}
impl D4File {
    fn open(&self) -> Result<D4Reader> {
        D4Reader::open(&self.path)
    }
}

#[pyproto]
impl PyIterProtocol for D4Iter {
    fn __iter__(slf: PyRefMut<Self>) -> Result<PyRefMut<Self>>{
        Ok(slf)
    }
    fn __next__(mut slf: PyRefMut<Self>) -> IterNextOutput<i32, &'static str> {
        if let Some(next) = slf.iter.next() {
            IterNextOutput::Yield(next)
        } else {
            IterNextOutput::Return("Ended")
        }
    }
}

#[pymethods]
impl D4File {
    #[new]
    pub fn new(path: &str) -> PyResult<Self> {
        let _inner = D4Reader::open(path)?;
        Ok(Self { path: path.to_string() })
    }

    pub fn chroms(&self) -> PyResult<Vec<(String, usize)>> {
        Ok(self.open()?
            .header()
            .chrom_list()
            .iter()
            .map(|x| (x.name.clone(), x.size))
            .collect())
    }

    pub fn histogram(&self, regions: &pyo3::types::PyList, /*regions: &[(&str, u32, u32)],*/ min: i32, max: i32) -> PyResult<Vec<(Vec<(i32, u32)>, u32, u32)>>{
        let mut input = self.open()?;
        let chroms = input.header().chrom_list();
        let mut spec = vec![];
        for item in  regions.iter() {
            let (chr, begin, end) = if let Ok(chr) = item.downcast::<PyString>() {
                (chr, None, None)
            } else if let Ok(tuple) = item.downcast::<PyTuple>() {
                let tuple = tuple.as_slice();
                let chr = tuple[0].downcast()?;
                let begin = tuple.get(1).map(|x| x.downcast::<PyInt>().ok()).unwrap_or(None);
                let end = tuple.get(2).map(|x| x.downcast::<PyInt>().ok()).unwrap_or(None);
                (chr, begin, end)
            } else {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, "Invalid range spec").into());
            };
            let chr = chr.to_str()?;
            let chrom = chroms.iter().find(|x|x.name == chr);
            if chrom.is_none() {
                let msg = format!("Chrom {} doesn't exists", chr);
                return Err(std::io::Error::new(std::io::ErrorKind::Other, msg).into());
            }
            let (begin, end) = match (begin, end ) {
                (Some(start), None) => (start.extract()?, chrom.unwrap().size as u32),
                (Some(start), Some(end)) => (start.extract()?, end.extract()?) ,
                _ =>(0, chrom.unwrap().size as u32),
            };
            spec.push((chr.to_string(), begin, end));
        }
        let result = TaskContext::<_, _, Histogram>::new(&mut input, &spec, min..max)?.run();
        let mut buf = vec![];
        for (_, _, _, (below, hist, above)) in result{
            let hist:Vec<_> = (min..).zip(hist.into_iter()).collect();
            buf.push((hist, below, above));
        }
        Ok(buf)
    }

    pub fn value_iter(&self, chr: &str, left: u32, right: u32) -> PyResult<D4Iter> {
        let mut inner = self.open()?;
        let partition = inner.split(None)?;

        let chr = chr.to_string();

        let iter = partition
            .into_iter()
            .map(move |(mut ptab, mut stab)| {
                let (part_chr, begin, end) = ptab.region();
                let part_chr = part_chr.to_string();
                let pd = ptab.as_decoder();
                (if part_chr != chr {
                    0..0
                } else {
                    left.max(begin)..right.min(end)
                })
                .map(move |pos| match pd.decode(pos as usize) {
                    DecodeResult::Definitely(value) => value,
                    DecodeResult::Maybe(value) => {
                        if let Some(st_value) = stab.decode(pos) {
                            st_value
                        } else {
                            value
                        }
                    }
                })
            })
            .flatten();
        Ok(D4Iter {
            inner,
            iter: Box::new(iter),
        })
    }
}

#[pymodule]
pub fn pyd4(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<D4File>()?;
    m.add_class::<D4Iter>()?;
    Ok(())
}
