use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::{ConversionError, WritePrecision};

// <measurement>[,<tag_key>=<tag_value>[,<tag_key>=<tag_value>]] <field_key>=<field_value>[,<field_key>=<field_value>] [<timestamp>]
// keys not be starting with underscore
pub struct DataPoint {
    measurement: String,
    tags: HashMap<String, String>,
    fields: HashMap<String, Value>,
    time: Option<Timestamp>,
}

pub(crate) trait LineProtocol {
    fn to_line_protocol(self, precision: WritePrecision) -> Result<String, ConversionError>;
}

impl<I> LineProtocol for I
where
    I: IntoIterator<Item = DataPoint>,
{
    fn to_line_protocol(self, precision: WritePrecision) -> Result<String, ConversionError> {
        let lines: Result<Vec<String>, ConversionError> = self
            .into_iter()
            .map(|p| p.to_line_protocol(precision))
            .collect();

        Ok(lines?.join("\n").to_string())
    }
}

impl LineProtocol for &DataPoint {
    fn to_line_protocol(self, precision: WritePrecision) -> Result<String, ConversionError> {
        debug_assert!(!self.fields.is_empty());

        let mut buf = String::new();

        buf.push_str(&self.measurement);

        for (k, v) in &self.tags {
            buf.push(',');
            buf.push_str(k);
            buf.push('=');
            buf.push_str(v);
        }

        buf.push(' ');

        let mut fields = self.fields.iter();

        let Some((k, v)) = fields.next() else {
            unreachable!("DataPoint must have at least one field!")
        };

        buf.push_str(k);
        buf.push('=');
        buf.push_str(&v.to_line_protocol()?);

        for (k, v) in fields {
            buf.push(',');
            buf.push_str(k);
            buf.push('=');
            buf.push_str(&v.to_line_protocol()?);
        }

        if let Some(timestamp) = &self.time {
            buf.push(' ');

            buf.push_str(&timestamp.to_line_protocol(precision)?)
        }

        Ok(buf)
    }
}

pub struct DataPointBuilder<const HAS_FIELD: bool = false> {
    data_point: DataPoint,
}

impl DataPointBuilder {
    pub fn new(measurement: impl Into<String>) -> DataPointBuilder {
        DataPointBuilder {
            data_point: DataPoint {
                measurement: measurement.into(),
                tags: Default::default(),
                fields: Default::default(),
                time: None,
            },
        }
    }
}

impl<const HAS_FIELD: bool> DataPointBuilder<HAS_FIELD> {
    pub fn with_field<K: Into<String>, V: Into<Value>>(
        mut self,
        key: K,
        value: V,
    ) -> DataPointBuilder<true> {
        self.data_point.fields.insert(key.into(), value.into());

        DataPointBuilder {
            data_point: self.data_point,
        }
    }

    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.data_point.tags.insert(key.into(), value.into());
        self
    }

    pub fn with_time(mut self, time: impl Into<Timestamp>) -> Self {
        self.data_point.time = Some(time.into());
        self
    }
}

/// Convert DataPointBuilder into the underlying DataPoint
///
/// Conversion is only available if there is at least one field:
/// ```
/// # use influx_write::{DataPoint, DataPointBuilder};
/// let point: DataPoint = DataPointBuilder::new("measurement").with_field("key", "value").into();
/// ```
///
/// This will therefore not compile:
/// ```compile_fail
/// # use influx_write::{DataPoint, DataPointBuilder};
/// let point: DataPoint = DataPointBuilder::new("measurement").into();
/// ```
impl From<DataPointBuilder<true>> for DataPoint {
    fn from(value: DataPointBuilder<true>) -> Self {
        value.data_point
    }
}

#[derive(PartialEq, Debug)]
pub enum Value {
    Float(f64),
    Integer(i64),
    UInteger(u64),
    String(String),
    Boolean(bool),
}

impl Value {
    fn to_line_protocol(&self) -> Result<String, ConversionError> {
        Ok(match self {
            Value::Float(f) => f.to_string(),
            Value::Integer(i) => i.to_string() + "i",
            Value::UInteger(u) => u.to_string() + "u",
            Value::String(s) => "\"".to_owned() + s + "\"",
            Value::Boolean(b) => b.to_string(),
        })
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Self::UInteger(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

#[derive(Eq, PartialEq, Debug)]
pub struct Timestamp {
    inner: DateTime<Utc>,
}

impl Timestamp {
    fn to_line_protocol(&self, precision: WritePrecision) -> Result<String, ConversionError> {
        Ok(match precision {
            WritePrecision::NS => self
                .inner
                .timestamp_nanos_opt()
                .ok_or(ConversionError::TimeConversionError(
                    "Can not convert ridiculously large date with nanosecond precision".to_owned(),
                ))?
                .to_string(),
            WritePrecision::US => self.inner.timestamp_micros().to_string(),
            WritePrecision::MS => self.inner.timestamp_millis().to_string(),
            WritePrecision::S => self.inner.timestamp().to_string(),
        })
    }
}

impl<T: Into<DateTime<Utc>>> From<T> for Timestamp {
    fn from(value: T) -> Self {
        Self {
            inner: value.into(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::time::SystemTime;

    use chrono::DateTime;

    use crate::influx::Timestamp;
    use crate::influx::Value::{Boolean, Float, Integer, String, UInteger};
    use crate::{DataPoint, DataPointBuilder};

    #[test]
    fn datapoint_builder() {
        let point: DataPoint = DataPointBuilder::new("measurement")
            .with_field("unsigned", 0u64)
            .with_field("signed", 0i64)
            .with_field("float", 0.)
            .with_field("string", "string")
            .with_field("boolean", false)
            .with_tag("tag", "value")
            .with_time(SystemTime::UNIX_EPOCH)
            .into();

        assert_eq!("measurement", point.measurement);
        assert_eq!(
            HashMap::from([
                ("unsigned".to_owned(), UInteger(0)),
                ("signed".to_owned(), Integer(0)),
                ("float".to_owned(), Float(0.)),
                ("string".to_owned(), String("string".to_owned())),
                ("boolean".to_owned(), Boolean(false))
            ]),
            point.fields
        );
        assert_eq!(
            HashMap::from([("tag".to_owned(), "value".to_owned())]),
            point.tags
        );
        assert_eq!(
            Some(Timestamp {
                inner: DateTime::UNIX_EPOCH
            }),
            point.time
        )
    }
}
