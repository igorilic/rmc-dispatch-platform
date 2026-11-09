//! Customer mappings: `customers/<name>/mapping.yaml` turns one customer's format into canonical
//! events, so no customer needs Rust code of its own (SPEC §5).
//!
//! A delivery (a webhook body or an SFTP file) holds one record, or one per CSV row. Each record
//! goes through the same steps:
//! 1. Pick the schema version whose `when` matches.
//! 2. Read each canonical field from a CSV column, a JSON pointer, or an XML path (`@attr` for attributes).
//! 3. Convert it: `map` values, numbers (decimal, decimal comma, integer, `scale`), timestamps
//!    (RFC 3339, or a `datetime` format read in the mapping's `timezone`).
//! 4. Check it against master data (see `validate`).
//! 5. Build the event, with an idempotency key from the listed fields or a hash of the content.
//!
//! Every problem in a record is collected, not just the first. So the DLQ and
//! `ingest-cli validate` show everything at once, ordered by canonical field.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

use chrono::{DateTime, LocalResult, NaiveDateTime, SecondsFormat, TimeZone, Utc};
use chrono_tz::Tz;
use domain::{
    Channel, CustomerId, Event, EventEnvelope, EventSource, MixCode, Order, OrderId, OrderStatus,
    Seed, SiteId,
};
use quick_xml::XmlVersion;
use quick_xml::events::{BytesStart, Event as XmlEvent};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::validate;

#[derive(Debug, thiserror::Error)]
pub enum MappingError {
    #[error("{}: {source}", path.display())]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("{}: {message}", path.display())]
    Invalid { path: PathBuf, message: String },
}

/// The canonical fields a mapping can fill. Declaration order is the order errors are reported in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Field {
    OrderRef,
    Customer,
    ShipTo,
    Mix,
    VolumeM3,
    RequestedStart,
    PourRateM3PerH,
    MaxGapMinutes,
    Status,
    UpdatedAt,
    Revision,
}

const REQUIRED: [Field; 9] = [
    Field::OrderRef,
    Field::Customer,
    Field::ShipTo,
    Field::Mix,
    Field::VolumeM3,
    Field::RequestedStart,
    Field::PourRateM3PerH,
    Field::MaxGapMinutes,
    Field::Status,
];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MappingFile {
    customer: String,
    channel: Channel,
    format: Format,
    csv: Option<CsvFile>,
    timezone: Option<String>,
    fields: Option<BTreeMap<Field, FieldSpec>>,
    idempotency: Option<IdempotencyFile>,
    #[serde(default)]
    versions: Vec<VersionFile>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Format {
    Json,
    Csv,
    Xml,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CsvFile {
    delimiter: char,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct VersionFile {
    when: Condition,
    fields: BTreeMap<Field, FieldSpec>,
    idempotency: IdempotencyFile,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Condition {
    path: String,
    equals: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdempotencyFile {
    #[serde(default)]
    content_hash: bool,
    #[serde(default)]
    fields: Vec<Field>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FieldSpec {
    column: Option<String>,
    path: Option<String>,
    number: Option<NumberFormat>,
    scale: Option<f64>,
    datetime: Option<String>,
    map: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum NumberFormat {
    Decimal,
    DecimalComma,
    Integer,
}

impl FieldSpec {
    /// How the customer's side calls this field: a CSV column or a path.
    pub(crate) fn name(&self) -> &str {
        self.column
            .as_deref()
            .or(self.path.as_deref())
            .unwrap_or_default()
    }
}

/// One loaded, checked `mapping.yaml`.
#[derive(Debug)]
pub struct Mapping {
    pub integration: String,
    /// The master-data customer this integration may order for.
    pub customer: String,
    pub channel: Channel,
    format: Format,
    delimiter: u8,
    timezone: Tz,
    versions: Vec<Version>,
}

#[derive(Debug)]
struct Version {
    when: Option<Condition>,
    fields: BTreeMap<Field, FieldSpec>,
    idempotency: Idempotency,
}

#[derive(Debug)]
enum Idempotency {
    ContentHash,
    Fields(Vec<Field>),
}

/// One problem with one field of one record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldError {
    /// `None` for problems with the whole record, which sort first.
    pub field: Option<Field>,
    /// The customer's name for it: a CSV column or a path.
    pub name: String,
    pub message: String,
}

impl fmt::Display for FieldError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.name, self.message)
    }
}

/// What arrived: a webhook body or a whole SFTP file.
pub struct Delivery<'a> {
    pub body: &'a [u8],
    pub channel: Channel,
    /// E.g. the file name; CSV rows add ` line N`.
    pub reference: &'a str,
    pub received_at: DateTime<Utc>,
}

/// The result for one record. `raw` is what to dead-letter: the whole document, or the CSV header
/// plus the one row, so a replay can map it on its own.
#[derive(Debug)]
pub struct Outcome {
    pub record: String,
    pub raw: Vec<u8>,
    pub result: Result<EventEnvelope, Vec<FieldError>>,
}

impl Mapping {
    pub fn load(dir: &Path, integration: &str) -> Result<Mapping, MappingError> {
        let path = dir.join(integration).join("mapping.yaml");
        let yaml = std::fs::read_to_string(&path).map_err(|source| MappingError::Read {
            path: path.clone(),
            source,
        })?;
        Mapping::parse(integration, &yaml)
            .map_err(|message| MappingError::Invalid { path, message })
    }

    /// Every `<dir>/<integration>/mapping.yaml`, sorted by integration name.
    pub fn load_all(dir: &Path) -> Result<Vec<Mapping>, MappingError> {
        let read = |source| MappingError::Read {
            path: dir.to_owned(),
            source,
        };
        let mut names = Vec::new();
        for entry in std::fs::read_dir(dir).map_err(read)? {
            let entry = entry.map_err(read)?;
            if entry.path().join("mapping.yaml").is_file() {
                names.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
        names.sort();
        names.iter().map(|name| Mapping::load(dir, name)).collect()
    }

    pub fn parse(integration: &str, yaml: &str) -> Result<Mapping, String> {
        let file: MappingFile = serde_saphyr::from_str(yaml).map_err(|e| e.to_string())?;
        if file.channel == Channel::Mqtt {
            return Err("customer integrations arrive by webhook or sftp, not mqtt".into());
        }
        let timezone_name = file.timezone.as_deref().unwrap_or("Europe/Berlin");
        let timezone: Tz = timezone_name
            .parse()
            .map_err(|_| format!("unknown timezone {timezone_name}"))?;
        let delimiter = file.csv.map_or(Ok(b','), |c| {
            u8::try_from(c.delimiter)
                .map_err(|_| format!("CSV delimiter {:?} must be ASCII", c.delimiter))
        })?;

        let versions = match (file.fields, file.idempotency, file.versions.is_empty()) {
            (Some(fields), Some(idempotency), true) => vec![(None, fields, idempotency)],
            (None, None, false) => file
                .versions
                .into_iter()
                .map(|v| (Some(v.when), v.fields, v.idempotency))
                .collect(),
            _ => {
                return Err(
                    "use either top-level fields and idempotency, or versions, not both".into(),
                );
            }
        };
        let versions = versions
            .into_iter()
            .map(|(when, fields, idempotency)| {
                check_version(file.format, when, fields, idempotency)
            })
            .collect::<Result<_, _>>()?;
        Ok(Mapping {
            integration: integration.to_owned(),
            customer: file.customer,
            channel: file.channel,
            format: file.format,
            delimiter,
            timezone,
            versions,
        })
    }

    /// Maps a delivery into one outcome per record.
    pub fn map(&self, delivery: &Delivery<'_>, seed: &Seed) -> Vec<Outcome> {
        let whole = |message: String| {
            vec![Outcome {
                record: "document".into(),
                raw: delivery.body.to_vec(),
                result: Err(vec![FieldError {
                    field: None,
                    name: delivery.reference.into(),
                    message,
                }]),
            }]
        };
        match self.format {
            Format::Csv => self.map_csv(delivery, seed),
            Format::Json => match serde_json::from_slice::<Value>(delivery.body) {
                Ok(doc) => vec![self.map_document(&Record::Json(&doc), delivery, seed)],
                Err(e) => whole(format!("invalid JSON: {e}")),
            },
            Format::Xml => match parse_xml(delivery.body) {
                Ok(doc) => vec![self.map_document(&Record::Xml(&doc), delivery, seed)],
                Err(e) => whole(e),
            },
        }
    }

    fn map_document(&self, record: &Record<'_>, delivery: &Delivery<'_>, seed: &Seed) -> Outcome {
        let result = self.map_record(record, delivery, delivery.reference, seed);
        Outcome {
            record: result.0.unwrap_or_else(|| "document".into()),
            raw: delivery.body.to_vec(),
            result: result.1,
        }
    }

    fn map_csv(&self, delivery: &Delivery<'_>, seed: &Seed) -> Vec<Outcome> {
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(self.delimiter)
            .from_reader(delivery.body);
        let headers = match reader.headers() {
            Ok(headers) => headers.clone(),
            Err(e) => {
                let error = FieldError {
                    field: None,
                    name: delivery.reference.into(),
                    message: format!("unreadable CSV header: {e}"),
                };
                return vec![Outcome {
                    record: "header".into(),
                    raw: delivery.body.to_vec(),
                    result: Err(vec![error]),
                }];
            }
        };
        let mut outcomes = Vec::new();
        for row in reader.records() {
            let outcome = match row {
                Ok(row) => {
                    let line = row.position().map_or(0, csv::Position::line);
                    let reference = format!("{} line {line}", delivery.reference);
                    let (order_ref, result) = self.map_record(
                        &Record::Csv {
                            headers: &headers,
                            row: &row,
                        },
                        delivery,
                        &reference,
                        seed,
                    );
                    let record = match order_ref {
                        Some(order_ref) => format!("line {line} ({order_ref})"),
                        None => format!("line {line}"),
                    };
                    Outcome {
                        record,
                        raw: self.csv_row(&headers, &row),
                        result,
                    }
                }
                Err(e) => {
                    let line = e.position().map_or(0, csv::Position::line);
                    let error = FieldError {
                        field: None,
                        name: format!("line {line}"),
                        message: format!("unreadable row: {e}"),
                    };
                    Outcome {
                        record: format!("line {line}"),
                        raw: delivery.body.to_vec(),
                        result: Err(vec![error]),
                    }
                }
            };
            outcomes.push(outcome);
        }
        outcomes
    }

    fn csv_row(&self, headers: &csv::StringRecord, row: &csv::StringRecord) -> Vec<u8> {
        let mut writer = csv::WriterBuilder::new()
            .delimiter(self.delimiter)
            .terminator(csv::Terminator::Any(b'\n'))
            .from_writer(Vec::new());
        let written = writer
            .write_record(headers)
            .and_then(|()| writer.write_record(row));
        match (written, writer.into_inner()) {
            (Ok(()), Ok(bytes)) => bytes,
            _ => Vec::new(),
        }
    }

    /// Returns the order ref (if it could be read) and the event or every error.
    fn map_record(
        &self,
        record: &Record<'_>,
        delivery: &Delivery<'_>,
        reference: &str,
        seed: &Seed,
    ) -> (Option<String>, Result<EventEnvelope, Vec<FieldError>>) {
        let version = match self.version(record) {
            Ok(version) => version,
            Err(error) => return (None, Err(vec![error])),
        };
        let (values, mut errors) = self.convert(version, record);
        errors.extend(validate::check(self, &values, &version.fields, seed));
        let order_ref = values.order_ref.clone();
        if !errors.is_empty() {
            errors.sort_by_key(|e| e.field);
            return (order_ref, Err(errors));
        }
        match values.into_order() {
            Some((order_ref, order, values)) => {
                let id = match &version.idempotency {
                    Idempotency::ContentHash => content_key(&self.integration, &order_ref, &order),
                    Idempotency::Fields(fields) => fields_key(&self.integration, fields, &values),
                };
                let source_revision = values
                    .revision
                    .clone()
                    .or_else(|| values.updated_at.map(rfc3339));
                let event = if order.status == OrderStatus::Cancelled {
                    Event::OrderCancelled {
                        order: order.id,
                        customer: order.customer,
                        source_revision,
                    }
                } else {
                    Event::OrderUpserted {
                        order,
                        source_revision,
                    }
                };
                let envelope = EventEnvelope {
                    id,
                    occurred_at: values.updated_at.unwrap_or(delivery.received_at),
                    received_at: delivery.received_at,
                    source: EventSource {
                        integration: self.integration.clone(),
                        channel: delivery.channel,
                        schema_version: version.when.as_ref().map(|w| w.equals.clone()),
                        reference: reference.to_owned(),
                    },
                    event,
                };
                (Some(order_ref), Ok(envelope))
            }
            // Unreachable after a clean conversion: every required field is checked at load time.
            None => (
                order_ref,
                Err(vec![FieldError {
                    field: None,
                    name: reference.into(),
                    message: "incomplete order".into(),
                }]),
            ),
        }
    }

    fn version(&self, record: &Record<'_>) -> Result<&Version, FieldError> {
        let mut conditions = self
            .versions
            .iter()
            .filter_map(|v| v.when.as_ref().map(|w| (v, w)))
            .peekable();
        let Some((_, first)) = conditions.peek() else {
            return self.versions.first().ok_or_else(|| FieldError {
                field: None,
                name: "mapping".into(),
                message: "no versions".into(),
            });
        };
        let path = first.path.clone();
        let actual = record.at_path(&path);
        let mut expected = Vec::new();
        for (version, when) in conditions {
            if record.at_path(&when.path).as_deref() == Some(when.equals.as_str()) {
                return Ok(version);
            }
            expected.push(format!("{:?}", when.equals));
        }
        let found = actual.map_or_else(|| "missing, so it".to_owned(), |a| format!("{a:?}"));
        let message = format!(
            "{found} matches no schema version (expected {})",
            expected.join(" or ")
        );
        Err(FieldError {
            field: None,
            name: path,
            message,
        })
    }

    fn convert(&self, version: &Version, record: &Record<'_>) -> (Values, Vec<FieldError>) {
        let mut values = Values::default();
        let mut errors = Vec::new();
        for (&field, spec) in &version.fields {
            let error = |message: String| FieldError {
                field: Some(field),
                name: spec.name().to_owned(),
                message,
            };
            let Some(mut raw) = record.get(spec) else {
                errors.push(error("missing".into()));
                continue;
            };
            if let Some(map) = &spec.map {
                match map.get(&raw) {
                    Some(target) => raw = target.clone(),
                    None => {
                        let known: Vec<&str> = map.keys().map(String::as_str).collect();
                        errors.push(error(format!(
                            "unknown value {raw:?} (expected one of {})",
                            known.join(", ")
                        )));
                        continue;
                    }
                }
            }
            if let Err(message) = values.set(field, spec, raw, self.timezone) {
                errors.push(error(message));
            }
        }
        (values, errors)
    }
}

fn check_version(
    format: Format,
    when: Option<Condition>,
    fields: BTreeMap<Field, FieldSpec>,
    idempotency: IdempotencyFile,
) -> Result<Version, String> {
    let snake = |f: &Field| {
        format!("{f:?}").chars().fold(String::new(), |mut s, c| {
            if c.is_uppercase() && !s.is_empty() {
                s.push('_');
            }
            s.push(c.to_ascii_lowercase());
            s
        })
    };
    for required in REQUIRED {
        if !fields.contains_key(&required) {
            return Err(format!("missing field {}", snake(&required)));
        }
    }
    for (field, spec) in &fields {
        let column_or_path = match (spec.column.is_some(), spec.path.is_some()) {
            (true, false) if format == Format::Csv => Ok(()),
            (false, true) if format != Format::Csv => Ok(()),
            (true, false) | (false, true) => {
                Err("CSV fields use column, JSON and XML fields use path")
            }
            _ => Err("exactly one of column or path"),
        };
        column_or_path.map_err(|e| format!("{}: {e}", snake(field)))?;
    }
    let status = fields.get(&Field::Status).and_then(|s| s.map.as_ref());
    let status = status.ok_or("status needs a map to our order statuses")?;
    for target in status.values() {
        serde_json::from_value::<OrderStatus>(Value::String(target.clone()))
            .map_err(|_| format!("status maps to {target}, which is not an order status"))?;
    }
    let idempotency = match (idempotency.content_hash, idempotency.fields.is_empty()) {
        (true, true) => Idempotency::ContentHash,
        (false, false) => {
            if let Some(unmapped) = idempotency.fields.iter().find(|f| !fields.contains_key(f)) {
                return Err(format!(
                    "idempotency field {} is not mapped",
                    snake(unmapped)
                ));
            }
            Idempotency::Fields(idempotency.fields)
        }
        _ => {
            return Err(
                "idempotency needs exactly one of content_hash: true or fields: [...]".into(),
            );
        }
    };
    Ok(Version {
        when,
        fields,
        idempotency,
    })
}

/// Converted field values of one record, before they become an order.
#[derive(Debug, Default)]
pub(crate) struct Values {
    pub(crate) order_ref: Option<String>,
    pub(crate) customer: Option<String>,
    pub(crate) ship_to: Option<String>,
    pub(crate) mix: Option<String>,
    volume_m3: Option<f64>,
    requested_start: Option<DateTime<Utc>>,
    pour_rate_m3_per_h: Option<f64>,
    max_gap_minutes: Option<u32>,
    status: Option<OrderStatus>,
    updated_at: Option<DateTime<Utc>>,
    revision: Option<String>,
}

impl Values {
    fn set(&mut self, field: Field, spec: &FieldSpec, raw: String, tz: Tz) -> Result<(), String> {
        match field {
            Field::OrderRef => self.order_ref = Some(raw),
            Field::Customer => self.customer = Some(raw),
            Field::ShipTo => self.ship_to = Some(raw),
            Field::Mix => self.mix = Some(raw),
            Field::Revision => self.revision = Some(raw),
            Field::VolumeM3 => self.volume_m3 = Some(positive(number(spec, &raw)?, &raw)?),
            Field::PourRateM3PerH => {
                self.pour_rate_m3_per_h = Some(positive(number(spec, &raw)?, &raw)?)
            }
            Field::MaxGapMinutes => {
                let gap: u32 = raw
                    .parse()
                    .map_err(|_| format!("{raw:?} is not a whole number"))?;
                self.max_gap_minutes = Some(positive(f64::from(gap), &raw).map(|_| gap)?);
            }
            Field::RequestedStart => self.requested_start = Some(timestamp(spec, &raw, tz)?),
            Field::UpdatedAt => self.updated_at = Some(timestamp(spec, &raw, tz)?),
            Field::Status => {
                let status = serde_json::from_value(Value::String(raw.clone()));
                self.status = Some(status.map_err(|_| format!("{raw} is not an order status"))?);
            }
        }
        Ok(())
    }

    fn into_order(self) -> Option<(String, Order, Values)> {
        let customer = self.customer.clone()?;
        let order_ref = self.order_ref.clone()?;
        let order = Order {
            id: OrderId(format!("{customer}/{order_ref}")),
            customer: CustomerId(customer),
            site: SiteId(self.ship_to.clone()?),
            mix: MixCode(self.mix.clone()?),
            total_volume_m3: self.volume_m3?,
            requested_start: self.requested_start?,
            pour_rate_m3_per_h: self.pour_rate_m3_per_h?,
            max_gap_minutes: self.max_gap_minutes?,
            status: self.status?,
        };
        Some((order_ref, order, self))
    }

    fn key_part(&self, field: Field) -> String {
        let text = |v: &Option<String>| v.clone().unwrap_or_default();
        let number = |v: Option<f64>| v.map(|n| n.to_string()).unwrap_or_default();
        match field {
            Field::OrderRef => text(&self.order_ref),
            Field::Customer => text(&self.customer),
            Field::ShipTo => text(&self.ship_to),
            Field::Mix => text(&self.mix),
            Field::Revision => text(&self.revision),
            Field::VolumeM3 => number(self.volume_m3),
            Field::PourRateM3PerH => number(self.pour_rate_m3_per_h),
            Field::MaxGapMinutes => self
                .max_gap_minutes
                .map(|n| n.to_string())
                .unwrap_or_default(),
            Field::RequestedStart => self.requested_start.map(rfc3339).unwrap_or_default(),
            Field::UpdatedAt => self.updated_at.map(rfc3339).unwrap_or_default(),
            Field::Status => self.status.map(|s| format!("{s:?}")).unwrap_or_default(),
        }
    }
}

fn rfc3339(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn number(spec: &FieldSpec, raw: &str) -> Result<f64, String> {
    let parsed = match spec.number.unwrap_or(NumberFormat::Decimal) {
        NumberFormat::DecimalComma if raw.contains('.') => None,
        NumberFormat::DecimalComma => raw.replace(',', ".").parse::<f64>().ok(),
        NumberFormat::Decimal | NumberFormat::Integer => raw.parse::<f64>().ok(),
    };
    let value = parsed
        .filter(|v| v.is_finite())
        .ok_or_else(|| match spec.number {
            Some(NumberFormat::DecimalComma) => {
                format!("{raw:?} is not a decimal-comma number (e.g. 12,5)")
            }
            _ => format!("{raw:?} is not a number"),
        })?;
    // ponytail: round scaled values to 6 decimals (1 ml of m³), so 0.001 × litres has no float noise.
    Ok(spec
        .scale
        .map_or(value, |s| (value * s * 1e6).round() / 1e6))
}

fn positive(value: f64, raw: &str) -> Result<f64, String> {
    if value > 0.0 {
        Ok(value)
    } else {
        Err(format!("must be greater than 0, got {raw}"))
    }
}

fn timestamp(spec: &FieldSpec, raw: &str, tz: Tz) -> Result<DateTime<Utc>, String> {
    let Some(format) = &spec.datetime else {
        let parsed = DateTime::parse_from_rfc3339(raw)
            .map_err(|_| format!("{raw:?} is not an RFC 3339 timestamp"))?;
        return Ok(parsed.with_timezone(&Utc));
    };
    let local = NaiveDateTime::parse_from_str(raw, format)
        .map_err(|_| format!("{raw:?} does not match {format}"))?;
    match tz.from_local_datetime(&local) {
        LocalResult::Single(t) => Ok(t.with_timezone(&Utc)),
        LocalResult::Ambiguous(..) => Err(format!("{raw:?} is ambiguous in {tz} (clocks change)")),
        LocalResult::None => Err(format!("{raw:?} does not exist in {tz} (clocks change)")),
    }
}

/// `<integration>:<order ref>:<16 hex of sha256 over the order>`. Stable across releases, unlike std's hasher.
pub fn content_key(integration: &str, order_ref: &str, order: &Order) -> String {
    let digest = Sha256::digest(serde_json::to_vec(order).unwrap_or_default());
    let hex: String = digest.iter().take(8).map(|b| format!("{b:02x}")).collect();
    format!("{integration}:{order_ref}:{hex}")
}

fn fields_key(integration: &str, fields: &[Field], values: &Values) -> String {
    let parts: Vec<String> = fields.iter().map(|f| values.key_part(*f)).collect();
    format!("{integration}:{}", parts.join(":"))
}

/// One record to read fields from.
enum Record<'a> {
    Json(&'a Value),
    Xml(&'a XmlElement),
    Csv {
        headers: &'a csv::StringRecord,
        row: &'a csv::StringRecord,
    },
}

impl Record<'_> {
    /// The trimmed value, or `None` if it is missing or blank.
    fn get(&self, spec: &FieldSpec) -> Option<String> {
        match self {
            Record::Csv { headers, row } => {
                let column = spec.column.as_deref()?;
                let index = headers.iter().position(|h| h == column)?;
                row.get(index).map(str::to_owned)
            }
            _ => self.at_path(spec.path.as_deref()?),
        }
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty())
    }

    fn at_path(&self, path: &str) -> Option<String> {
        match self {
            Record::Json(doc) => match doc.pointer(path)? {
                Value::String(s) => Some(s.clone()),
                Value::Number(n) => Some(n.to_string()),
                Value::Bool(b) => Some(b.to_string()),
                _ => None,
            },
            Record::Xml(root) => root.at_path(path),
            Record::Csv { .. } => None,
        }
    }
}

/// Just enough XML tree to read paths like `/Order/Header/@orderNo`.
#[derive(Debug, Default)]
struct XmlElement {
    name: String,
    attributes: Vec<(String, String)>,
    text: String,
    children: Vec<XmlElement>,
}

impl XmlElement {
    fn at_path(&self, path: &str) -> Option<String> {
        let mut parts = path.strip_prefix('/')?.split('/');
        if parts.next()? != self.name {
            return None;
        }
        let mut element = self;
        let mut parts = parts.peekable();
        while let Some(part) = parts.next() {
            if let Some(attribute) = part.strip_prefix('@') {
                let value = element
                    .attributes
                    .iter()
                    .find(|(k, _)| k == attribute)
                    .map(|(_, v)| v.clone());
                return if parts.peek().is_none() { value } else { None };
            }
            element = element.children.iter().find(|c| c.name == part)?;
        }
        Some(element.text.trim().to_owned())
    }
}

/// Parses with quick-xml, which never expands DTD entities (no XXE). Only the five predefined
/// entities and character references are resolved; any other entity is an error.
fn parse_xml(body: &[u8]) -> Result<XmlElement, String> {
    let text = std::str::from_utf8(body).map_err(|e| format!("XML is not UTF-8: {e}"))?;
    let mut reader = quick_xml::Reader::from_str(text);
    let mut stack: Vec<XmlElement> = Vec::new();
    let mut root = None;
    loop {
        let event = reader
            .read_event()
            .map_err(|e| format!("invalid XML at byte {}: {e}", reader.error_position()))?;
        match event {
            XmlEvent::Start(e) => stack.push(element(&e)?),
            XmlEvent::Empty(e) => attach(&mut stack, &mut root, element(&e)?),
            XmlEvent::End(_) => {
                let done = stack.pop().ok_or("invalid XML: unexpected end tag")?;
                attach(&mut stack, &mut root, done);
            }
            XmlEvent::Text(t) => {
                if let Some(open) = stack.last_mut() {
                    open.text.push_str(&t.xml_content(XmlVersion::Implicit1_0));
                }
            }
            XmlEvent::GeneralRef(r) => {
                let resolved = entity(&r)?;
                if let Some(open) = stack.last_mut() {
                    open.text.push(resolved);
                }
            }
            XmlEvent::Eof => break,
            _ => {}
        }
    }
    root.ok_or_else(|| "invalid XML: no root element".into())
}

fn attach(stack: &mut [XmlElement], root: &mut Option<XmlElement>, element: XmlElement) {
    match stack.last_mut() {
        Some(parent) => parent.children.push(element),
        None => *root = Some(element),
    }
}

fn element(start: &BytesStart<'_>) -> Result<XmlElement, String> {
    let mut attributes = Vec::new();
    for attribute in start.attributes() {
        let attribute = attribute.map_err(|e| format!("invalid XML attribute: {e}"))?;
        let value = attribute
            .normalized_value(XmlVersion::Implicit1_0)
            .map_err(|e| format!("invalid XML attribute value: {e}"))?;
        attributes.push((attribute.key.as_ref().to_owned(), value.into_owned()));
    }
    Ok(XmlElement {
        name: start.name().as_ref().to_owned(),
        attributes,
        ..XmlElement::default()
    })
}

fn entity(name: &str) -> Result<char, String> {
    match name {
        "amp" => Ok('&'),
        "lt" => Ok('<'),
        "gt" => Ok('>'),
        "quot" => Ok('"'),
        "apos" => Ok('\''),
        _ => name
            .strip_prefix('#')
            .and_then(|n| match n.strip_prefix('x') {
                Some(hex) => u32::from_str_radix(hex, 16).ok(),
                None => n.parse().ok(),
            })
            .and_then(char::from_u32)
            .ok_or_else(|| {
                format!("invalid XML: unknown entity &{name}; (DTD entities are never expanded)")
            }),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use chrono::{TimeDelta, TimeZone};
    use domain::{CustomerId, MixCode, OrderId, OrderStatus, SiteId};
    use proptest::prelude::*;
    use serde_json::Value;

    use super::*;

    fn customers_dir() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../customers")
    }

    fn received_at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 9, 9, 20, 0).unwrap()
    }

    fn map_bytes(integration: &str, body: &[u8], reference: &str) -> Vec<Outcome> {
        let mapping = Mapping::load(&customers_dir(), integration).unwrap();
        let seed = Seed::rhine_neckar().unwrap();
        let delivery = Delivery {
            body,
            channel: mapping.channel,
            reference,
            received_at: received_at(),
        };
        mapping.map(&delivery, &seed)
    }

    fn map_sample(integration: &str, file: &str) -> Vec<Outcome> {
        let body =
            std::fs::read(customers_dir().join(integration).join("samples").join(file)).unwrap();
        map_bytes(integration, &body, file)
    }

    fn errors(outcome: &Outcome) -> Vec<String> {
        outcome
            .result
            .as_ref()
            .unwrap_err()
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    /// Golden files write content-hash ids as `<content-hash>`: the hash itself is opaque.
    fn without_hash(mut event: Value) -> Value {
        let id = event["id"].as_str().unwrap().to_owned();
        let (prefix, last) = id.rsplit_once(':').unwrap();
        if last.len() == 16 && last.chars().all(|c| c.is_ascii_hexdigit()) {
            event["id"] = Value::String(format!("{prefix}:<content-hash>"));
        }
        event
    }

    #[test]
    fn good_samples_map_to_the_golden_events() {
        let cases = [
            ("bauwerk-ag", "order.json", "order.events.json"),
            ("rheinbau-gmbh", "orders.csv", "orders.events.json"),
            ("neckar-infra", "order-v1.xml", "order-v1.events.json"),
            ("neckar-infra", "order-v2.xml", "order-v2.events.json"),
        ];
        for (integration, sample, golden) in cases {
            let events: Vec<Value> = map_sample(integration, sample)
                .into_iter()
                .map(|o| without_hash(serde_json::to_value(o.result.unwrap()).unwrap()))
                .collect();
            let path = customers_dir()
                .join(integration)
                .join("samples")
                .join(golden);
            let golden: Value =
                serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
            assert_eq!(Value::Array(events), golden, "{integration}/{sample}");
        }
    }

    #[test]
    fn broken_samples_report_every_problem() {
        let rows = map_sample("rheinbau-gmbh", "broken.csv");
        assert_eq!(rows.len(), 3);
        assert!(rows[0].result.is_ok() && rows[1].result.is_ok());
        assert_eq!(rows[2].record, "line 4 (RB-7790)");
        assert_eq!(
            errors(&rows[2]),
            [
                "Warenempfänger: ship-to 50001007 belongs to customer 10001001, not 10001002",
                "Menge: \"12.5\" is not a decimal-comma number (e.g. 12,5)",
                "Liefertermin: \"29.03.2026 02:30\" does not exist in Europe/Berlin (clocks change)",
            ]
        );
        assert_eq!(
            String::from_utf8(rows[2].raw.clone()).unwrap(),
            "Auftragsnr;Kundennr;Warenempfänger;Sorte;Menge;Liefertermin;Leistung;MaxPause;Status\n\
             RB-7790;10001002;50001007;C30/37-XC4;12.5;29.03.2026 02:30;30,0;20;bestätigt\n",
            "a dead-lettered row keeps its header so it can be replayed on its own"
        );

        let doc = map_sample("bauwerk-ag", "broken.json");
        assert_eq!(doc[0].record, "BW-2026-0099");
        assert_eq!(
            errors(&doc[0]),
            [
                "/product: unknown mix C99/99",
                "/qty/value: must be greater than 0, got -5",
                "/pourStart: missing",
            ]
        );

        let doc = map_sample("neckar-infra", "broken.xml");
        assert_eq!(doc[0].record, "document");
        assert_eq!(
            errors(&doc[0]),
            ["/Order/@version: \"3\" matches no schema version (expected \"1\" or \"2\")"]
        );
    }

    #[test]
    fn orders_for_another_customer_or_unknown_master_data_are_rejected() {
        let body =
            std::fs::read_to_string(customers_dir().join("bauwerk-ag/samples/order.json")).unwrap();
        let body = body
            .replace("\"10001001\"", "\"10001002\"")
            .replace("50001007", "50009999");
        let out = map_bytes("bauwerk-ag", body.as_bytes(), "order.json");
        assert_eq!(
            errors(&out[0]),
            [
                "/customerNo: orders for customer 10001002 are not accepted from bauwerk-ag (expects 10001001)",
                "/deliverTo/shipTo: unknown ship-to 50009999",
            ]
        );
    }

    #[test]
    fn xml_entities_resolve_but_dtd_entities_are_never_expanded() {
        let sample =
            std::fs::read_to_string(customers_dir().join("neckar-infra/samples/order-v1.xml"))
                .unwrap();
        let escaped = sample.replace(
            "<Product>C30/37-XC4</Product>",
            "<Product>C30/37 &amp; X&#33;</Product>",
        );
        let out = map_bytes("neckar-infra", escaped.as_bytes(), "x.xml");
        assert_eq!(errors(&out[0]), ["/Order/Product: unknown mix C30/37 & X!"]);

        let xxe = sample
            .replace(
                "<Order",
                "<!DOCTYPE Order [<!ENTITY xxe SYSTEM \"file:///etc/passwd\">]>\n<Order",
            )
            .replace("<ShipTo>50001028</ShipTo>", "<ShipTo>&xxe;</ShipTo>");
        let out = map_bytes("neckar-infra", xxe.as_bytes(), "x.xml");
        let errors = errors(&out[0]);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("unknown entity &xxe;"), "{errors:?}");
    }

    #[test]
    fn duplicates_share_a_key_and_changes_do_not() {
        let id = |integration: &str, body: &str| {
            let out = map_bytes(integration, body.as_bytes(), "x");
            out[0].result.as_ref().unwrap().id.clone()
        };
        let bauwerk =
            std::fs::read_to_string(customers_dir().join("bauwerk-ag/samples/order.json")).unwrap();
        let retried = bauwerk.clone();
        let updated = bauwerk
            .replace("10:15:02", "10:22:40")
            .replace("42.5", "50.0");
        assert_eq!(id("bauwerk-ag", &bauwerk), id("bauwerk-ag", &retried));
        assert_ne!(id("bauwerk-ag", &bauwerk), id("bauwerk-ag", &updated));

        let csv = std::fs::read_to_string(customers_dir().join("rheinbau-gmbh/samples/orders.csv"))
            .unwrap();
        let changed = csv.replace(
            "RB-7781;10001002;50001023;C25/30-XC2;42,5",
            "RB-7781;10001002;50001023;C25/30-XC2;50,0",
        );
        assert_eq!(
            id("rheinbau-gmbh", &csv),
            id("rheinbau-gmbh", &csv.clone()),
            "next snapshot, same row"
        );
        assert_ne!(id("rheinbau-gmbh", &csv), id("rheinbau-gmbh", &changed));
    }

    #[test]
    fn mapping_files_are_checked_when_loaded() {
        let base =
            std::fs::read_to_string(customers_dir().join("rheinbau-gmbh/mapping.yaml")).unwrap();
        assert!(Mapping::parse("rheinbau-gmbh", &base).is_ok());

        let cases = [
            (
                base.replace("  mix:             ", "  #"),
                "missing field mix",
            ),
            (base.replace("volume_m3:", "volume:"), "volume"),
            (
                base.replace("{ column: Sorte }", "{ column: Sorte, path: /x }"),
                "exactly one of column or path",
            ),
            (
                base.replace("storniert: cancelled", "storniert: gone"),
                "gone",
            ),
            (
                base.replace("{ content_hash: true }", "{ fields: [revision] }"),
                "revision is not mapped",
            ),
            (
                base.replace("Europe/Berlin", "Mars/Olympus"),
                "Mars/Olympus",
            ),
        ];
        for (yaml, expected) in cases {
            let error = Mapping::parse("rheinbau-gmbh", &yaml)
                .err()
                .unwrap_or_default();
            assert!(
                error.contains(expected),
                "expected {expected:?} in {error:?}"
            );
        }
    }

    fn order(volume: f64, gap: u32, start_minutes: i64, status: OrderStatus) -> Order {
        Order {
            id: OrderId("10001002/RB-7781".into()),
            customer: CustomerId("10001002".into()),
            site: SiteId("50001023".into()),
            mix: MixCode("C25/30-XC2".into()),
            total_volume_m3: volume,
            requested_start: received_at() + TimeDelta::minutes(start_minutes),
            pour_rate_m3_per_h: 30.0,
            max_gap_minutes: gap,
            status,
        }
    }

    proptest! {
        #[test]
        fn content_keys_are_stable_and_change_with_any_field(
            volume in 1.0f64..200.0, gap in 1u32..60, start in 0i64..100_000
        ) {
            let base = order(volume, gap, start, OrderStatus::Pending);
            let key = |o: &Order| content_key("rheinbau-gmbh", "RB-7781", o);
            prop_assert_eq!(key(&base), key(&base.clone()));
            prop_assert_ne!(key(&base), key(&order(volume + 0.5, gap, start, OrderStatus::Pending)));
            prop_assert_ne!(key(&base), key(&order(volume, gap + 1, start, OrderStatus::Pending)));
            prop_assert_ne!(key(&base), key(&order(volume, gap, start + 15, OrderStatus::Pending)));
            prop_assert_ne!(key(&base), key(&order(volume, gap, start, OrderStatus::Cancelled)));
        }
    }
}
