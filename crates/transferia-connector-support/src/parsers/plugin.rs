use std::collections::BTreeMap;
use std::sync::Arc;

use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;

use super::detection::{ParserDetection, ParserDetector};
use super::{CommonParserConfig, ParserPlan};

const BUILTIN_PARSERS: &[&str] = &[
    "json_parser",
    "schema_registry",
    "debezium",
    "raw_to_table",
    "benchmark_discard",
];

#[derive(Clone, Copy)]
pub struct ParserPluginSpec {
    pub kind: &'static str,

    pub title: &'static str,

    pub connectors: &'static [&'static str],
}

trait ErasedParserPlugin: Send + Sync {
    fn build(
        &self,
        common: &CommonParserConfig,
        raw: &serde_yaml::Value,
        source_name: &str,
    ) -> anyhow::Result<ParserPlan>;
}

struct TypedParserPlugin<C, F> {
    build: F,
    marker: std::marker::PhantomData<fn() -> C>,
}

impl<C, F> ErasedParserPlugin for TypedParserPlugin<C, F>
where
    C: DeserializeOwned + Send + Sync + 'static,
    F: Fn(&CommonParserConfig, C, &str) -> anyhow::Result<ParserPlan> + Send + Sync + 'static,
{
    fn build(
        &self,
        common: &CommonParserConfig,
        raw: &serde_yaml::Value,
        source_name: &str,
    ) -> anyhow::Result<ParserPlan> {
        (self.build)(common, serde_yaml::from_value(raw.clone())?, source_name)
    }
}

#[derive(Clone)]
struct ParserPluginRegistration {
    title: &'static str,
    connectors: &'static [&'static str],
    schema: JsonValue,

    detector: Option<Arc<dyn ParserDetector>>,

    plugin: Arc<dyn ErasedParserPlugin>,
}

#[derive(Clone, Default)]
pub struct ParserPluginRegistry {
    plugins: BTreeMap<&'static str, ParserPluginRegistration>,
}

impl ParserPluginRegistry {
    pub fn register<C, F>(&mut self, spec: ParserPluginSpec, build: F) -> anyhow::Result<()>
    where
        C: DeserializeOwned + JsonSchema + Send + Sync + 'static,
        F: Fn(&CommonParserConfig, C, &str) -> anyhow::Result<ParserPlan> + Send + Sync + 'static,
    {
        validate_spec(&spec)?;
        let schema = parser_variant_schema::<C>(&spec)?;
        let registration = ParserPluginRegistration {
            title: spec.title,
            connectors: spec.connectors,
            schema,
            detector: None,
            plugin: Arc::new(TypedParserPlugin::<C, F> {
                build,
                marker: std::marker::PhantomData,
            }),
        };
        anyhow::ensure!(
            self.plugins.insert(spec.kind, registration).is_none(),
            "parser plugin '{}' is registered more than once",
            spec.kind
        );
        Ok(())
    }

    pub fn register_detector<D>(&mut self, kind: &str, detector: D) -> anyhow::Result<()>
    where
        D: ParserDetector + 'static,
    {
        let registration = self
            .plugins
            .get_mut(kind)
            .ok_or_else(|| anyhow::anyhow!("parser detector '{kind}' has no registered parser"))?;
        anyhow::ensure!(
            registration.detector.is_none(),
            "parser detector '{kind}' is registered more than once"
        );
        registration.detector = Some(Arc::new(detector));
        Ok(())
    }

    #[must_use]
    pub fn detect_samples(&self, payloads: &[&[u8]], max_rows: usize) -> Vec<ParserDetection> {
        let mut detections = super::detection::detect_samples(payloads, max_rows);
        detections.extend(self.plugins.iter().filter_map(|(kind, registration)| {
            let detector = registration.detector.as_ref()?;
            let detection = detector.try_parse_samples(payloads, max_rows).ok()??;
            (detection.key == *kind).then_some(detection)
        }));
        detections
    }

    pub fn build(
        &self,
        kind: &str,
        common: &CommonParserConfig,
        raw: &serde_yaml::Value,
        source_name: &str,
    ) -> anyhow::Result<Option<ParserPlan>> {
        self.plugins
            .get(kind)
            .map(|registration| registration.plugin.build(common, raw, source_name))
            .transpose()
    }

    pub fn variants_for<'a>(
        &'a self,
        connector: &'a str,
    ) -> impl Iterator<Item = (&'a str, &'a JsonValue)> + 'a {
        self.plugins.iter().filter_map(move |(kind, registration)| {
            registration
                .connectors
                .contains(&connector)
                .then_some((*kind, &registration.schema))
        })
    }

    pub fn kinds(&self) -> impl Iterator<Item = &str> {
        self.plugins.keys().copied()
    }

    pub fn registrations(&self) -> impl Iterator<Item = (&str, &str, &[&str], &JsonValue)> {
        self.plugins.iter().map(|(kind, registration)| {
            (
                *kind,
                registration.title,
                registration.connectors,
                &registration.schema,
            )
        })
    }
}

fn validate_spec(spec: &ParserPluginSpec) -> anyhow::Result<()> {
    anyhow::ensure!(
        !spec.kind.is_empty()
            && spec
                .kind
                .bytes()
                .all(|byte| { byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' }),
        "parser plugin kind must be non-empty snake_case"
    );
    anyhow::ensure!(
        !BUILTIN_PARSERS.contains(&spec.kind),
        "parser plugin '{}' conflicts with a built-in parser",
        spec.kind
    );
    anyhow::ensure!(
        !spec.title.trim().is_empty(),
        "parser plugin title must not be empty"
    );
    anyhow::ensure!(
        !spec.connectors.is_empty(),
        "parser plugin '{}' must target at least one connector",
        spec.kind
    );
    let mut connectors = std::collections::BTreeSet::new();
    for connector in spec.connectors {
        anyhow::ensure!(
            !connector.is_empty(),
            "parser plugin '{}' contains an empty connector key",
            spec.kind
        );
        anyhow::ensure!(
            connectors.insert(*connector),
            "parser plugin '{}' repeats connector '{}'",
            spec.kind,
            connector
        );
    }
    Ok(())
}

fn parser_variant_schema<C: JsonSchema>(spec: &ParserPluginSpec) -> anyhow::Result<JsonValue> {
    let generator = schemars::generate::SchemaSettings::draft2020_12()
        .with(|settings| settings.inline_subschemas = true)
        .into_generator();
    let mut config = serde_json::to_value(generator.into_root_schema_for::<C>())?;
    remove_schema_meta(&mut config);
    let generator = schemars::generate::SchemaSettings::draft2020_12()
        .with(|settings| settings.inline_subschemas = true)
        .into_generator();
    let mut common = serde_json::to_value(generator.into_root_schema_for::<CommonParserConfig>())?;
    remove_schema_meta(&mut common);
    common
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("common parser schema must be an object"))?
        .extend([
            (
                "title".to_owned(),
                JsonValue::String("Parser settings".to_owned()),
            ),
            (
                "x-ui".to_owned(),
                serde_json::json!({ "widget": "parser_common" }),
            ),
        ]);
    Ok(serde_json::json!({
        "title": spec.title,
        "x-ui": {
            "capabilities": {
                "component": "parser",
                "key": spec.kind,
                "record_semantics": ["append_only"]
            }
        },
        "type": "object",
        "properties": {
            "common": common,
            spec.kind: config,
        },
        "required": ["common", spec.kind],
        "additionalProperties": false,
    }))
}

fn remove_schema_meta(schema: &mut JsonValue) {
    if let Some(object) = schema.as_object_mut() {
        object.remove("$schema");
        object.remove("title");
    }
}
