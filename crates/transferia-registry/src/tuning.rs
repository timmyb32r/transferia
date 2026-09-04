use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tokio::time::{sleep_until, Instant};
use tokio_util::sync::CancellationToken;

/// Scaling used to compare explicitly declared numeric candidates.
#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NumericScale {
    #[default]
    Linear,

    Logarithmic,
}

/// A connector-owned, bounded parameter search space.
///
/// The optimizer never derives parameters from JSON Schema. Every mutable JSON
/// pointer and every value domain must be registered by the connector author.
/// `baseline` is the connector's authored production default, not the value in
/// an individual delivery. Tuning overlays these defaults only onto active,
/// declared pointers and preserves every other configuration value verbatim.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum TuningParameter {
    SignedInteger {
        pointer: String,

        label: String,

        baseline: i64,

        minimum: i64,

        maximum: i64,

        #[serde(default)]
        candidates: Vec<i64>,

        #[serde(default)]
        scale: NumericScale,
    },

    UnsignedInteger {
        pointer: String,

        label: String,

        baseline: u64,

        minimum: u64,

        maximum: u64,

        #[serde(default)]
        candidates: Vec<u64>,

        #[serde(default)]
        scale: NumericScale,
    },

    Number {
        pointer: String,

        label: String,

        baseline: f64,

        minimum: f64,

        maximum: f64,

        #[serde(default)]
        candidates: Vec<f64>,

        #[serde(default)]
        scale: NumericScale,
    },

    Choice {
        pointer: String,

        label: String,

        baseline: JsonValue,

        values: Vec<JsonValue>,
    },
}

impl TuningParameter {
    #[must_use]
    pub fn pointer(&self) -> &str {
        match self {
            Self::SignedInteger { pointer, .. }
            | Self::UnsignedInteger { pointer, .. }
            | Self::Number { pointer, .. }
            | Self::Choice { pointer, .. } => pointer,
        }
    }

    #[must_use]
    pub fn label(&self) -> &str {
        match self {
            Self::SignedInteger { label, .. }
            | Self::UnsignedInteger { label, .. }
            | Self::Number { label, .. }
            | Self::Choice { label, .. } => label,
        }
    }

    #[must_use]
    pub fn baseline(&self) -> JsonValue {
        match self {
            Self::SignedInteger { baseline, .. } => JsonValue::from(*baseline),
            Self::UnsignedInteger { baseline, .. } => JsonValue::from(*baseline),
            Self::Number { baseline, .. } => JsonValue::from(*baseline),
            Self::Choice { baseline, .. } => baseline.clone(),
        }
    }
}

/// User-visible limits for one endpoint optimization.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TuningBudget {
    pub max_trials: usize,

    /// Optional wall-clock limit shared by all trials for this endpoint.
    ///
    /// A finite `max_trials` is always required. Automatic tuning intentionally
    /// has no second, implicit deadline: each trial has an explicit duration at
    /// the service boundary, while endpoint setup and mandatory scratch cleanup
    /// must be allowed to finish safely. Time-budgeted tuning supplies this
    /// additional deadline.
    pub max_duration_ms: Option<u64>,
}

impl TuningBudget {
    pub fn validate(self) -> anyhow::Result<Self> {
        anyhow::ensure!(self.max_trials > 0, "tuning max_trials must be positive");
        if let Some(max_duration_ms) = self.max_duration_ms {
            anyhow::ensure!(
                max_duration_ms > 0,
                "tuning max_duration_ms must be positive"
            );
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EndpointTuningRequest {
    pub configuration: JsonValue,

    pub parameters: Vec<TuningParameter>,

    pub budget: TuningBudget,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TuningTrial {
    pub rows_per_second: f64,

    pub parameters: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TuningResult {
    pub baseline_rows_per_second: f64,

    pub optimized_rows_per_second: f64,

    pub gain_percent: f64,

    pub trials: usize,

    pub parameters: BTreeMap<String, JsonValue>,

    pub trial_history: Vec<TuningTrial>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TuningPairResult {
    pub source: TuningResult,

    pub destination: TuningResult,
}

/// A tuning evaluator returns this only after its per-trial cancellation has
/// stopped all work and completed mandatory scratch cleanup successfully.
#[derive(Debug, thiserror::Error)]
#[error("tuning evaluation cancelled after cleanup")]
pub struct TuningEvaluationCancelled;

#[derive(Debug, thiserror::Error)]
#[error("endpoint tuning was cancelled")]
struct EndpointTuningCancelled;

/// Validates connector-owned tuning metadata against one concrete endpoint
/// configuration. Parameters absent from this active configuration branch must
/// be filtered before calling this function.
pub fn validate_tuning_parameters(
    configuration: &JsonValue,
    parameters: &[TuningParameter],
) -> anyhow::Result<()> {
    validate_parameter_metadata_structure(parameters)?;
    for parameter in parameters {
        let current = configuration.pointer(parameter.pointer()).ok_or_else(|| {
            anyhow::anyhow!(
                "tuning parameter '{}' points to missing configuration value '{}'",
                parameter.label(),
                parameter.pointer()
            )
        })?;
        validate_runtime_parameter_slot(parameter, current)?;
    }
    Ok(())
}

fn validate_parameter_metadata_structure(parameters: &[TuningParameter]) -> anyhow::Result<()> {
    let mut pointers = BTreeSet::new();
    for parameter in parameters {
        let pointer = parameter.pointer();
        anyhow::ensure!(
            pointer.starts_with('/') && pointer.len() > 1,
            "tuning parameter '{}' must use a non-root JSON pointer",
            parameter.label()
        );
        anyhow::ensure!(
            pointers.insert(pointer),
            "tuning parameter JSON pointer '{pointer}' is registered more than once"
        );
        anyhow::ensure!(
            !parameter.label().trim().is_empty(),
            "tuning parameter '{pointer}' must have a non-empty label"
        );
        validate_parameter_domain(parameter, &parameter.baseline())?;
    }
    Ok(())
}

/// Validates registry metadata against every applicable constraint from the
/// authored JSON Schema. A pointer may be absent from the authored initial value
/// when it belongs to another valid `oneOf`/`anyOf` branch.
pub(crate) fn validate_tuning_parameters_against_schema(
    schema: &JsonValue,
    configuration: &JsonValue,
    parameters: &[TuningParameter],
) -> anyhow::Result<()> {
    validate_parameter_metadata_structure(parameters)?;
    for parameter in parameters {
        if let Some(initial) = configuration.pointer(parameter.pointer()) {
            anyhow::ensure!(
                initial == &parameter.baseline(),
                "tuning baseline for '{}' does not match the endpoint's authored default",
                parameter.pointer()
            );
        }
        let alternatives = schema_alternatives_for_pointer(schema, parameter.pointer())?;
        anyhow::ensure!(
            !alternatives.is_empty(),
            "tuning parameter '{}' points outside the endpoint JSON Schema",
            parameter.pointer()
        );
        let mut first_error = None;
        let accepted = alternatives.iter().any(|nodes| {
            match validate_parameter_against_schema_alternative(schema, parameter, nodes) {
                Ok(()) => true,
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                    false
                }
            }
        });
        if !accepted {
            return Err(first_error.unwrap_or_else(|| {
                anyhow::anyhow!(
                    "tuning parameter '{}' has no compatible endpoint JSON Schema branch",
                    parameter.pointer()
                )
            }));
        }
    }
    Ok(())
}

fn validate_runtime_parameter_slot(
    parameter: &TuningParameter,
    current: &JsonValue,
) -> anyhow::Result<()> {
    let pointer = parameter.pointer();
    let valid = match parameter {
        TuningParameter::SignedInteger { .. } => current.as_i64().is_some(),
        TuningParameter::UnsignedInteger { .. } => current.as_u64().is_some(),
        TuningParameter::Number { .. } => current.as_f64().is_some_and(f64::is_finite),
        TuningParameter::Choice { baseline, .. } => same_json_kind(current, baseline),
    };
    anyhow::ensure!(
        valid,
        "current value for tuning parameter '{pointer}' has an incompatible JSON type"
    );
    Ok(())
}

const fn same_json_kind(left: &JsonValue, right: &JsonValue) -> bool {
    matches!(
        (left, right),
        (JsonValue::Null, JsonValue::Null)
            | (JsonValue::Bool(_), JsonValue::Bool(_))
            | (JsonValue::Number(_), JsonValue::Number(_))
            | (JsonValue::String(_), JsonValue::String(_))
            | (JsonValue::Array(_), JsonValue::Array(_))
            | (JsonValue::Object(_), JsonValue::Object(_))
    )
}

/// Tunes one endpoint using a deterministic gradient-boosted regression-stump surrogate.
///
/// The callback is never called more than `max_trials` times and is
/// bounded by the optional declared wall-clock deadline. Each callback receives a
/// per-trial cancellation token. It must stop all side effects, clean up its
/// isolated resources, and return after that token is cancelled; tuning waits
/// for this cleanup before it returns from a timeout or external cancellation.
pub async fn tune_endpoint<E, Fut>(
    mut request: EndpointTuningRequest,
    cancellation: CancellationToken,
    mut evaluate: E,
) -> anyhow::Result<TuningResult>
where
    E: FnMut(JsonValue, CancellationToken) -> Fut,
    Fut: Future<Output = anyhow::Result<f64>>,
{
    let budget = request.budget.validate()?;
    request
        .parameters
        .retain(|parameter| request.configuration.pointer(parameter.pointer()).is_some());
    // Canonical order makes coverage and the surrogate independent of connector
    // registration order. The pointer is already required to be unique.
    request
        .parameters
        .sort_by(|left, right| left.pointer().cmp(right.pointer()));
    validate_tuning_parameters(&request.configuration, &request.parameters)?;
    let baseline_configuration =
        configuration_with_declared_baselines(&request.configuration, &request.parameters)?;
    let deadline = budget
        .max_duration_ms
        .map(Duration::from_millis)
        .map(|duration| {
            Instant::now()
                .checked_add(duration)
                .ok_or_else(|| anyhow::anyhow!("tuning deadline overflows the monotonic clock"))
        })
        .transpose()?;

    let baseline_parameters = parameter_values(&baseline_configuration, &request.parameters)?;
    let baseline_score = evaluate_bounded(
        &mut evaluate,
        baseline_configuration.clone(),
        deadline,
        &cancellation,
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("tuning budget expired before the baseline completed"))?;
    let mut trials = vec![EvaluatedCandidate {
        features: baseline_features(&baseline_configuration, &request.parameters)?,
        parameters: baseline_parameters,
        score: baseline_score,
    }];

    if request.parameters.is_empty() || budget.max_trials == 1 {
        return build_result(trials);
    }
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        return build_result(trials);
    }

    let candidates = candidate_pool(
        &baseline_configuration,
        &request.parameters,
        budget.max_trials,
        deadline,
        &cancellation,
    )?;
    let mut evaluated_keys = BTreeSet::from([parameter_key(&trials[0].parameters)?]);
    let mut mandatory_candidates = candidates.mandatory.iter();

    while trials.len() < budget.max_trials
        && deadline.is_none_or(|deadline| Instant::now() < deadline)
    {
        let candidate = if let Some(candidate) = mandatory_candidates.next() {
            Some(candidate)
        } else {
            let model = BoostedStumpModel::fit(&trials);
            select_candidate(&candidates.exploration, &evaluated_keys, &trials, &model)
        };
        let Some(candidate) = candidate else {
            break;
        };
        let Some(score) = evaluate_bounded(
            &mut evaluate,
            candidate.configuration.clone(),
            deadline,
            &cancellation,
        )
        .await?
        else {
            break;
        };
        evaluated_keys.insert(parameter_key(&candidate.parameters)?);
        trials.push(EvaluatedCandidate {
            features: candidate.features.clone(),
            parameters: candidate.parameters.clone(),
            score,
        });
    }

    build_result(trials)
}

fn configuration_with_declared_baselines(
    configuration: &JsonValue,
    parameters: &[TuningParameter],
) -> anyhow::Result<JsonValue> {
    let mut baseline = configuration.clone();
    for parameter in parameters {
        let slot = baseline.pointer_mut(parameter.pointer()).ok_or_else(|| {
            anyhow::anyhow!(
                "tuning parameter '{}' disappeared from baseline configuration",
                parameter.pointer()
            )
        })?;
        *slot = parameter.baseline();
    }
    Ok(baseline)
}

/// Runs source and destination searches concurrently under independent budgets.
pub async fn tune_source_and_sink<SE, SFut, DE, DFut>(
    source: EndpointTuningRequest,
    destination: EndpointTuningRequest,
    cancellation: CancellationToken,
    source_evaluate: SE,
    destination_evaluate: DE,
) -> anyhow::Result<TuningPairResult>
where
    SE: FnMut(JsonValue, CancellationToken) -> SFut,
    SFut: Future<Output = anyhow::Result<f64>>,
    DE: FnMut(JsonValue, CancellationToken) -> DFut,
    DFut: Future<Output = anyhow::Result<f64>>,
{
    enum First<T> {
        Source(T),
        Destination(T),
    }

    let source_cancellation = cancellation.child_token();
    let destination_cancellation = cancellation.child_token();
    let source_tuning = tune_endpoint(source, source_cancellation.clone(), source_evaluate);
    let destination_tuning = tune_endpoint(
        destination,
        destination_cancellation.clone(),
        destination_evaluate,
    );
    tokio::pin!(source_tuning);
    tokio::pin!(destination_tuning);

    let first = tokio::select! {
        source = &mut source_tuning => First::Source(source),
        destination = &mut destination_tuning => First::Destination(destination),
    };
    match first {
        First::Source(Ok(source)) => Ok(TuningPairResult {
            source,
            destination: destination_tuning.await?,
        }),
        First::Destination(Ok(destination)) => Ok(TuningPairResult {
            source: source_tuning.await?,
            destination,
        }),
        First::Source(Err(primary)) => {
            destination_cancellation.cancel();
            sibling_shutdown_result(primary, destination_tuning.await)
        }
        First::Destination(Err(primary)) => {
            source_cancellation.cancel();
            sibling_shutdown_result(primary, source_tuning.await)
        }
    }
}

fn sibling_shutdown_result<T>(
    primary: anyhow::Error,
    sibling: anyhow::Result<T>,
) -> anyhow::Result<TuningPairResult> {
    match sibling {
        Ok(_) => Err(primary),
        Err(error) if error.is::<EndpointTuningCancelled>() => Err(primary),
        Err(error) => Err(anyhow::anyhow!(
            "{primary:#}; sibling endpoint failed while shutting down: {error:#}"
        )),
    }
}

async fn evaluate_bounded<E, Fut>(
    evaluate: &mut E,
    configuration: JsonValue,
    deadline: Option<Instant>,
    cancellation: &CancellationToken,
) -> anyhow::Result<Option<f64>>
where
    E: FnMut(JsonValue, CancellationToken) -> Fut,
    Fut: Future<Output = anyhow::Result<f64>>,
{
    if cancellation.is_cancelled() {
        return Err(EndpointTuningCancelled.into());
    }
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        return Ok(None);
    }
    let trial_cancellation = cancellation.child_token();
    let evaluation = evaluate(configuration, trial_cancellation.clone());
    tokio::pin!(evaluation);
    let result = if let Some(deadline) = deadline {
        tokio::select! {
            biased;

            () = cancellation.cancelled() => {
                trial_cancellation.cancel();
                if let Err(error) = evaluation.await {
                    if !error.is::<TuningEvaluationCancelled>() {
                        return Err(error);
                    }
                }
                return Err(EndpointTuningCancelled.into());
            }
            () = sleep_until(deadline) => {
                trial_cancellation.cancel();
                if let Err(error) = evaluation.await {
                    if !error.is::<TuningEvaluationCancelled>() {
                        return Err(error);
                    }
                }
                return Ok(None);
            }
            result = &mut evaluation => result,
        }
    } else {
        tokio::select! {
            biased;

            () = cancellation.cancelled() => {
                trial_cancellation.cancel();
                if let Err(error) = evaluation.await {
                    if !error.is::<TuningEvaluationCancelled>() {
                        return Err(error);
                    }
                }
                return Err(EndpointTuningCancelled.into());
            }
            result = &mut evaluation => result,
        }
    };
    let score = result?;
    anyhow::ensure!(
        score.is_finite() && score >= 0.0,
        "tuning evaluator returned an invalid rows-per-second value"
    );
    Ok(Some(score))
}

fn validate_parameter_domain(
    parameter: &TuningParameter,
    baseline: &JsonValue,
) -> anyhow::Result<()> {
    match parameter {
        TuningParameter::SignedInteger {
            pointer,
            minimum,
            maximum,
            candidates,
            scale,
            ..
        } => {
            anyhow::ensure!(minimum <= maximum, "invalid range for '{pointer}'");
            anyhow::ensure!(
                !candidates.is_empty(),
                "numeric tuning parameter '{pointer}' must declare finite candidates"
            );
            if *scale == NumericScale::Logarithmic {
                anyhow::ensure!(
                    *minimum > 0,
                    "logarithmic range '{pointer}' must be positive"
                );
            }
            let baseline = baseline.as_i64().ok_or_else(|| {
                anyhow::anyhow!("tuning parameter '{pointer}' baseline must be a signed integer")
            })?;
            validate_numeric_values(pointer, baseline, *minimum, *maximum, candidates)
        }
        TuningParameter::UnsignedInteger {
            pointer,
            minimum,
            maximum,
            candidates,
            scale,
            ..
        } => {
            anyhow::ensure!(minimum <= maximum, "invalid range for '{pointer}'");
            anyhow::ensure!(
                !candidates.is_empty(),
                "numeric tuning parameter '{pointer}' must declare finite candidates"
            );
            if *scale == NumericScale::Logarithmic {
                anyhow::ensure!(
                    *minimum > 0,
                    "logarithmic range '{pointer}' must be positive"
                );
            }
            let baseline = baseline.as_u64().ok_or_else(|| {
                anyhow::anyhow!("tuning parameter '{pointer}' baseline must be an unsigned integer")
            })?;
            validate_numeric_values(pointer, baseline, *minimum, *maximum, candidates)
        }
        TuningParameter::Number {
            pointer,
            minimum,
            maximum,
            candidates,
            scale,
            ..
        } => {
            anyhow::ensure!(
                minimum.is_finite() && maximum.is_finite() && minimum <= maximum,
                "invalid finite range for '{pointer}'"
            );
            anyhow::ensure!(
                !candidates.is_empty(),
                "numeric tuning parameter '{pointer}' must declare finite candidates"
            );
            if *scale == NumericScale::Logarithmic {
                anyhow::ensure!(
                    *minimum > 0.0,
                    "logarithmic range '{pointer}' must be positive"
                );
            }
            let baseline = baseline.as_f64().ok_or_else(|| {
                anyhow::anyhow!("tuning parameter '{pointer}' baseline must be a number")
            })?;
            anyhow::ensure!(
                baseline.is_finite(),
                "baseline for '{pointer}' must be finite"
            );
            anyhow::ensure!(
                (*minimum..=*maximum).contains(&baseline),
                "baseline for '{pointer}' is outside its tuning range"
            );
            let mut unique = Vec::with_capacity(candidates.len());
            for candidate in candidates {
                anyhow::ensure!(
                    candidate.is_finite() && (*minimum..=*maximum).contains(candidate),
                    "candidate for '{pointer}' is outside its finite tuning range"
                );
                anyhow::ensure!(
                    !unique.contains(candidate),
                    "tuning parameter '{pointer}' has duplicate candidates"
                );
                unique.push(*candidate);
            }
            anyhow::ensure!(
                candidates.contains(&baseline),
                "explicit candidates for '{pointer}' must include its baseline"
            );
            Ok(())
        }
        TuningParameter::Choice {
            pointer, values, ..
        } => {
            anyhow::ensure!(
                !values.is_empty(),
                "choice tuning parameter '{pointer}' must declare values"
            );
            anyhow::ensure!(
                values.contains(baseline),
                "baseline for '{pointer}' is not an allowed choice"
            );
            for (index, value) in values.iter().enumerate() {
                anyhow::ensure!(
                    !values[index + 1..].contains(value),
                    "choice tuning parameter '{pointer}' has duplicate values"
                );
            }
            Ok(())
        }
    }
}

fn schema_alternatives_for_pointer<'a>(
    root: &'a JsonValue,
    pointer: &str,
) -> anyhow::Result<Vec<Vec<&'a JsonValue>>> {
    let tokens = decode_pointer(pointer)?;
    walk_schema_pointer(root, root, &tokens)
}

fn walk_schema_pointer<'a>(
    root: &'a JsonValue,
    schema: &'a JsonValue,
    tokens: &[String],
) -> anyhow::Result<Vec<Vec<&'a JsonValue>>> {
    if let Some(reference) = schema.get("$ref").and_then(JsonValue::as_str) {
        let pointer = reference.strip_prefix('#').ok_or_else(|| {
            anyhow::anyhow!("external JSON Schema reference is not supported in tuning metadata")
        })?;
        let resolved = root.pointer(pointer).ok_or_else(|| {
            anyhow::anyhow!("unresolved JSON Schema reference '{reference}' in tuning metadata")
        })?;
        return walk_schema_pointer(root, resolved, tokens);
    }
    if let Some(all_of) = schema.get("allOf").and_then(JsonValue::as_array) {
        let mut combined = vec![Vec::new()];
        for branch in all_of {
            let branch_alternatives = walk_schema_pointer(root, branch, tokens)?;
            if branch_alternatives.is_empty() {
                continue;
            }
            let mut next = Vec::new();
            for existing in &combined {
                for alternative in &branch_alternatives {
                    let mut constraints = existing.clone();
                    constraints.extend(alternative);
                    next.push(constraints);
                }
            }
            combined = next;
        }
        return Ok(combined);
    }
    for keyword in ["oneOf", "anyOf"] {
        if let Some(branches) = schema.get(keyword).and_then(JsonValue::as_array) {
            let mut alternatives = Vec::new();
            for branch in branches {
                alternatives.extend(walk_schema_pointer(root, branch, tokens)?);
            }
            return Ok(alternatives);
        }
    }
    if tokens.is_empty() {
        return Ok(vec![vec![schema]]);
    }
    let token = &tokens[0];
    if let Some(property) = schema
        .get("properties")
        .and_then(JsonValue::as_object)
        .and_then(|properties| properties.get(token))
    {
        return walk_schema_pointer(root, property, &tokens[1..]);
    }
    if let Some(items) = schema.get("items") {
        if token.parse::<usize>().is_ok() {
            return walk_schema_pointer(root, items, &tokens[1..]);
        }
    }
    Ok(Vec::new())
}

fn validate_parameter_against_schema_alternative(
    root: &JsonValue,
    parameter: &TuningParameter,
    nodes: &[&JsonValue],
) -> anyhow::Result<()> {
    for node in nodes {
        validate_parameter_against_schema(parameter, node)?;
    }
    let mut combined = serde_json::Map::new();
    for keyword in ["$schema", "$defs", "definitions"] {
        if let Some(value) = root.get(keyword) {
            combined.insert(keyword.to_owned(), value.clone());
        }
    }
    combined.insert(
        "allOf".to_owned(),
        JsonValue::Array(nodes.iter().map(|node| (*node).clone()).collect()),
    );
    let schema = JsonValue::Object(combined);
    let validator = jsonschema::options().build(&schema).map_err(|error| {
        anyhow::anyhow!(
            "failed to compile endpoint JSON Schema for tuning parameter '{}': {error}",
            parameter.pointer()
        )
    })?;
    for value in schema_domain_samples(parameter) {
        validator.validate(&value).map_err(|error| {
            anyhow::anyhow!(
                "tuning candidate for '{}' conflicts with endpoint JSON Schema: {error}",
                parameter.pointer()
            )
        })?;
    }
    Ok(())
}

fn schema_domain_samples(parameter: &TuningParameter) -> Vec<JsonValue> {
    match parameter {
        TuningParameter::SignedInteger { candidates, .. } => numeric_domain_samples(candidates),
        TuningParameter::UnsignedInteger { candidates, .. } => numeric_domain_samples(candidates),
        TuningParameter::Number { candidates, .. } => numeric_domain_samples(candidates),
        TuningParameter::Choice { values, .. } => values.clone(),
    }
}

fn numeric_domain_samples<T>(candidates: &[T]) -> Vec<JsonValue>
where
    T: Copy + Into<JsonValue>,
{
    candidates.iter().copied().map(Into::into).collect()
}

fn validate_parameter_against_schema(
    parameter: &TuningParameter,
    schema: &JsonValue,
) -> anyhow::Result<()> {
    let pointer = parameter.pointer();
    match parameter {
        TuningParameter::SignedInteger {
            minimum,
            maximum,
            candidates,
            ..
        } => {
            ensure_schema_type(schema, pointer, &["integer"])?;
            validate_schema_numeric_range(schema, pointer, *minimum as f64, *maximum as f64)?;
            for candidate in candidates {
                ensure_schema_accepts_value(schema, pointer, &JsonValue::from(*candidate))?;
            }
        }
        TuningParameter::UnsignedInteger {
            minimum,
            maximum,
            candidates,
            ..
        } => {
            ensure_schema_type(schema, pointer, &["integer"])?;
            validate_schema_numeric_range(schema, pointer, *minimum as f64, *maximum as f64)?;
            for candidate in candidates {
                ensure_schema_accepts_value(schema, pointer, &JsonValue::from(*candidate))?;
            }
        }
        TuningParameter::Number {
            minimum,
            maximum,
            candidates,
            ..
        } => {
            ensure_schema_type(schema, pointer, &["integer", "number"])?;
            validate_schema_numeric_range(schema, pointer, *minimum, *maximum)?;
            for candidate in candidates {
                ensure_schema_accepts_value(schema, pointer, &JsonValue::from(*candidate))?;
            }
        }
        TuningParameter::Choice { values, .. } => {
            for value in values {
                ensure_schema_accepts_value(schema, pointer, value)?;
            }
        }
    }
    Ok(())
}

fn ensure_schema_type(schema: &JsonValue, pointer: &str, allowed: &[&str]) -> anyhow::Result<()> {
    if let Some(schema_type) = schema.get("type").and_then(JsonValue::as_str) {
        anyhow::ensure!(
            allowed.contains(&schema_type),
            "tuning parameter '{pointer}' type conflicts with endpoint JSON Schema type '{schema_type}'"
        );
    }
    Ok(())
}

fn validate_schema_numeric_range(
    schema: &JsonValue,
    pointer: &str,
    minimum: f64,
    maximum: f64,
) -> anyhow::Result<()> {
    if let Some(schema_minimum) = schema.get("minimum").and_then(JsonValue::as_f64) {
        anyhow::ensure!(
            minimum >= schema_minimum,
            "tuning range for '{pointer}' extends below endpoint JSON Schema minimum"
        );
    }
    if let Some(schema_maximum) = schema.get("maximum").and_then(JsonValue::as_f64) {
        anyhow::ensure!(
            maximum <= schema_maximum,
            "tuning range for '{pointer}' extends above endpoint JSON Schema maximum"
        );
    }
    if let Some(schema_minimum) = schema.get("exclusiveMinimum").and_then(JsonValue::as_f64) {
        anyhow::ensure!(
            minimum > schema_minimum,
            "tuning range for '{pointer}' includes endpoint JSON Schema exclusive minimum"
        );
    }
    if let Some(schema_maximum) = schema.get("exclusiveMaximum").and_then(JsonValue::as_f64) {
        anyhow::ensure!(
            maximum < schema_maximum,
            "tuning range for '{pointer}' includes endpoint JSON Schema exclusive maximum"
        );
    }
    Ok(())
}

fn ensure_schema_accepts_value(
    schema: &JsonValue,
    pointer: &str,
    value: &JsonValue,
) -> anyhow::Result<()> {
    if let Some(constant) = schema.get("const") {
        anyhow::ensure!(
            constant == value,
            "tuning candidate for '{pointer}' conflicts with endpoint JSON Schema const"
        );
    }
    if let Some(values) = schema.get("enum").and_then(JsonValue::as_array) {
        anyhow::ensure!(
            values.contains(value),
            "tuning candidate for '{pointer}' is absent from endpoint JSON Schema enum"
        );
    }
    if let Some(schema_type) = schema.get("type").and_then(JsonValue::as_str) {
        let valid = match schema_type {
            "null" => value.is_null(),
            "boolean" => value.is_boolean(),
            "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
            "number" => value.is_number(),
            "string" => value.is_string(),
            "array" => value.is_array(),
            "object" => value.is_object(),
            _ => true,
        };
        anyhow::ensure!(
            valid,
            "tuning candidate for '{pointer}' conflicts with endpoint JSON Schema type"
        );
    }
    if value.is_number() {
        let value = value.as_f64().ok_or_else(|| {
            anyhow::anyhow!(
                "tuning candidate for '{pointer}' cannot be represented as a finite f64"
            )
        })?;
        validate_schema_numeric_range(schema, pointer, value, value)?;
    }
    Ok(())
}

fn decode_pointer(pointer: &str) -> anyhow::Result<Vec<String>> {
    anyhow::ensure!(pointer.starts_with('/'), "invalid JSON pointer '{pointer}'");
    pointer[1..]
        .split('/')
        .map(|token| {
            let mut decoded = String::with_capacity(token.len());
            let mut chars = token.chars();
            while let Some(character) = chars.next() {
                if character != '~' {
                    decoded.push(character);
                    continue;
                }
                match chars.next() {
                    Some('0') => decoded.push('~'),
                    Some('1') => decoded.push('/'),
                    _ => anyhow::bail!("invalid JSON pointer escape in '{pointer}'"),
                }
            }
            Ok(decoded)
        })
        .collect()
}

fn validate_numeric_values<T>(
    pointer: &str,
    baseline: T,
    minimum: T,
    maximum: T,
    candidates: &[T],
) -> anyhow::Result<()>
where
    T: Copy + PartialOrd + PartialEq,
{
    anyhow::ensure!(
        baseline >= minimum && baseline <= maximum,
        "baseline for '{pointer}' is outside its tuning range"
    );
    anyhow::ensure!(
        candidates.contains(&baseline),
        "explicit candidates for '{pointer}' must include its baseline"
    );
    for (index, candidate) in candidates.iter().enumerate() {
        anyhow::ensure!(
            *candidate >= minimum && *candidate <= maximum,
            "candidate for '{pointer}' is outside its tuning range"
        );
        anyhow::ensure!(
            !candidates[index + 1..].contains(candidate),
            "tuning parameter '{pointer}' has duplicate candidates"
        );
    }
    Ok(())
}

#[derive(Clone)]
struct Candidate {
    features: Vec<f64>,
    configuration: JsonValue,
    parameters: BTreeMap<String, JsonValue>,
}

struct EvaluatedCandidate {
    features: Vec<f64>,
    parameters: BTreeMap<String, JsonValue>,
    score: f64,
}

struct CandidatePool {
    /// One far-from-baseline probe per parameter, followed by one probe per
    /// parameter pair. These are evaluated before model-guided acquisition.
    mandatory: Vec<Candidate>,

    /// Remaining connector-declared combinations available to the surrogate.
    exploration: Vec<Candidate>,
}

fn candidate_pool(
    baseline: &JsonValue,
    parameters: &[TuningParameter],
    max_trials: usize,
    deadline: Option<Instant>,
    cancellation: &CancellationToken,
) -> anyhow::Result<CandidatePool> {
    let domains = parameters.iter().map(parameter_domain).collect::<Vec<_>>();
    let product_size = domains
        .iter()
        .try_fold(1_usize, |product, domain| product.checked_mul(domain.len()))
        .unwrap_or(usize::MAX);
    let target_size = product_size.saturating_sub(1).min(
        max_trials
            .saturating_mul(parameters.len().saturating_mul(8).max(32))
            .max(max_trials),
    );
    let mandatory_limit = max_trials.saturating_sub(1);
    let mut mandatory = Vec::new();
    let mut exploration = Vec::new();
    let mut keys = BTreeSet::new();

    let baseline_values = parameters
        .iter()
        .map(TuningParameter::baseline)
        .collect::<Vec<_>>();
    keys.insert(parameter_key(
        &parameters
            .iter()
            .zip(&baseline_values)
            .map(|(parameter, value)| (parameter.pointer().to_owned(), value.clone()))
            .collect(),
    )?);
    let alternatives = domains
        .iter()
        .zip(&baseline_values)
        .map(|(domain, baseline)| {
            domain
                .iter()
                .filter(|value| *value != baseline)
                .cloned()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let representatives = parameters
        .iter()
        .zip(&alternatives)
        .map(|(parameter, values)| representative_value(parameter, values))
        .collect::<anyhow::Result<Vec<_>>>()?;

    // With P+1 trials every active parameter is probed once, irrespective of
    // registration order. The representative is deliberately far from the
    // baseline so the first probe has useful signal.
    for (parameter_index, representative) in representatives.iter().enumerate() {
        if mandatory.len() >= mandatory_limit
            || !candidate_enumeration_allowed(deadline, cancellation)?
        {
            return Ok(CandidatePool {
                mandatory,
                exploration,
            });
        }
        let Some(representative) = representative else {
            continue;
        };
        let mut values = baseline_values.clone();
        values[parameter_index] = representative.clone();
        push_candidate(&mut mandatory, &mut keys, baseline, parameters, &values)?;
    }

    // With 1+P+P*(P-1)/2 trials every pair interaction receives a direct
    // probe before gradient-boosted acquisition. This prevents additive early
    // observations from permanently hiding a late, interaction-only optimum.
    for left in 0..parameters.len() {
        for right in left + 1..parameters.len() {
            if mandatory.len() >= mandatory_limit
                || !candidate_enumeration_allowed(deadline, cancellation)?
            {
                return Ok(CandidatePool {
                    mandatory,
                    exploration,
                });
            }
            let (Some(left_value), Some(right_value)) =
                (&representatives[left], &representatives[right])
            else {
                continue;
            };
            let mut values = baseline_values.clone();
            values[left] = left_value.clone();
            values[right] = right_value.clone();
            push_candidate(&mut mandatory, &mut keys, baseline, parameters, &values)?;
        }
    }

    // Add all remaining one-dimensional probes in rounds, so no parameter's
    // full domain can crowd later parameters out of the model's candidate set.
    let maximum_alternatives = alternatives.iter().map(Vec::len).max().unwrap_or(0);
    for rank in 0..maximum_alternatives {
        for (parameter_index, values) in alternatives.iter().enumerate() {
            if mandatory.len() + exploration.len() >= target_size
                || !candidate_enumeration_allowed(deadline, cancellation)?
            {
                return Ok(CandidatePool {
                    mandatory,
                    exploration,
                });
            }
            let Some(value) = values.get(rank) else {
                continue;
            };
            let mut candidate_values = baseline_values.clone();
            candidate_values[parameter_index] = value.clone();
            push_candidate(
                &mut exploration,
                &mut keys,
                baseline,
                parameters,
                &candidate_values,
            )?;
        }
    }

    // Likewise, offer all declared pair combinations in rounds before filling
    // the rest of the pool. Acquisition may then distinguish nonlinear pair
    // effects rather than seeing only a prefix of the Cartesian product.
    let pairs = (0..parameters.len())
        .flat_map(|left| (left + 1..parameters.len()).map(move |right| (left, right)))
        .collect::<Vec<_>>();
    let maximum_pair_combinations = pairs
        .iter()
        .map(|(left, right)| {
            alternatives[*left]
                .len()
                .saturating_mul(alternatives[*right].len())
        })
        .max()
        .unwrap_or(0);
    for combination in 0..maximum_pair_combinations {
        for (left, right) in &pairs {
            if mandatory.len() + exploration.len() >= target_size
                || !candidate_enumeration_allowed(deadline, cancellation)?
            {
                return Ok(CandidatePool {
                    mandatory,
                    exploration,
                });
            }
            let left_values = &alternatives[*left];
            let right_values = &alternatives[*right];
            if left_values.is_empty() || right_values.is_empty() {
                continue;
            }
            let pair_size = left_values.len().saturating_mul(right_values.len());
            if combination >= pair_size {
                continue;
            }
            let mut candidate_values = baseline_values.clone();
            candidate_values[*left] = left_values[combination % left_values.len()].clone();
            candidate_values[*right] = right_values[combination / left_values.len()].clone();
            push_candidate(
                &mut exploration,
                &mut keys,
                baseline,
                parameters,
                &candidate_values,
            )?;
        }
    }

    let mut indexes = vec![0_usize; domains.len()];
    loop {
        if mandatory.len() + exploration.len() >= target_size {
            break;
        }
        if !candidate_enumeration_allowed(deadline, cancellation)? {
            break;
        }
        let values = domains
            .iter()
            .zip(&indexes)
            .map(|(domain, index)| domain[*index].clone())
            .collect::<Vec<_>>();
        push_candidate(&mut exploration, &mut keys, baseline, parameters, &values)?;
        if !advance_cartesian_indexes(&mut indexes, &domains) {
            break;
        }
    }
    Ok(CandidatePool {
        mandatory,
        exploration,
    })
}

fn representative_value(
    parameter: &TuningParameter,
    alternatives: &[JsonValue],
) -> anyhow::Result<Option<JsonValue>> {
    let baseline_fraction = value_fraction(parameter, &parameter.baseline())?;
    alternatives
        .iter()
        .map(|value| {
            Ok((
                (value_fraction(parameter, value)? - baseline_fraction).abs(),
                value,
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()
        .map(|values| {
            values
                .into_iter()
                .max_by(|(left, _), (right, _)| left.total_cmp(right))
                .map(|(_, value)| value.clone())
        })
}

fn parameter_domain(parameter: &TuningParameter) -> Vec<JsonValue> {
    match parameter {
        TuningParameter::SignedInteger { candidates, .. } => {
            candidates.iter().copied().map(JsonValue::from).collect()
        }
        TuningParameter::UnsignedInteger { candidates, .. } => {
            candidates.iter().copied().map(JsonValue::from).collect()
        }
        TuningParameter::Number { candidates, .. } => {
            candidates.iter().copied().map(JsonValue::from).collect()
        }
        TuningParameter::Choice { values, .. } => values.clone(),
    }
}

fn candidate_enumeration_allowed(
    deadline: Option<Instant>,
    cancellation: &CancellationToken,
) -> anyhow::Result<bool> {
    if cancellation.is_cancelled() {
        return Err(EndpointTuningCancelled.into());
    }
    Ok(deadline.is_none_or(|deadline| Instant::now() < deadline))
}

fn advance_cartesian_indexes(indexes: &mut [usize], domains: &[Vec<JsonValue>]) -> bool {
    for (index, domain) in indexes.iter_mut().zip(domains).rev() {
        *index += 1;
        if *index < domain.len() {
            return true;
        }
        *index = 0;
    }
    false
}

fn push_candidate(
    candidates: &mut Vec<Candidate>,
    keys: &mut BTreeSet<String>,
    baseline: &JsonValue,
    parameters: &[TuningParameter],
    candidate_values: &[JsonValue],
) -> anyhow::Result<()> {
    let mut configuration = baseline.clone();
    let mut values = BTreeMap::new();
    for (parameter, value) in parameters.iter().zip(candidate_values) {
        let slot = configuration
            .pointer_mut(parameter.pointer())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "tuning parameter '{}' disappeared from candidate configuration",
                    parameter.pointer()
                )
            })?;
        *slot = value.clone();
        values.insert(parameter.pointer().to_owned(), value.clone());
    }
    let key = parameter_key(&values)?;
    if keys.insert(key) {
        candidates.push(Candidate {
            features: baseline_features(&configuration, parameters)?,
            configuration,
            parameters: values,
        });
    }
    Ok(())
}

fn baseline_features(
    configuration: &JsonValue,
    parameters: &[TuningParameter],
) -> anyhow::Result<Vec<f64>> {
    parameters
        .iter()
        .map(|parameter| {
            let value = configuration.pointer(parameter.pointer()).ok_or_else(|| {
                anyhow::anyhow!(
                    "tuning parameter '{}' is missing from configuration",
                    parameter.pointer()
                )
            })?;
            value_fraction(parameter, value)
        })
        .collect()
}

fn value_fraction(parameter: &TuningParameter, value: &JsonValue) -> anyhow::Result<f64> {
    let fraction = match parameter {
        TuningParameter::SignedInteger {
            minimum,
            maximum,
            scale,
            ..
        } => {
            let value = value
                .as_i64()
                .ok_or_else(|| anyhow::anyhow!("expected signed integer"))?;
            numeric_fraction(value as f64, *minimum as f64, *maximum as f64, *scale)
        }
        TuningParameter::UnsignedInteger {
            minimum,
            maximum,
            scale,
            ..
        } => {
            let value = value
                .as_u64()
                .ok_or_else(|| anyhow::anyhow!("expected unsigned integer"))?;
            numeric_fraction(value as f64, *minimum as f64, *maximum as f64, *scale)
        }
        TuningParameter::Number {
            minimum,
            maximum,
            scale,
            ..
        } => {
            let value = value
                .as_f64()
                .ok_or_else(|| anyhow::anyhow!("expected number"))?;
            numeric_fraction(value, *minimum, *maximum, *scale)
        }
        TuningParameter::Choice { values, .. } => {
            discrete_fraction(values.iter().position(|item| item == value), values.len())?
        }
    };
    Ok(fraction.clamp(0.0, 1.0))
}

fn discrete_fraction(index: Option<usize>, len: usize) -> anyhow::Result<f64> {
    let index = index.ok_or_else(|| anyhow::anyhow!("baseline is not in declared candidates"))?;
    Ok(if len == 1 {
        0.0
    } else {
        index as f64 / (len - 1) as f64
    })
}

fn numeric_fraction(value: f64, minimum: f64, maximum: f64, scale: NumericScale) -> f64 {
    if maximum.total_cmp(&minimum).is_eq() {
        return 0.0;
    }
    match scale {
        NumericScale::Linear => (value - minimum) / (maximum - minimum),
        NumericScale::Logarithmic => (value.ln() - minimum.ln()) / (maximum.ln() - minimum.ln()),
    }
}

fn parameter_values(
    configuration: &JsonValue,
    parameters: &[TuningParameter],
) -> anyhow::Result<BTreeMap<String, JsonValue>> {
    parameters
        .iter()
        .map(|parameter| {
            configuration
                .pointer(parameter.pointer())
                .cloned()
                .map(|value| (parameter.pointer().to_owned(), value))
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "tuning parameter '{}' is missing from configuration",
                        parameter.pointer()
                    )
                })
        })
        .collect()
}

fn parameter_key(parameters: &BTreeMap<String, JsonValue>) -> anyhow::Result<String> {
    serde_json::to_string(parameters).map_err(Into::into)
}

fn select_candidate<'a>(
    candidates: &'a [Candidate],
    evaluated_keys: &BTreeSet<String>,
    trials: &[EvaluatedCandidate],
    model: &BoostedStumpModel,
) -> Option<&'a Candidate> {
    let score_scale = score_standard_deviation(trials).max(1.0);
    candidates
        .iter()
        .filter(|candidate| {
            parameter_key(&candidate.parameters).is_ok_and(|key| !evaluated_keys.contains(&key))
        })
        .max_by(|left, right| {
            acquisition(left, trials, model, score_scale).total_cmp(&acquisition(
                right,
                trials,
                model,
                score_scale,
            ))
        })
}

fn acquisition(
    candidate: &Candidate,
    trials: &[EvaluatedCandidate],
    model: &BoostedStumpModel,
    score_scale: f64,
) -> f64 {
    let nearest_distance = trials
        .iter()
        .map(|trial| squared_distance(&candidate.features, &trial.features))
        .fold(f64::INFINITY, f64::min)
        .sqrt();
    score_scale.mul_add(nearest_distance, model.predict(&candidate.features))
}

fn squared_distance(left: &[f64], right: &[f64]) -> f64 {
    left.iter()
        .zip(right)
        .map(|(left, right)| (left - right).powi(2))
        .sum()
}

fn score_standard_deviation(trials: &[EvaluatedCandidate]) -> f64 {
    let mean = trials.iter().map(|trial| trial.score).sum::<f64>() / trials.len() as f64;
    (trials
        .iter()
        .map(|trial| (trial.score - mean).powi(2))
        .sum::<f64>()
        / trials.len() as f64)
        .sqrt()
}

fn build_result(trials: Vec<EvaluatedCandidate>) -> anyhow::Result<TuningResult> {
    let baseline = trials
        .first()
        .ok_or_else(|| anyhow::anyhow!("tuning produced no baseline trial"))?
        .score;
    let best = trials
        .iter()
        .max_by(|left, right| left.score.total_cmp(&right.score))
        .ok_or_else(|| anyhow::anyhow!("tuning produced no candidate result"))?;
    let gain_percent = if baseline > 0.0 {
        (best.score - baseline) * 100.0 / baseline
    } else {
        0.0
    };
    Ok(TuningResult {
        baseline_rows_per_second: baseline,
        optimized_rows_per_second: best.score,
        gain_percent,
        trials: trials.len(),
        parameters: best.parameters.clone(),
        trial_history: trials
            .into_iter()
            .map(|trial| TuningTrial {
                rows_per_second: trial.score,
                parameters: trial.parameters,
            })
            .collect(),
    })
}

#[derive(Default)]
struct BoostedStumpModel {
    base: f64,
    stumps: Vec<RegressionStump>,
}

impl BoostedStumpModel {
    fn fit(trials: &[EvaluatedCandidate]) -> Self {
        if trials.is_empty() {
            return Self::default();
        }
        let base = trials.iter().map(|trial| trial.score).sum::<f64>() / trials.len() as f64;
        let mut predictions = vec![base; trials.len()];
        let mut stumps = Vec::new();
        for _ in 0..trials.len().min(24) {
            let residuals = trials
                .iter()
                .zip(&predictions)
                .map(|(trial, prediction)| trial.score - prediction)
                .collect::<Vec<_>>();
            let Some(mut stump) = RegressionStump::fit(trials, &residuals) else {
                break;
            };
            stump.left *= 0.2;
            stump.right *= 0.2;
            for (prediction, trial) in predictions.iter_mut().zip(trials) {
                *prediction += stump.predict(&trial.features);
            }
            stumps.push(stump);
        }
        Self { base, stumps }
    }

    fn predict(&self, features: &[f64]) -> f64 {
        self.base
            + self
                .stumps
                .iter()
                .map(|stump| stump.predict(features))
                .sum::<f64>()
    }
}

struct RegressionStump {
    feature: usize,
    threshold: f64,
    left: f64,
    right: f64,
}

impl RegressionStump {
    fn fit(trials: &[EvaluatedCandidate], residuals: &[f64]) -> Option<Self> {
        let feature_count = trials.first()?.features.len();
        let mut best: Option<(f64, Self)> = None;
        for feature in 0..feature_count {
            let mut values = trials
                .iter()
                .map(|trial| trial.features[feature])
                .collect::<Vec<_>>();
            values.sort_by(f64::total_cmp);
            values.dedup();
            for pair in values.windows(2) {
                let threshold = f64::midpoint(pair[0], pair[1]);
                let (left, right) = partition_means(trials, residuals, feature, threshold)?;
                let error = trials
                    .iter()
                    .zip(residuals)
                    .map(|(trial, residual)| {
                        let prediction = if trial.features[feature] <= threshold {
                            left
                        } else {
                            right
                        };
                        (residual - prediction).powi(2)
                    })
                    .sum();
                if best
                    .as_ref()
                    .is_none_or(|(best_error, _)| error < *best_error)
                {
                    best = Some((
                        error,
                        Self {
                            feature,
                            threshold,
                            left,
                            right,
                        },
                    ));
                }
            }
        }
        best.map(|(_, stump)| stump)
    }

    fn predict(&self, features: &[f64]) -> f64 {
        if features[self.feature] <= self.threshold {
            self.left
        } else {
            self.right
        }
    }
}

fn partition_means(
    trials: &[EvaluatedCandidate],
    residuals: &[f64],
    feature: usize,
    threshold: f64,
) -> Option<(f64, f64)> {
    let mut left_sum = 0.0;
    let mut left_count = 0_usize;
    let mut right_sum = 0.0;
    let mut right_count = 0_usize;
    for (trial, residual) in trials.iter().zip(residuals) {
        if trial.features[feature] <= threshold {
            left_sum += residual;
            left_count += 1;
        } else {
            right_sum += residual;
            right_count += 1;
        }
    }
    (left_count > 0 && right_count > 0)
        .then_some((left_sum / left_count as f64, right_sum / right_count as f64))
}
