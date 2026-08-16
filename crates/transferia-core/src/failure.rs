#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureDisposition {
    Retryable,
    Fatal,
}

#[derive(Debug)]
pub struct DataPlaneFailure {
    disposition: FailureDisposition,
    source: anyhow::Error,
}

impl DataPlaneFailure {
    #[must_use]
    pub const fn retryable(source: anyhow::Error) -> Self {
        Self {
            disposition: FailureDisposition::Retryable,
            source,
        }
    }

    #[must_use]
    pub const fn fatal(source: anyhow::Error) -> Self {
        Self {
            disposition: FailureDisposition::Fatal,
            source,
        }
    }

    #[must_use]
    pub fn retryable_or_passthrough(source: anyhow::Error) -> Self {
        Self::with_fallback(source, FailureDisposition::Retryable)
    }

    #[must_use]
    pub fn fatal_or_passthrough(source: anyhow::Error) -> Self {
        Self::with_fallback(source, FailureDisposition::Fatal)
    }

    #[must_use]
    pub const fn disposition(&self) -> FailureDisposition {
        self.disposition
    }

    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self.disposition, FailureDisposition::Retryable)
    }

    #[must_use]
    pub fn into_source(self) -> anyhow::Error {
        self.source
    }

    #[must_use]
    pub fn context(self, context: impl std::fmt::Display + Send + Sync + 'static) -> Self {
        Self {
            disposition: self.disposition,
            source: self.source.context(context),
        }
    }

    fn with_fallback(source: anyhow::Error, fallback: FailureDisposition) -> Self {
        match source.downcast::<Self>() {
            Ok(failure) => failure,
            Err(source) => Self {
                disposition: fallback,
                source,
            },
        }
    }
}

impl std::fmt::Display for DataPlaneFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(formatter)
    }
}

impl std::error::Error for DataPlaneFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

pub type DataPlaneResult<T> = Result<T, DataPlaneFailure>;

#[cfg(test)]
#[path = "tests/failure.rs"]
mod tests;
