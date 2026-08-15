use core::fmt;

use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogbrokerDriver {
    #[schemars(title = "YDB")]
    Ydb,

    #[schemars(title = "PQv1")]
    Pqv1,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum LogbrokerAuthConfig {
    #[schemars(title = "Token")]
    Token {
        #[schemars(extend("x-ui" = { "widget": "password" }))]
        token: String,
    },

    #[schemars(title = "Token file")]
    TokenFile { token_file: String },
}

impl LogbrokerAuthConfig {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        match self {
            Self::Token { token } => anyhow::ensure!(
                !token.trim().is_empty(),
                "logbroker.auth.token must not be empty"
            ),
            Self::TokenFile { token_file } => anyhow::ensure!(
                !token_file.trim().is_empty(),
                "logbroker.auth.token_file must not be empty"
            ),
        }
        Ok(())
    }

    pub(crate) fn load_token(&self) -> anyhow::Result<String> {
        self.validate()?;
        let token = match self {
            Self::Token { token } => token.clone(),
            Self::TokenFile { token_file } => {
                let expanded = shellexpand::full(token_file).map_err(|error| {
                    anyhow::anyhow!(
                        "Failed to expand logbroker.auth.token_file '{token_file}': {error}"
                    )
                })?;
                std::fs::read_to_string(expanded.as_ref()).map_err(|error| {
                    anyhow::anyhow!("Failed to read YDB access token from '{expanded}': {error}")
                })?
            }
        };
        let token = token.trim().to_owned();
        anyhow::ensure!(!token.is_empty(), "YDB access token is empty");
        Ok(token)
    }
}

impl fmt::Debug for LogbrokerAuthConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Token { .. } => formatter
                .debug_struct("Token")
                .field("token", &"[REDACTED]")
                .finish(),
            Self::TokenFile { token_file } => formatter
                .debug_struct("TokenFile")
                .field("token_file", token_file)
                .finish(),
        }
    }
}
