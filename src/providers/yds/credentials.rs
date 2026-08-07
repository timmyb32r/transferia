pub enum YdbCredentials {
    Anonymous(ydb::AnonymousCredentials),
    AccessToken(ydb::AccessTokenCredentials),
    ServiceAccount(ydb::ServiceAccountCredentials),
}

impl ydb::Credentials for YdbCredentials {
    fn create_token(&self) -> ydb::YdbResult<ydb::TokenInfo> {
        match *self {
            Self::Anonymous(ref c) => c.create_token(),
            Self::AccessToken(ref c) => c.create_token(),
            Self::ServiceAccount(ref c) => c.create_token(),
        }
    }

    fn debug_string(&self) -> String {
        match self {
            Self::Anonymous(_) => "anonymous".to_string(),
            Self::AccessToken(_) => "access_token".to_string(),
            Self::ServiceAccount(_) => "service_account".to_string(),
        }
    }
}

/// Build credentials AND extract raw token string for `PQv1` auth.
pub fn build_credentials_with_token(
    auth: &crate::config::yaml::AuthConfig,
) -> anyhow::Result<(YdbCredentials, Option<String>)> {
    match auth.auth_type.as_str() {
        "" | "anonymous" => Ok((YdbCredentials::Anonymous(ydb::AnonymousCredentials::new()), None)),
        "access_token" => {
            let token = read_token(auth)?;
            Ok((YdbCredentials::AccessToken(ydb::AccessTokenCredentials::from(token.clone())), Some(token)))
        }
        "service_account" => {
            let path = auth.sa_file.as_deref()
                .ok_or_else(|| anyhow::anyhow!("service_account auth requires 'sa_file' field"))?;
            let creds = ydb::ServiceAccountCredentials::from_file(path)
                .map_err(|e| anyhow::anyhow!("Failed to load service account key from '{path}': {e}"))?;
            Ok((YdbCredentials::ServiceAccount(creds), None))
        }
        other => anyhow::bail!("Unsupported auth type '{other}'"),
    }
}

pub fn build_credentials(auth: &crate::config::yaml::AuthConfig) -> anyhow::Result<YdbCredentials> {
    match auth.auth_type.as_str() {
        "" | "anonymous" => Ok(YdbCredentials::Anonymous(ydb::AnonymousCredentials::new())),
        "access_token" => {
            let token = read_token(auth)?;
            Ok(YdbCredentials::AccessToken(ydb::AccessTokenCredentials::from(token)))
        }
        "service_account" => {
            let path = auth.sa_file.as_deref()
                .ok_or_else(|| anyhow::anyhow!("service_account auth requires 'sa_file' field"))?;
            let creds = ydb::ServiceAccountCredentials::from_file(path)
                .map_err(|e| anyhow::anyhow!("Failed to load service account key from '{path}': {e}"))?;
            Ok(YdbCredentials::ServiceAccount(creds))
        }
        other => anyhow::bail!("Unsupported auth type '{other}'"),
    }
}

fn read_token(auth: &crate::config::yaml::AuthConfig) -> anyhow::Result<String> {
    if let Some(path) = auth.token_file.as_deref() {
        let expanded = shellexpand::full(path)
            .map_err(|e| anyhow::anyhow!("Failed to expand token_file path '{path}': {e}"))?;
        Ok(std::fs::read_to_string(expanded.as_ref())
            .map_err(|e| anyhow::anyhow!("Failed to read token from '{expanded}': {e}"))?
            .trim()
            .to_string())
    } else if let Some(tok) = auth.token.as_deref() {
        Ok(tok.to_string())
    } else {
        anyhow::bail!("access_token auth requires either 'token' or 'token_file' field")
    }
}
