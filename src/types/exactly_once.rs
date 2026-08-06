/// Одна колонка составного ключа уникальности.
#[derive(Debug, Clone)]
pub struct ExactlyOnceColumn {
    pub name: std::sync::Arc<str>,
}

/// Составной ключ уникальности. Колонки физически лежат в RecordBatch;
/// дескриптор называет их роли.
#[derive(Debug, Clone)]
pub struct ExactlyOnceKey {
    /// Колонка-«пространство офсетов»:
    ///   YDS: Int64 (partition id)
    ///   S3:  Utf8  (full S3 object key)
    pub partition: ExactlyOnceColumn,
    /// Монотонный офсет внутри партиции: Int64.
    pub offset: ExactlyOnceColumn,
}

/// Значение ключа партиции — ключ HashMap'а waterline.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PartitionKey {
    Int(i64),
    /// S3: полный объектный ключ (не базовое имя файла).
    Str(String),
}

impl PartitionKey {
    /// SQL-литерал для подстановки в `WHERE partition = {val}`.
    ///
    /// `Int` → число; `Str` → `unhex('<hex-encoded-bytes>')`.
    /// Hex-кодирование исключает ручное экранирование:
    ///   - CH-литералы применяют C-style unescape → backslash теряется;
    ///   - clickhouse-arrow 0.2.1 экранирует только `'`, не `\`.
    ///   - unhex('...') делегирует корректность `hex::encode`, а не ручному escape.
    pub fn to_sql_literal(&self) -> String {
        match self {
            PartitionKey::Int(v) => v.to_string(),
            PartitionKey::Str(v) => format!("unhex('{}')", hex::encode(v.as_bytes())),
        }
    }
}
