use serde::{Deserialize, Serialize};

/// A half-open source range. Lines and columns are zero based.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SourceRange {
    pub start: SourcePosition,
    pub end: SourcePosition,
}

/// Both byte and UTF-16 columns are included so Core and editors can use the
/// same diagnostic without guessing how a Unicode scalar was encoded.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SourcePosition {
    pub line: u32,
    pub utf8_byte_column: u32,
    pub utf16_column: u32,
    pub byte_offset: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DiagnosticSeverity {
    Error,
    Warning,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Diagnostic {
    pub code: String,
    pub severity: DiagnosticSeverity,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pointer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range: Option<SourceRange>,
}

impl Diagnostic {
    pub fn error(
        code: impl Into<String>,
        message: impl Into<String>,
        pointer: Option<String>,
        range: Option<SourceRange>,
    ) -> Self {
        Self {
            code: code.into(),
            severity: DiagnosticSeverity::Error,
            message: message.into(),
            pointer,
            range,
        }
    }
}

pub(crate) fn range_at(source: &str, byte_offset: usize, width: usize) -> SourceRange {
    let offset = byte_offset.min(source.len());
    let start_prefix = &source[..offset];
    let line = start_prefix.bytes().filter(|byte| *byte == b'\n').count() as u32;
    let line_start = start_prefix.rfind('\n').map_or(0, |index| index + 1);
    let line_prefix = &source[line_start..offset];
    let start = SourcePosition {
        line,
        utf8_byte_column: (offset - line_start) as u32,
        utf16_column: line_prefix.encode_utf16().count() as u32,
        byte_offset: offset as u32,
    };
    let end_offset = offset.saturating_add(width).min(source.len());
    let end_prefix = &source[..end_offset];
    let end_line = end_prefix.bytes().filter(|byte| *byte == b'\n').count() as u32;
    let end_line_start = end_prefix.rfind('\n').map_or(0, |index| index + 1);
    let end = SourcePosition {
        line: end_line,
        utf8_byte_column: (end_offset - end_line_start) as u32,
        utf16_column: source[end_line_start..end_offset].encode_utf16().count() as u32,
        byte_offset: end_offset as u32,
    };
    SourceRange { start, end }
}

#[cfg(test)]
mod tests {
    use super::range_at;

    #[test]
    fn reports_utf8_and_utf16_columns_independently() {
        let range = range_at("a😀éx", "a😀é".len(), 1);
        assert_eq!(range.start.utf8_byte_column, 7);
        assert_eq!(range.start.utf16_column, 4);
        assert_eq!(range.end.utf16_column, 5);
    }
}
