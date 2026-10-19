//! The output a service left behind, for an error that says it stopped.

/// How many lines an error shows of a service that stopped.
pub(crate) const FAILURE_OUTPUT_LINES: usize = 10;

/// The last lines of `lines` under a heading, or `None` when there are none.
///
/// The cause of a crash is usually among the last lines a service wrote, so
/// this keeps the end of the output, not the start.
pub(crate) fn failure_output_preview(lines: &[String]) -> Option<String> {
    if lines.is_empty() {
        return None;
    }
    let tail = &lines[lines.len().saturating_sub(FAILURE_OUTPUT_LINES)..];
    Some(format!("Last {} lines:\n{}", tail.len(), tail.join("\n")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn numbered(count: usize) -> Vec<String> {
        (1..=count).map(|n| format!("line {n}")).collect()
    }

    #[test]
    fn no_output_has_no_preview() {
        assert_eq!(failure_output_preview(&[]), None);
    }

    #[test]
    fn short_output_is_shown_whole() {
        assert_eq!(
            failure_output_preview(&numbered(3)).as_deref(),
            Some("Last 3 lines:\nline 1\nline 2\nline 3"),
        );
    }

    #[test]
    fn long_output_keeps_the_last_lines_where_the_error_is() {
        let preview = failure_output_preview(&numbered(20)).unwrap();
        assert!(preview.starts_with("Last 10 lines:\nline 11\n"));
        assert!(preview.ends_with("\nline 20"));
        assert!(!preview.contains("line 10\n"));
    }
}
