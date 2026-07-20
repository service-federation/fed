//! Duration parsing utilities.
//!
//! This module provides parsing for human-readable duration strings
//! like "5s", "30s", "1m", "500ms".

use std::time::Duration;

/// Parse a duration string like "10s", "30s", "1m", "500ms".
///
/// Supported formats:
/// - `"Nms"` - N milliseconds (e.g., "500ms")
/// - `"Ns"` - N seconds (e.g., "30s")
/// - `"Nm"` - N minutes (e.g., "5m")
/// - `"Nh"` - N hours (e.g., "24h")
/// - `"N"` - N seconds (no suffix, assumes seconds)
///
/// Returns `None` if the string cannot be parsed, including when the value
/// would overflow (e.g. an hour count larger than `u64::MAX` seconds).
///
/// # Examples
///
/// ```
/// use fed::config::parse_duration_string;
/// use std::time::Duration;
///
/// assert_eq!(parse_duration_string("5s"), Some(Duration::from_secs(5)));
/// assert_eq!(parse_duration_string("500ms"), Some(Duration::from_millis(500)));
/// assert_eq!(parse_duration_string("1m"), Some(Duration::from_secs(60)));
/// assert_eq!(parse_duration_string("24h"), Some(Duration::from_secs(86_400)));
/// assert_eq!(parse_duration_string("30"), Some(Duration::from_secs(30)));
/// ```
pub fn parse_duration_string(s: &str) -> Option<Duration> {
    let s = s.trim();

    if s.is_empty() {
        return None;
    }

    if s.ends_with("ms") {
        s.trim_end_matches("ms")
            .parse::<u64>()
            .ok()
            .map(Duration::from_millis)
    } else if s.ends_with('s') {
        s.trim_end_matches('s')
            .parse::<u64>()
            .ok()
            .map(Duration::from_secs)
    } else if s.ends_with('m') {
        // checked_mul, not `*`: a minute count above u64::MAX/60 still parses as
        // a u64, and the unchecked multiply panicked in debug builds on config
        // input. Overflow is a parse failure, so the caller's default applies.
        s.trim_end_matches('m')
            .parse::<u64>()
            .ok()
            .and_then(|m| m.checked_mul(60))
            .map(Duration::from_secs)
    } else if s.ends_with('h') {
        s.trim_end_matches('h')
            .parse::<u64>()
            .ok()
            .and_then(|h| h.checked_mul(60 * 60))
            .map(Duration::from_secs)
    } else {
        // Default to seconds if no suffix
        s.parse::<u64>().ok().map(Duration::from_secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration_seconds() {
        assert_eq!(parse_duration_string("5s"), Some(Duration::from_secs(5)));
        assert_eq!(parse_duration_string("30s"), Some(Duration::from_secs(30)));
        assert_eq!(
            parse_duration_string("120s"),
            Some(Duration::from_secs(120))
        );
    }

    #[test]
    fn test_parse_duration_minutes() {
        assert_eq!(parse_duration_string("1m"), Some(Duration::from_secs(60)));
        assert_eq!(parse_duration_string("5m"), Some(Duration::from_secs(300)));
    }

    #[test]
    fn test_parse_duration_hours() {
        assert_eq!(parse_duration_string("1h"), Some(Duration::from_secs(3600)));
        assert_eq!(
            parse_duration_string("24h"),
            Some(Duration::from_secs(86_400))
        );
    }

    #[test]
    fn test_parse_duration_rejects_overflow_instead_of_panicking() {
        // These parse as u64 but overflow when scaled to seconds. The caller's
        // default should apply rather than the process aborting on config input.
        assert_eq!(parse_duration_string(&format!("{}m", u64::MAX)), None);
        assert_eq!(parse_duration_string(&format!("{}h", u64::MAX)), None);
    }

    #[test]
    fn test_parse_duration_milliseconds() {
        assert_eq!(
            parse_duration_string("500ms"),
            Some(Duration::from_millis(500))
        );
        assert_eq!(
            parse_duration_string("100ms"),
            Some(Duration::from_millis(100))
        );
    }

    #[test]
    fn test_parse_duration_no_suffix() {
        // Defaults to seconds when no suffix
        assert_eq!(parse_duration_string("10"), Some(Duration::from_secs(10)));
        assert_eq!(parse_duration_string("60"), Some(Duration::from_secs(60)));
    }

    #[test]
    fn test_parse_duration_with_whitespace() {
        assert_eq!(
            parse_duration_string("  5s  "),
            Some(Duration::from_secs(5))
        );
        assert_eq!(
            parse_duration_string(" 100ms "),
            Some(Duration::from_millis(100))
        );
    }

    #[test]
    fn test_parse_duration_invalid() {
        assert_eq!(parse_duration_string(""), None);
        assert_eq!(parse_duration_string("abc"), None);
        assert_eq!(parse_duration_string("5x"), None);
        assert_eq!(parse_duration_string("-5s"), None);
    }

    #[test]
    fn test_parse_duration_zero() {
        assert_eq!(parse_duration_string("0s"), Some(Duration::from_secs(0)));
        assert_eq!(parse_duration_string("0ms"), Some(Duration::from_millis(0)));
    }
}
