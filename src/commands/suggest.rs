//! "Did you mean ...?" suggestions for mistyped service/script names.

/// Find the candidate closest to `input`, if it's close enough to be a
/// plausible typo (edit distance ≤ 1/3 of the input length, minimum 2).
pub(crate) fn closest_match<'a, I>(input: &str, candidates: I) -> Option<&'a str>
where
    I: IntoIterator<Item = &'a str>,
{
    let max_distance = (input.len() / 3).max(2);
    candidates
        .into_iter()
        .map(|c| (levenshtein(input, c), c))
        .filter(|(d, _)| *d <= max_distance)
        .min_by_key(|(d, _)| *d)
        .map(|(_, c)| c)
}

/// Append a `Did you mean '...'?` hint to `message` when a close match exists.
pub(crate) fn with_did_you_mean<'a, I>(message: &str, input: &str, candidates: I) -> String
where
    I: IntoIterator<Item = &'a str>,
{
    match closest_match(input, candidates) {
        Some(m) => format!("{message} Did you mean '{m}'?"),
        None => message.to_string(),
    }
}

/// Classic dynamic-programming Levenshtein edit distance.
fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut curr = vec![0usize; b.len() + 1];

    for (i, ca) in a.iter().enumerate() {
        curr[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let cost = if ca == cb { 0 } else { 1 };
            curr[j + 1] = (prev[j] + cost).min(prev[j + 1] + 1).min(curr[j] + 1);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_levenshtein() {
        assert_eq!(levenshtein("kitten", "sitting"), 3);
        assert_eq!(levenshtein("", "abc"), 3);
        assert_eq!(levenshtein("same", "same"), 0);
    }

    #[test]
    fn test_closest_match_finds_typo() {
        let candidates = ["backend", "frontend", "database"];
        assert_eq!(
            closest_match("backned", candidates.iter().copied()),
            Some("backend")
        );
        assert_eq!(
            closest_match("databse", candidates.iter().copied()),
            Some("database")
        );
    }

    #[test]
    fn test_closest_match_rejects_distant_names() {
        let candidates = ["backend", "frontend"];
        assert_eq!(closest_match("zzzzzzz", candidates.iter().copied()), None);
    }

    #[test]
    fn test_with_did_you_mean() {
        let candidates = ["web", "api"];
        assert_eq!(
            with_did_you_mean(
                "Service 'wbe' not found.",
                "wbe",
                candidates.iter().copied()
            ),
            "Service 'wbe' not found. Did you mean 'web'?"
        );
    }
}
