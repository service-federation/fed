use std::fs;
use std::path::Path;

#[test]
fn uploaded_action_artifacts_expire_after_one_day() {
    let workflows = Path::new(env!("CARGO_MANIFEST_DIR")).join(".github/workflows");
    let mut uploads = 0;

    for entry in fs::read_dir(&workflows).expect("read workflow directory") {
        let path = entry.expect("read workflow entry").path();
        if !matches!(
            path.extension().and_then(|extension| extension.to_str()),
            Some("yml" | "yaml")
        ) {
            continue;
        }

        let source = fs::read_to_string(&path).expect("read workflow");
        let lines: Vec<&str> = source.lines().collect();
        for (index, line) in lines.iter().enumerate() {
            if !line.contains("uses: actions/upload-artifact@") {
                continue;
            }
            uploads += 1;
            let action_indent = line.len() - line.trim_start().len();
            let block = lines[index + 1..]
                .iter()
                .take_while(|candidate| {
                    let trimmed = candidate.trim_start();
                    let indent = candidate.len() - trimmed.len();
                    trimmed.is_empty() || !trimmed.starts_with("- ") || indent > action_indent
                })
                .copied()
                .collect::<Vec<_>>()
                .join("\n");

            assert!(
                block.lines().any(|line| line.trim() == "retention-days: 1"),
                "{} contains an upload-artifact step without `retention-days: 1`:\n{}",
                path.display(),
                line.trim()
            );
        }
    }

    assert!(uploads > 0, "expected at least one upload-artifact step");
}
