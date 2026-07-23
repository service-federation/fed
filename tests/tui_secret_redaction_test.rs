//! TUI secret-exposure boundary tests.
//!
//! Sensitivity derives from declared `type: secret` plus transitive
//! provenance — never only from name heuristics. These tests build a real
//! orchestrator (so secrets resolve through the actual pipeline), then prove
//! that sentinel secret values never reach the rendered parameter screen or a
//! clipboard payload, while ordinary parameters keep working.

use crossterm::event::{KeyCode, KeyEvent};
use fed::tui::app::{App, CopyDecision, View};
use fed::{OutputMode, RunContext};
use ratatui::{Terminal, backend::TestBackend};

/// Sentinel that must never appear in rendered output or clipboard payloads.
const SECRET_SENTINEL: &str = "sentinel-license-value-93af";

async fn build_app() -> (App, tempfile::TempDir) {
    // LICENSE: a declared secret whose name contains no secret-like substring.
    // DERIVED: depends on the secret through its default template.
    // KEY_COUNT: ordinary non-secret parameter with a KEY token in its name.
    let yaml = format!(
        r#"
parameters:
  LICENSE:
    type: secret
    generate: "printf {SECRET_SENTINEL}"
  DERIVED:
    default: "prefix-{{{{LICENSE}}}}-suffix"
  KEY_COUNT:
    default: "7"
"#
    );

    let parser = fed::Parser::new();
    let config = parser.parse_config(&yaml).expect("parse");
    let temp = tempfile::tempdir().unwrap();

    let ctx = RunContext {
        offline: true,
        is_interactive: false,
        output_mode: OutputMode::Captured,
        profiles: vec![],
        required_secret_names: None,
    };

    let orchestrator = fed::Orchestrator::builder()
        .config(config)
        .work_dir(temp.path().to_path_buf())
        .run_context(ctx)
        .auto_resolve_conflicts(true)
        .build()
        .await
        .expect("build");

    let mut app = App::new(orchestrator);
    app.view = View::Parameters;
    // Return the tempdir so it outlives the orchestrator's state DB.
    (app, temp)
}

fn render_to_string(app: &App) -> String {
    let backend = TestBackend::new(100, 30);
    let mut terminal = Terminal::new(backend).unwrap();
    terminal.draw(|f| fed::tui::ui::draw(f, app)).unwrap();
    let buffer = terminal.backend().buffer().clone();
    buffer.content().iter().map(|cell| cell.symbol()).collect()
}

fn select_param(app: &mut App, name: &str) {
    let idx = app
        .get_filtered_params()
        .iter()
        .position(|v| v.name == name)
        .unwrap_or_else(|| panic!("parameter '{name}' not in filtered list"));
    app.params_selected = idx;
}

#[tokio::test]
async fn secret_resolves_through_real_pipeline_but_views_are_redacted() {
    let (app, _temp) = build_app().await;

    let params = app.get_filtered_params();
    let get = |name: &str| {
        params
            .iter()
            .find(|v| v.name == name)
            .unwrap_or_else(|| panic!("missing parameter '{name}'"))
    };

    // The secret actually resolved (the pipeline ran the generate command) —
    // sensitivity, not absence, is what redacts it.
    assert!(get("LICENSE").value.is_sensitive());
    assert!(
        get("DERIVED").value.is_sensitive(),
        "value derived from a secret must remain sensitive"
    );
    assert!(!get("KEY_COUNT").value.is_sensitive());

    // The view model retains no raw secret material at all.
    let debug_dump = format!("{:?}", app.parameters_cache);
    assert!(
        !debug_dump.contains(SECRET_SENTINEL),
        "raw secret must not be retained in the TUI view model"
    );
}

#[tokio::test]
async fn rendered_parameter_screen_masks_sensitive_rows_only() {
    let (app, _temp) = build_app().await;
    let rendered = render_to_string(&app);

    assert!(
        !rendered.contains(SECRET_SENTINEL),
        "sentinel secret leaked into rendered output"
    );
    assert!(
        !rendered.contains("prefix-"),
        "derived-from-secret value leaked into rendered output"
    );
    assert!(rendered.contains("LICENSE"));
    assert!(rendered.contains("DERIVED"));
    assert!(rendered.contains("********"), "mask missing from rendering");
    // Ordinary parameter renders its real value.
    assert!(rendered.contains("KEY_COUNT"));
    assert!(rendered.contains("7"));
}

#[tokio::test]
async fn copy_is_disabled_for_sensitive_rows_and_works_for_plain_rows() {
    let (mut app, _temp) = build_app().await;

    // Sensitive row: both copy forms are blocked, no payload exists.
    for name in ["LICENSE", "DERIVED"] {
        select_param(&mut app, name);
        for include_key in [false, true] {
            match app.selected_param_copy_payload(include_key) {
                CopyDecision::Sensitive { name: n } => assert_eq!(n, name),
                other => panic!("expected Sensitive for '{name}', got {other:?}"),
            }
        }
    }

    // Pressing 'y' on a sensitive row surfaces a visible refusal.
    select_param(&mut app, "LICENSE");
    app.handle_key(KeyEvent::from(KeyCode::Char('y')))
        .await
        .unwrap();
    let status = app.status_message.as_ref().expect("status message");
    assert!(
        status.text.contains("sensitive"),
        "refusal must be explicit, got: {}",
        status.text
    );
    assert!(!status.text.contains(SECRET_SENTINEL));

    // The refusal is actually rendered on the parameters screen (status bar),
    // and the post-keypress frame still contains no secret material.
    let rendered = render_to_string(&app);
    assert!(
        rendered.contains("copying is disabled"),
        "refusal must be visible in the rendered frame"
    );
    assert!(!rendered.contains(SECRET_SENTINEL));

    // Plain row: copy produces exactly the raw payload.
    select_param(&mut app, "KEY_COUNT");
    match app.selected_param_copy_payload(false) {
        CopyDecision::Copy { name, payload } => {
            assert_eq!(name, "KEY_COUNT");
            assert_eq!(payload, "7");
        }
        other => panic!("expected Copy, got {other:?}"),
    }
    match app.selected_param_copy_payload(true) {
        CopyDecision::Copy { payload, .. } => assert_eq!(payload, "KEY_COUNT=7"),
        other => panic!("expected Copy, got {other:?}"),
    }
    app.handle_key(KeyEvent::from(KeyCode::Char('y')))
        .await
        .unwrap();
    let status = app.status_message.as_ref().expect("status message");
    assert!(status.text.contains("Copied"), "got: {}", status.text);
}

#[tokio::test]
async fn filter_cannot_probe_secret_content() {
    let (mut app, _temp) = build_app().await;

    // Filtering by the secret's content must not match the redacted row —
    // the view model has no raw value to match against.
    app.params_filter = "sentinel-license".to_string();
    assert!(
        app.get_filtered_params().is_empty(),
        "value filter matched a redacted row — secret content is probeable"
    );

    // Filtering by name still finds the row.
    app.params_filter = "license".to_string();
    let names: Vec<_> = app
        .get_filtered_params()
        .iter()
        .map(|v| v.name.clone())
        .collect();
    assert_eq!(names, vec!["LICENSE".to_string()]);
}
