//! `fed variant` — read and write the persisted variant selection.
//!
//! The selection lives in `.fed/variants.yaml` so that every later command
//! (`fed restart catalog`, `fed status`, the TUI, the background supervisor)
//! resolves the same implementation without the user repeating `--variant`.
//! See [`fed::config::variants`] for the precedence rules these commands
//! feed into.

use crate::cli::VariantCommands;
use crate::commands::ports::resolve_work_dir;
use crate::output::UserOutput;
use fed::Parser as ConfigParser;
use fed::config::variants::{PersistedVariants, VariantSelection, variants_file_path};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub async fn run_variant(
    cmd: &VariantCommands,
    workdir: Option<PathBuf>,
    config_path: Option<PathBuf>,
    cli_variants: &[String],
    offline: bool,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    let work_dir = resolve_work_dir(workdir, config_path.as_deref())?;

    match cmd {
        VariantCommands::List { json } => {
            list(&work_dir, config_path, cli_variants, offline, *json, out).await
        }
        VariantCommands::Set { entries } => {
            let config = load_config(&work_dir, config_path, offline).await?;
            set(&work_dir, entries, config, out)
        }
        VariantCommands::Unset { services } => unset(&work_dir, services, out),
        VariantCommands::Clear => clear(&work_dir, out),
    }
}

/// `fed variant set go,ts,rust catalog:java`
///
/// Preference-list names replace the stored list outright (a preference list
/// is an ordering, and merging two orderings has no sensible meaning), while
/// pins are merged into the stored map so adding one doesn't drop the others.
fn set(
    work_dir: &Path,
    entries: &[String],
    mut config: fed::Config,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    let (prefer, pin) = fed::config::variants::split_entries(entries)?;
    let mut persisted = PersistedVariants::load(work_dir)?;

    if !prefer.is_empty() {
        persisted.prefer = prefer;
    }
    for (service, variant) in pin {
        persisted.pin.insert(service, variant);
    }

    fed::config::variants::resolve_variants(
        &mut config,
        &VariantSelection::from_persisted(&persisted),
        &[],
    )?;
    persisted.save(work_dir)?;
    report(&persisted, work_dir, out);
    Ok(())
}

fn unset(work_dir: &Path, services: &[String], out: &dyn UserOutput) -> anyhow::Result<()> {
    let mut persisted = PersistedVariants::load(work_dir)?;
    for service in services {
        if persisted.pin.remove(service).is_none() {
            out.status(&format!("No variant pinned for '{service}'."));
        }
    }
    persisted.save(work_dir)?;
    report(&persisted, work_dir, out);
    Ok(())
}

fn clear(work_dir: &Path, out: &dyn UserOutput) -> anyhow::Result<()> {
    PersistedVariants::default().save(work_dir)?;
    out.success(&format!(
        "Cleared {}",
        variants_file_path(work_dir).display()
    ));
    Ok(())
}

fn report(persisted: &PersistedVariants, work_dir: &Path, out: &dyn UserOutput) {
    if persisted.prefer.is_empty() && persisted.pin.is_empty() {
        out.success(&format!(
            "Cleared {}",
            variants_file_path(work_dir).display()
        ));
        return;
    }
    out.success(&format!("Wrote {}", variants_file_path(work_dir).display()));
    if !persisted.prefer.is_empty() {
        out.status(&format!("  prefer: {}", persisted.prefer.join(", ")));
    }
    let pins: BTreeMap<_, _> = persisted.pin.iter().collect();
    for (service, variant) in pins {
        out.status(&format!("  pin: {service} -> {variant}"));
    }
}

#[derive(serde::Serialize)]
struct VariantListEntry {
    variant: String,
    source: String,
    available: Vec<String>,
}

/// `fed variant list` — the resolved variant for every service that has
/// any, and which precedence layer decided it.
///
/// Loads the config itself (rather than taking an orchestrator's, which is
/// already variant-resolved and so no longer knows the alternatives) and
/// applies the same [`VariantSelection`] a real start would.
async fn list(
    work_dir: &Path,
    config_path: Option<PathBuf>,
    cli_variants: &[String],
    offline: bool,
    json: bool,
    out: &dyn UserOutput,
) -> anyhow::Result<()> {
    let config = load_config(work_dir, config_path, offline).await?;

    let selection = VariantSelection::load(cli_variants, work_dir)?;
    fed::config::variants::resolve_variants(&mut config.clone(), &selection, &[])?;

    let mut rows: Vec<(String, VariantListEntry)> = Vec::new();
    for (name, service) in &config.services {
        if service.variants.is_empty() {
            continue;
        }
        let (chosen, source) = selection.pick_with_source(name, service)?;
        let mut available: Vec<String> = service.variants.keys().cloned().collect();
        available.sort();
        rows.push((
            name.clone(),
            VariantListEntry {
                variant: chosen.to_string(),
                source: source.to_string(),
                available,
            },
        ));
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));

    if json {
        let obj: BTreeMap<_, _> = rows.into_iter().collect();
        out.status(&serde_json::to_string_pretty(&obj)?);
        return Ok(());
    }

    if rows.is_empty() {
        out.status("No service in this config declares `variants:`.");
        return Ok(());
    }

    out.status("Service variants:");
    for (name, entry) in &rows {
        out.status(&format!(
            "  {:<24} {:<10} ({}; available: {})",
            name,
            entry.variant,
            entry.source,
            entry.available.join(", ")
        ));
    }
    Ok(())
}

async fn load_config(
    work_dir: &Path,
    config_path: Option<PathBuf>,
    offline: bool,
) -> anyhow::Result<fed::Config> {
    let parser = ConfigParser::new();
    let path = match config_path {
        Some(p) => p,
        None => {
            ConfigParser::find_config_in_dir(work_dir).or_else(|_| parser.find_config_file())?
        }
    };
    let config = parser
        .load_config_with_packages_offline(&path, offline)
        .await?;
    config.validate()?;

    Ok(config)
}
