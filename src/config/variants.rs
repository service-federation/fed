//! Variant selection and resolution.
//!
//! A service with `variants:` names one contract and several interchangeable
//! implementations of it. Exactly one is chosen here, merged over the outer
//! service, and put back into `config.services` under the same name — so
//! every later stage (profile filtering, the dependency graph, the service
//! factory, health checks, status) sees an ordinary service and needs to know
//! nothing about variants.
//!
//! Variant names are a vocabulary shared *across* services (`java`, `go`,
//! `rust`, `ts`), so the main control is an ordered preference list rather
//! than a per-service flag: `fed start --variant go,ts,rust` means "prefer Go,
//! then TypeScript, then Rust, for whichever services offer them". A pin
//! (`--variant catalog:java`) handles the odd one out.
//!
//! Precedence, highest first:
//!
//! 1. CLI pin — `--variant catalog:java`
//! 2. CLI preference list — `--variant go,ts,rust`
//! 3. Persisted selection — `.fed/variants.yaml`, written by `fed variant set`
//! 4. The service's own `default_variant`
//!
//! The persisted layer exists so a later `fed restart catalog`, `fed status`,
//! the TUI and the background supervisor all agree on what is running without
//! the user repeating the flag on every invocation.

use super::{Config, Service};
use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Persisted selection file, relative to the work dir.
pub const VARIANTS_FILE_REL: &str = ".fed/variants.yaml";

/// Absolute path of the persisted selection file for a work dir.
pub fn variants_file_path(work_dir: &Path) -> PathBuf {
    crate::fed_dir::fed_dir(work_dir).join("variants.yaml")
}

/// On-disk shape of `.fed/variants.yaml`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PersistedVariants {
    /// Ordered preference list, most-preferred first.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub prefer: Vec<String>,
    /// Per-service pins, which beat the preference list.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub pin: HashMap<String, String>,
}

impl PersistedVariants {
    /// Read the file, or return an empty selection when it does not exist.
    pub fn load(work_dir: &Path) -> Result<Self> {
        let path = variants_file_path(work_dir);
        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(e) => {
                return Err(Error::Filesystem(format!(
                    "reading {}: {}",
                    path.display(),
                    e
                )));
            }
        };
        serde_yaml::from_str(&content).map_err(|e| {
            Error::Parse(format!(
                "Failed to parse {}: {}. Fix it by hand or run `fed variant clear`.",
                path.display(),
                e
            ))
        })
    }

    /// Write the file, creating `.fed/` if needed. An empty selection removes
    /// the file rather than leaving an empty document behind.
    pub fn save(&self, work_dir: &Path) -> Result<()> {
        let path = variants_file_path(work_dir);
        if self.prefer.is_empty() && self.pin.is_empty() {
            match std::fs::remove_file(&path) {
                Ok(()) => return Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(e) => {
                    return Err(Error::Filesystem(format!(
                        "removing {}: {}",
                        path.display(),
                        e
                    )));
                }
            }
        }
        crate::fed_dir::ensure_fed_dir(work_dir)?;
        let yaml = serde_yaml::to_string(self)
            .map_err(|e| Error::Config(format!("serializing variant selection: {e}")))?;
        // Readers (including the supervisor) must see a complete old or new
        // selection, never a partially written YAML document.
        use std::io::Write;
        let temporary = path.with_extension(format!("yaml.{:016x}.tmp", rand::random::<u64>()));
        let result = (|| -> std::io::Result<()> {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)?;
            file.write_all(yaml.as_bytes())?;
            file.sync_all()?;
            std::fs::rename(&temporary, &path)
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&temporary);
        }
        result.map_err(|e| Error::Filesystem(format!("writing {}: {}", path.display(), e)))
    }
}

/// Where a service's resolved variant came from. Reported by
/// `fed variant list` so a surprising choice is traceable to the thing that
/// made it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VariantSource {
    /// A `--variant <service>:<variant>` pin.
    CliPin,
    /// The `--variant a,b,c` preference list.
    CliPrefer,
    /// A pin in `.fed/variants.yaml`.
    FilePin,
    /// The preference list in `.fed/variants.yaml`.
    FilePrefer,
    /// The service's own `default_variant`.
    Default,
    /// The service's sole variant, with no `default_variant` needed.
    OnlyVariant,
}

impl std::fmt::Display for VariantSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::CliPin => "--variant pin",
            Self::CliPrefer => "--variant preference list",
            Self::FilePin => "pinned in .fed/variants.yaml",
            Self::FilePrefer => "preferred in .fed/variants.yaml",
            Self::Default => "default_variant",
            Self::OnlyVariant => "only variant",
        };
        f.write_str(s)
    }
}

/// The four precedence layers, resolved together.
///
/// CLI and file layers are kept apart rather than merged at load time
/// because a CLI *preference list* outranks a *file pin* — flattening them
/// into one `(prefer, pin)` pair would lose that ordering.
#[derive(Debug, Clone, Default)]
pub struct VariantSelection {
    cli_prefer: Vec<String>,
    cli_pin: HashMap<String, String>,
    file_prefer: Vec<String>,
    file_pin: HashMap<String, String>,
}

impl VariantSelection {
    /// Build a selection from `--variant` entries layered over
    /// `.fed/variants.yaml`.
    pub fn load(cli: &[String], work_dir: &Path) -> Result<Self> {
        let (cli_prefer, cli_pin) = split_entries(cli)?;
        let persisted = PersistedVariants::load(work_dir)?;
        Ok(Self {
            cli_prefer,
            cli_pin,
            file_prefer: persisted.prefer,
            file_pin: persisted.pin,
        })
    }

    /// Validate a proposed persisted selection before writing it.
    pub fn from_persisted(persisted: &PersistedVariants) -> Self {
        Self {
            file_prefer: persisted.prefer.clone(),
            file_pin: persisted.pin.clone(),
            ..Default::default()
        }
    }

    /// A selection built only from `--variant` entries, ignoring any
    /// persisted file. Used by `fed variant list` to show what the flags
    /// alone would do.
    pub fn from_cli(cli: &[String]) -> Result<Self> {
        let (cli_prefer, cli_pin) = split_entries(cli)?;
        Ok(Self {
            cli_prefer,
            cli_pin,
            ..Default::default()
        })
    }

    /// Every service name that some layer pins, CLI first.
    fn pinned_services(&self) -> Vec<&String> {
        let mut out: Vec<&String> = self.cli_pin.keys().collect();
        for name in self.file_pin.keys() {
            if !self.cli_pin.contains_key(name) {
                out.push(name);
            }
        }
        out.sort();
        out
    }

    /// Every variant name mentioned in either preference list.
    fn preferred_names(&self) -> impl Iterator<Item = &String> {
        self.cli_prefer.iter().chain(self.file_prefer.iter())
    }

    /// Choose a variant for `service`, and report which layer decided it.
    ///
    /// `service` must have at least one variant; callers reach this only
    /// after checking that. Validation has already guaranteed that
    /// `default_variant` is present whenever there is more than one variant
    /// and that it names a real one, so the fallback chain cannot come up
    /// empty on a validated config.
    pub fn pick_with_source<'a>(
        &self,
        name: &str,
        service: &'a Service,
    ) -> Result<(&'a str, VariantSource)> {
        if let Some(pinned) = self.cli_pin.get(name) {
            return Ok((resolve_pin(name, service, pinned)?, VariantSource::CliPin));
        }

        if let Some(hit) = first_match(&self.cli_prefer, service) {
            return Ok((hit, VariantSource::CliPrefer));
        }

        if let Some(pinned) = self.file_pin.get(name) {
            return Ok((resolve_pin(name, service, pinned)?, VariantSource::FilePin));
        }

        if let Some(hit) = first_match(&self.file_prefer, service) {
            return Ok((hit, VariantSource::FilePrefer));
        }

        if let Some(default) = &service.default_variant {
            let key = service
                .variants
                .get_key_value(default.as_str())
                .ok_or_else(|| {
                    Error::Validation(format!(
                        "Service '{}' sets default_variant: '{}', which is not one of its variants. Available: {}",
                        name,
                        default,
                        available(service)
                    ))
                })?;
            return Ok((key.0.as_str(), VariantSource::Default));
        }

        if service.variants.len() == 1 {
            let only = service.variants.keys().next().expect("len == 1");
            return Ok((only.as_str(), VariantSource::OnlyVariant));
        }

        Err(Error::Validation(format!(
            "Service '{}' has {} variants ({}) but no `default_variant`, and nothing selected one.",
            name,
            service.variants.len(),
            available(service)
        )))
    }

    /// Choose a variant for `service`. See [`Self::pick_with_source`].
    pub fn pick<'a>(&self, name: &str, service: &'a Service) -> Result<&'a str> {
        self.pick_with_source(name, service).map(|(v, _)| v)
    }
}

/// Split `--variant` / persisted entries into a preference list and pins.
///
/// Entries are split on commas so `--variant go,ts,rust` is one flag, and an
/// entry containing a colon is a `service:variant` pin rather than a
/// preference. Order within the preference list is significant and preserved.
pub fn split_entries(entries: &[String]) -> Result<(Vec<String>, HashMap<String, String>)> {
    let mut prefer = Vec::new();
    let mut pin = HashMap::new();

    for entry in entries {
        for part in entry.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            match part.split_once(':') {
                Some((service, variant)) => {
                    let (service, variant) = (service.trim(), variant.trim());
                    if service.is_empty() || variant.is_empty() {
                        return Err(Error::Validation(format!(
                            "Invalid --variant entry '{part}'. A pin looks like 'service:variant'."
                        )));
                    }
                    if variant.contains(':') {
                        return Err(Error::Validation(format!(
                            "Invalid --variant entry '{part}'. A pin has exactly one ':'."
                        )));
                    }
                    pin.insert(service.to_string(), variant.to_string());
                }
                None => {
                    if !prefer.iter().any(|p: &String| p == part) {
                        prefer.push(part.to_string());
                    }
                }
            }
        }
    }

    Ok((prefer, pin))
}

/// Look up a pinned variant name on `service`, with a listing of what it
/// does offer when the pin misses.
fn resolve_pin<'a>(name: &str, service: &'a Service, pinned: &str) -> Result<&'a str> {
    service
        .variants
        .get_key_value(pinned)
        .map(|(k, _)| k.as_str())
        .ok_or_else(|| {
            Error::Validation(format!(
                "Service '{}' has no variant '{}'. Available: {}",
                name,
                pinned,
                available(service)
            ))
        })
}

/// The first name in `prefer` that `service` actually offers.
fn first_match<'a>(prefer: &[String], service: &'a Service) -> Option<&'a str> {
    prefer
        .iter()
        .find_map(|want| service.variants.get_key_value(want.as_str()))
        .map(|(k, _)| k.as_str())
}

/// Comma-separated variant names, sorted, for error messages.
fn available(service: &Service) -> String {
    let mut names: Vec<&str> = service.variants.keys().map(String::as_str).collect();
    names.sort();
    names.join(", ")
}

/// Resolve every service that declares `variants:`, replacing it in
/// `config.services` with the chosen variant merged over it.
///
/// Idempotent: resolution clears `variants`/`default_variant` on the merged
/// service, so a second pass over an already-resolved config does nothing.
/// That matters because the authoritative call happens in `main.rs` while
/// `OrchestratorBuilder::build` makes a defensive one for configs that reach
/// an orchestrator by another route.
///
/// Runs *before* profile filtering, so a pin for a profile-gated service
/// still finds its target; if that service then gets filtered out, the
/// selection is reported as an unused warning rather than silently ignored.
pub fn resolve_variants(
    config: &mut Config,
    selection: &VariantSelection,
    active_profiles: &[String],
) -> Result<()> {
    if config.services.values().all(|s| s.variants.is_empty())
        && config.services.values().any(|s| s.variant.is_some())
    {
        return Ok(());
    }
    // Pins name a specific service, so a pin that matches nothing is a
    // mistake worth stopping for — unlike a preference-list name, which is
    // deliberately allowed to miss (see below).
    for name in selection.pinned_services() {
        match config.services.get(name) {
            None => {
                return Err(Error::Validation(format!(
                    "Cannot pin a variant for '{name}': no such service in this config."
                )));
            }
            Some(service) if service.variants.is_empty() && service.variant.is_none() => {
                return Err(Error::Validation(format!(
                    "Service '{name}' has no variants."
                )));
            }
            Some(_) => {}
        }
    }

    // A preference-list name no service offers is deliberately not an error:
    // one list (`go,ts,rust`) is meant to serve several checkouts, most of
    // which will only know some of those names. Collected before resolution,
    // which clears `variants` as it goes.
    let known: std::collections::HashSet<String> = config
        .services
        .values()
        .flat_map(|s| s.variants.keys().cloned())
        .collect();
    for want in selection.preferred_names() {
        if !known.contains(want) {
            tracing::warn!(
                "No service in this config has a '{}' variant — preference ignored here.",
                want
            );
        }
    }

    let names: Vec<String> = config
        .services
        .iter()
        .filter(|(_, s)| !s.variants.is_empty())
        .map(|(n, _)| n.clone())
        .collect();

    for name in names {
        let outer = config.services.get(&name).expect("name came from services");
        let (chosen, source) = selection.pick_with_source(&name, outer)?;
        let chosen = chosen.to_string();

        let mut merged = outer.variants[&chosen].clone();
        crate::package::ServiceMerger::merge_service(&mut merged, outer)?;
        // `merge_service` copies every field, so the merged service arrives
        // carrying the outer service's whole variant map. Clearing it is what
        // makes the result an ordinary service — and what makes a second
        // resolution pass a no-op instead of a re-derivation.
        merged.variants.clear();
        merged.default_variant = None;
        merged.variant = Some(chosen);
        if !is_active(&merged, active_profiles)
            && !matches!(source, VariantSource::Default | VariantSource::OnlyVariant)
        {
            tracing::warn!(
                "Variant selected for service '{}', but it is excluded by the active profiles and will not start.",
                name
            );
        }

        config.services.insert(name, merged);
    }

    // The selected implementation must satisfy ordinary service contracts,
    // including dependencies and scalar fields supplied by the variant.
    config.validate()?;
    Ok(())
}

/// Whether `service` survives the active profile filter — the same
/// Compose-style rule `Orchestrator::initialize` applies.
fn is_active(service: &Service, active_profiles: &[String]) -> bool {
    service.profiles.is_empty() || service.profiles.iter().any(|p| active_profiles.contains(p))
}
