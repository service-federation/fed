// Split from resolver.rs (see git history before this commit for pre-split blame).
use super::*;

/// Outcome of consulting the team vault for a set of queried names, after the
/// grace window / freshness policy has been applied (see 02-cold-vault.md).
enum VaultOutcome {
    /// The vault answered with values (within grace, or after an honest block).
    /// Authoritative — the cache is rewritten with fresh stamps.
    Values(HashMap<String, String>),
    /// Grace expired but the cache covers every queried name freshly. Proceed on
    /// the cache; the abandoned request is left to warm the backend.
    CacheFresh,
    /// The vault could not be reached or used. Fall back to the cache regardless
    /// of age (with a warning); the reason names the cloud in any missing error.
    Failed(String),
    /// Not logged in / checkout not linked — ordinary local mode.
    Local,
}

/// Current unix time in whole seconds (0 if the clock predates the epoch).
fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Whether the cache can cover every queried name *freshly*: each name is
/// cached and its stamp is younger than `max_age`. A missing name or a missing
/// stamp (pre-upgrade entry) counts as not-fresh, forcing an honest refresh.
///
/// A stamp in the future (`stamped > now`) is also not fresh: `saturating_sub`
/// would report age 0 and treat it as fresh until wall time caught up, so a
/// clock skew or a tampered stamp could pin a rotated value as "fresh" for
/// hours. Requiring `stamped <= now` closes that.
pub(crate) fn cache_covers_fresh(
    names: &[String],
    cache_values: &HashMap<String, String>,
    cache_stamps: &HashMap<String, u64>,
    now: u64,
    max_age_secs: u64,
) -> bool {
    names.iter().all(|name| {
        cache_values.contains_key(name)
            && cache_stamps
                .get(name)
                .is_some_and(|stamped| *stamped <= now && now - *stamped < max_age_secs)
    })
}

impl Resolver {
    /// Test seam: stub the team-vault lookup with fixed values.
    #[cfg(test)]
    pub(crate) fn set_test_vault_values(&mut self, values: HashMap<String, String>) {
        self.test_vault_values = Some(values);
    }

    /// Test seam: stub the team-vault lookup as failing (unreachable cloud).
    #[cfg(test)]
    pub(crate) fn set_test_vault_failure(&mut self, message: &str) {
        self.test_vault_failure = Some(message.to_string());
    }

    /// Whether a manual-secret name is in scope for this run. Names outside the
    /// scope are neither queried from the vault nor treated as required.
    fn name_in_scope(&self, name: &str) -> bool {
        match &self.required_names {
            Some(required) => required.contains(name),
            None => true,
        }
    }

    /// Compute the set of parameters deferred this run.
    ///
    /// The base set ("poison") is the manual secrets that are out of this run's
    /// scope: scoping (`fed <script>`) only fetches the secrets the target
    /// script transitively references, so anything outside that closure is never
    /// fetched and cannot be resolved. On top of that, any parameter whose value
    /// transitively depends on a poison name (via `default`, `generate`, or
    /// environment-specific interpolation) cannot be resolved either. The union
    /// is what must be deferred rather than failed.
    ///
    /// Determined from scope alone — it must be computed *before* secret
    /// resolution (whose `generate` DAG would otherwise execute a deferred
    /// generate over a secret it never fetches). Every deferred name is provably
    /// outside the scanned closure (an in-scope reference would put the secret
    /// in scope). A deferred name is therefore inert for this run: every
    /// downstream stage skips it, so it is never validated (port, `either`, or
    /// unresolved-template), never allocated a port, and never persisted — the
    /// secret-generate fallback writes nothing for it, so no random value can be
    /// stored and later mistaken for a real one. Over-approximation only ever
    /// touches things this run never spawns, exactly like the service-deferral
    /// rule.
    ///
    /// Returns an empty set for unscoped runs (`required_names == None`), so
    /// `fed start` and interactive `fed` stay exactly as strict as before: with
    /// nothing deferred, every unresolved template and every `generate` runs
    /// (and fails) as it does today.
    pub(super) fn compute_deferred_params(&self, config: &Config) -> HashSet<String> {
        if self.required_names.is_none() {
            return HashSet::new();
        }
        let poison: HashSet<String> = config
            .get_effective_parameters()
            .iter()
            .filter(|(name, param)| param.is_manual_secret() && !self.name_in_scope(name))
            .map(|(name, _)| name.clone())
            .collect();
        if poison.is_empty() {
            return HashSet::new();
        }
        // Close over parameters that transitively reference a poison name, then
        // union the poison names themselves.
        let mut deferred = crate::parameter::scanner::parameters_tainted_by(config, &poison);
        deferred.extend(poison);
        deferred
    }

    /// Whether a service must be deferred (dropped from the resolved config)
    /// this run because it references a deferred parameter — an out-of-scope
    /// missing manual secret, or a parameter that transitively depends on one.
    ///
    /// Scoping (`fed <script>`) only fetches the secrets the target script
    /// transitively references, so a deferred name is provably outside the
    /// scanned closure: the scanner walks every in-scope service whole-struct,
    /// so any name an in-scope service references is in scope (and not deferred)
    /// by construction. A reference to a deferred name can therefore only appear
    /// in a service this run will never spawn. Such services are dropped instead
    /// of hard-failing the whole run on a value it doesn't need; if one is
    /// somehow spawned anyway it fails loudly with `ServiceNotFound` rather than
    /// running with an unresolved value.
    ///
    /// Returns `false` for unscoped runs (`deferred_params` is empty), so
    /// `fed start` and interactive `fed` stay exactly as strict as before.
    pub(crate) fn service_should_defer(&self, service: &crate::config::Service) -> bool {
        if self.deferred_params.is_empty() {
            return false;
        }
        // Serialize the whole service and sweep every {{NAME}} out of it (the
        // same over-approximation the scanner uses), deferring on any reference
        // to a deferred parameter.
        let Ok(yaml) = serde_yaml::to_string(service) else {
            return false;
        };
        get_template_regex()
            .captures_iter(&yaml)
            .any(|cap| self.deferred_params.contains(cap[1].trim()))
    }

    /// Consult the team vault for `queried_names`, applying the grace-window +
    /// freshness policy (see 02-cold-vault.md).
    ///
    /// The single async fetch is fired here and joined with a short grace: a
    /// warm vault (~0.17s) answers well inside it, so values are fresh every
    /// run. If grace expires, a fresh cache short-circuits the wait (and the
    /// abandoned request warms the backend); otherwise we block honestly on a
    /// cold start, announced on stderr, up to the generous budget. Connect/DNS
    /// failures never burn the budget — nothing is listening.
    fn obtain_vault_outcome(
        &self,
        work_dir: &Path,
        queried_names: &[String],
        analysis: &crate::parameter::secret::SecretAnalysis,
    ) -> VaultOutcome {
        // Test seams bypass the timing machinery with a fixed outcome.
        if let Some(msg) = &self.test_vault_failure {
            return VaultOutcome::Failed(msg.clone());
        }
        if let Some(stub) = &self.test_vault_values {
            let values: HashMap<String, String> = queried_names
                .iter()
                .filter_map(|n| stub.get(n).map(|v| (n.clone(), v.clone())))
                .collect();
            return VaultOutcome::Values(values);
        }

        let Some(handle) = crate::cloud::spawn_fetch_values(work_dir, queried_names) else {
            return VaultOutcome::Local;
        };

        let classify = |join: crate::cloud::VaultJoin, url: &str| -> Option<VaultOutcome> {
            match join {
                crate::cloud::VaultJoin::Answered(Ok(values)) => Some(VaultOutcome::Values(values)),
                crate::cloud::VaultJoin::Answered(Err(f)) => {
                    // Both unreachable and reached-but-failed fall back to the
                    // cache; carry a message that names the cloud.
                    Some(VaultOutcome::Failed(format!("{} ({})", f.message(), url)))
                }
                crate::cloud::VaultJoin::Pending => None,
            }
        };

        // Phase 1: the short grace wait.
        if let Some(outcome) = classify(handle.join(crate::cloud::vault_grace()), &handle.url) {
            return outcome;
        }

        // Grace expired. If the cache can cover every queried name freshly,
        // proceed on it and abandon the request (it warms the backend).
        if cache_covers_fresh(
            queried_names,
            &analysis.cache_values,
            &analysis.cache_stamps,
            unix_now(),
            crate::cloud::vault_max_age().as_secs(),
        ) {
            return VaultOutcome::CacheFresh;
        }

        // Phase 2: the cache can't cover it — block honestly on the cold start.
        eprintln!("waking vault… (cold start can take ~20s)");
        match classify(handle.join(crate::cloud::vault_timeout()), &handle.url) {
            Some(outcome) => outcome,
            None => VaultOutcome::Failed(format!(
                "cloud: no response within {}s ({})",
                crate::cloud::vault_timeout().as_secs(),
                handle.url
            )),
        }
    }

    /// Resolve secret parameters: generate missing auto-secrets, fail on missing manual secrets.
    ///
    /// This runs before `.env` loading so that newly-generated values are picked up
    /// by the normal `apply_env_file_to_parameters` path.
    pub(super) fn resolve_secrets(&self, config: &mut Config) -> Result<()> {
        let work_dir = match self.work_dir.as_ref() {
            Some(dir) => dir.clone(),
            None => return Ok(()), // No work dir → skip (unit tests, etc.)
        };

        // Skip early (and avoid creating .fed/) when there are no secret params.
        if !config
            .get_effective_parameters()
            .values()
            .any(|p| p.is_secret_type())
        {
            return Ok(());
        }

        // .fed/ holds the vault cache and (by default) generated secrets.
        // Ensure it exists — with its self-ignoring .gitignore — before any
        // git-status checks so the default paths analyze as ignored.
        crate::fed_dir::ensure_fed_dir(&work_dir)?;

        // Generated secrets always live at .fed/secrets.generated.env.
        let secrets_file_path = crate::fed_dir::default_generated_secrets_path(&work_dir);
        // The env_file key under which the generated secrets file is loaded.
        let generated_env_key = crate::fed_dir::GENERATED_SECRETS_REL.to_string();
        let cache_path = crate::fed_dir::secrets_cache_path(&work_dir);
        let memory_only = self.secret_cache == crate::orchestrator::SecretCacheMode::Memory;

        // Cache safety gate: the cache holds real secret values, so it must
        // never sit in a commit-eligible location. With the self-managed
        // .fed/.gitignore this always passes; a user-edited permissive
        // .fed/.gitignore disables caching entirely — an existing cache file
        // is DELETED (leaving secrets on disk where git can pick them up is
        // the unsafe option) and its values are neither read nor rewritten.
        let cache_usable = if memory_only {
            // Memory cache policy is an affirmative no-persistence request:
            // remove an earlier file-backed cache before resolving anything,
            // then neither read nor rewrite it. Failing to remove it must be
            // loud; silently leaving plaintext behind would violate the mode's
            // central promise.
            if cache_path.exists() {
                std::fs::remove_file(&cache_path).map_err(|e| {
                    Error::Filesystem(format!(
                        "Cannot remove vault secrets cache '{}' for memory-only mode: {}",
                        cache_path.display(),
                        e
                    ))
                })?;
            }
            tracing::info!(
                "Team-vault cache is memory-only; fetched values will not be written to disk"
            );
            false
        } else {
            let (in_repo, ignored) = crate::parameter::secret::path_git_status(&cache_path);
            if in_repo && !ignored {
                let existed = cache_path.exists();
                if existed && let Err(e) = std::fs::remove_file(&cache_path) {
                    tracing::warn!(
                        "could not remove commit-eligible secrets cache {}: {}",
                        cache_path.display(),
                        e
                    );
                }
                tracing::warn!(
                    "vault secret caching disabled: {} is not gitignored (was .fed/.gitignore \
                     edited?).{} Offline runs won't have vault values until .fed/.gitignore \
                     ignores it again.",
                    cache_path.display(),
                    if existed {
                        " The existing cache file was removed."
                    } else {
                        ""
                    }
                );
                false
            } else {
                true
            }
        };

        let mut analysis = match crate::parameter::secret::analyze_secrets(
            config,
            &work_dir,
            &secrets_file_path,
            &cache_path,
        )? {
            Some(a) => a,
            None => return Ok(()), // No secret parameters at all
        };
        if !cache_usable {
            analysis.cache_values.clear();
            analysis.cache_stamps.clear();
        }

        // Team vault: when online and linked, the vault is authoritative for
        // manual secrets — query it for every missing (required AND optional)
        // name, including names the cache could satisfy, so rotated or revoked
        // values are picked up. Requires `fed login` + `fed link`; skipped
        // with --offline.
        //
        // Pre-network TTL skip (04-vault-ttl-cache.md): if the cache already
        // freshly covers every queried name — within `FED_VAULT_TTL` (default
        // 5m) of its last fetch — the vault stays authoritative but is never
        // even queried: no fetch fires, no thread spawns. This used to be
        // gated to `Environment::Development` only, since the local cache
        // (`.fed/secrets.cache.env`) is a single project-wide file, not
        // partitioned by environment, and a pre-network skip with no
        // environment check could let a fresh `staging` cache silently answer
        // a `production` run within the TTL window (Sol's adversarial
        // finding, see Design §4 in 04-vault-ttl-cache.md). The environment
        // axis was removed entirely in fed 8.0
        // (`08-environments-removal.md`), so the guard is gone too — the skip
        // is now unconditional on TTL/cache-freshness alone.
        let mut vault_resolved: Vec<(String, String)> = Vec::new();
        let mut vault_query_succeeded = false;
        // Captures why the vault lookup failed (network/auth), so a later
        // missing-secret error can name the real cause instead of blaming the
        // user's env_file for an unreachable cloud.
        let mut vault_failure: Option<String> = None;
        // Scope the vault query to names this run actually references. The
        // analysis itself stays project-wide (D2): its `cache_values` feed the
        // cache rewrite below, and scoping them would prune other scripts'
        // cached secrets on every run. Only what we *fetch* — and, at the end,
        // what we *fail on* — is scoped.
        let queried_names: Vec<String> = analysis
            .missing_manual
            .iter()
            .map(|(name, _)| name.clone())
            .chain(analysis.missing_optional_manual.iter().cloned())
            .filter(|name| self.name_in_scope(name))
            .collect();
        if !queried_names.is_empty() && !self.offline {
            let ttl = crate::cloud::vault_ttl();
            let ttl_covers = ttl.as_secs() > 0
                && cache_covers_fresh(
                    &queried_names,
                    &analysis.cache_values,
                    &analysis.cache_stamps,
                    unix_now(),
                    ttl.as_secs(),
                );
            if ttl_covers {
                // Cache is fresh within FED_VAULT_TTL — skip the network call
                // entirely. vault_query_succeeded stays false, so this falls
                // through to the existing cache-fallback branch below
                // unchanged. The cache is deliberately NOT rewritten here:
                // nothing changed, and rewriting would reset the very stamps
                // the next run's TTL check depends on, for no benefit.
                tracing::debug!(
                    "team vault skipped: cached secret values are fresh (< {:?})",
                    ttl
                );
            } else {
                match self.obtain_vault_outcome(&work_dir, &queried_names, &analysis) {
                    VaultOutcome::Values(values) => {
                        vault_query_succeeded = true;
                        for (name, value) in values {
                            if let Some(param) =
                                config.get_effective_parameters_mut().get_mut(&name)
                            {
                                param.value = Some(value.clone());
                            }
                            vault_resolved.push((name, value));
                        }
                        let resolved_names: HashSet<&str> =
                            vault_resolved.iter().map(|(n, _)| n.as_str()).collect();
                        analysis
                            .missing_manual
                            .retain(|(name, _)| !resolved_names.contains(name.as_str()));
                        analysis
                            .missing_optional_manual
                            .retain(|name| !resolved_names.contains(name.as_str()));
                        if !vault_resolved.is_empty() {
                            tracing::info!(
                                "Resolved {} secret(s) from the team vault",
                                vault_resolved.len()
                            );
                        }
                    }
                    VaultOutcome::CacheFresh => {
                        // Grace expired but the cache covers every queried name
                        // freshly — proceed on it, no warning. The abandoned
                        // in-flight request has already warmed the backend.
                        tracing::debug!(
                            "team vault slow to answer; proceeding on fresh cached values"
                        );
                    }
                    VaultOutcome::Local => {} // not logged in / not linked — local mode
                    VaultOutcome::Failed(reason) => {
                        // Reached-but-unusable or unreachable: fall back to the
                        // cache regardless of age (offline work must keep working),
                        // and remember the reason so a missing-secret failure names
                        // the cloud instead of the user's env_file.
                        tracing::warn!(
                            "team vault unavailable ({}); proceeding on cached secret values where available",
                            reason
                        );
                        vault_failure = Some(reason);
                    }
                }
            }
        }

        if vault_query_succeeded {
            // The vault answered: rewrite the cache to mirror it, stamping the
            // freshly-fetched names with the current time. Non-queried entries
            // are carried forward with their existing stamps; queried names the
            // vault no longer has are dropped (rotation/revocation), as are keys
            // for parameters no longer declared (analyze_secrets filtered those).
            let now = unix_now();
            let mut new_cache: HashMap<String, crate::parameter::secret::CacheEntry> = analysis
                .cache_values
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        crate::parameter::secret::CacheEntry {
                            value: v.clone(),
                            fetched_at: analysis.cache_stamps.get(k).copied(),
                        },
                    )
                })
                .collect();
            for name in &queried_names {
                new_cache.remove(name);
            }
            for (name, value) in &vault_resolved {
                new_cache.insert(
                    name.clone(),
                    crate::parameter::secret::CacheEntry {
                        value: value.clone(),
                        fetched_at: Some(now),
                    },
                );
            }
            // cache_usable was decided (and warned about) up front; an unsafe
            // path means no cache writes at all.
            if cache_usable
                && let Err(e) = crate::parameter::secret::write_cache_file(&cache_path, &new_cache)
            {
                tracing::warn!(
                    "could not cache vault secrets to {}: {}",
                    cache_path.display(),
                    e
                );
            }
        } else {
            // Vault unavailable (--offline, unlinked, or lookup failed): the
            // cache satisfies missing manual secrets, required ones included.
            let cache = &analysis.cache_values;
            for (name, _) in &analysis.missing_manual {
                if let Some(value) = cache.get(name)
                    && let Some(param) = config.get_effective_parameters_mut().get_mut(name)
                {
                    param.value = Some(value.clone());
                }
            }
            for name in &analysis.missing_optional_manual {
                if let Some(value) = cache.get(name)
                    && let Some(param) = config.get_effective_parameters_mut().get_mut(name)
                {
                    param.value = Some(value.clone());
                }
            }
            analysis
                .missing_manual
                .retain(|(name, _)| !cache.contains_key(name));
            analysis
                .missing_optional_manual
                .retain(|name| !cache.contains_key(name));
        }

        // Optional manual secrets the vault couldn't supply fall back to an
        // empty string so they resolve without error.
        for name in &analysis.missing_optional_manual {
            if let Some(param) = config.get_effective_parameters_mut().get_mut(name)
                && param.value.is_none()
            {
                param.value = Some(String::new());
            }
        }

        // Fail on missing manual secrets — user must provide these. Only
        // names in scope for this run count: a script must not fail on a
        // project-wide secret it never references (that is the whole point of
        // scoping — see 01-secret-scoping.md). Unscoped names stay in the
        // analysis (so the cache logic above is unaffected) but are excluded
        // from the failure here.
        let unmet: Vec<&(String, Option<String>)> = analysis
            .missing_manual
            .iter()
            .filter(|(name, _)| self.name_in_scope(name))
            .collect();
        if !unmet.is_empty() {
            let details: Vec<String> = unmet
                .iter()
                .map(|(name, desc)| match desc {
                    Some(d) => format!("  - {} ({})", name, d),
                    None => format!("  - {}", name),
                })
                .collect();
            let env_files_hint = if config.env_file.is_empty() {
                "your env_file".to_string()
            } else {
                config.env_file.join(", ")
            };
            // When the vault lookup itself failed (unreachable cloud, revoked
            // token), name that cause — otherwise the user is told to "add it to
            // your env_file" for a secret that was actually sitting in the vault.
            if let Some(reason) = &vault_failure {
                return Err(Error::Validation(format!(
                    "Missing secret values — the team vault could not be reached, so these could not be fetched ({}):\n{}\n\nOnce the vault is reachable again fed will fetch them; or add them to your env_file ({}) to proceed offline. These secrets have source: manual, so fed won't generate them.",
                    reason,
                    details.join("\n"),
                    env_files_hint
                )));
            }
            return Err(Error::Validation(format!(
                "Missing secret values — add them to your env_file ({}), or put them in your team vault (fed login, fed link, then set them in the dashboard):\n{}\n\nThese secrets have source: manual, so fed won't generate them.",
                env_files_hint,
                details.join("\n")
            )));
        }

        // Prepend the generated secrets file to env_file so it's loaded by
        // apply_env_file_to_parameters (lowest priority — user's .env files can
        // override). Only when it exists — it may not on the very first run.
        // The vault cache is deliberately NOT loaded as an env file: its values
        // are applied directly above, so leftover entries can never trip strict
        // env loading or shadow the vault.
        if secrets_file_path.exists() && !config.env_file.contains(&generated_env_key) {
            config.env_file.insert(0, generated_env_key.clone());
        }

        // Run DAG-based resolution for all secrets with `generate` commands.
        // This handles invalidation cascading even for secrets that have existing values.
        //
        // Deferred params are dropped from the DAG input: a deferred `generate`
        // references an out-of-scope missing manual secret this run never
        // fetches, so executing it here would hard-fail on a value the run
        // doesn't need. Every deferred name is provably outside the scanned
        // closure, so this can't drop a generate the target actually uses.
        //
        // The DAG is seeded with an EMPTY resolved map: a secret generator that
        // references any parameter (`printf %s {{SEED}}`) fails with
        // ParameterNotFound, exactly as before. A reference-less generator
        // (`openssl rand -hex 32`, `uuidgen`) still generates and persists.
        let effective_params: HashMap<String, crate::config::Parameter> = config
            .get_effective_parameters()
            .iter()
            .filter(|(name, _param)| !self.deferred_params.contains(*name))
            .map(|(name, param)| (name.clone(), param.clone()))
            .collect();
        let generate_results = crate::parameter::generate::resolve_generate_params(
            &effective_params,
            &analysis.existing_values,
            &HashMap::new(),
        )?;

        let dag_generated: HashSet<String> =
            generate_results.iter().map(|r| r.name.clone()).collect();

        let mut generated: Vec<(String, String)> = generate_results
            .into_iter()
            .filter(|r| r.was_generated)
            .map(|r| (r.name, r.value))
            .collect();

        // Simple secrets (no generate command) — use random alphanumeric.
        //
        // Two kinds of name must never reach this random fallback:
        //   - deferred names: a scoped run persists nothing for a parameter it
        //     defers (its value is out of scope this run); writing a random value
        //     would be an out-of-scope side effect.
        //   - anything carrying a `generate` command: if its generator did not run
        //     in the DAG above (it was deferred, or a dependency was), the fix is
        //     to run that generator on a later in-scope run — never to substitute
        //     randomness. A random value here would be persisted and then kept by
        //     the next run (generate.rs preserves existing secret values),
        //     permanently poisoning the derived secret.
        let effective = config.get_effective_parameters();
        for name in &analysis.needs_generation {
            if dag_generated.contains(name) || self.deferred_params.contains(name) {
                continue;
            }
            if effective.get(name).is_some_and(|p| p.has_generate()) {
                continue;
            }
            generated.push((name.clone(), crate::parameter::secret::generate_secret()));
        }

        // Nothing to write? We're done.
        if generated.is_empty() {
            return Ok(());
        }

        // Gitignore gate: if the secrets file is in a git repo and not ignored,
        // refuse. For the default .fed/ location this only trips if the user
        // edited .fed/.gitignore to unignore it.
        if analysis.in_git_repo && !analysis.is_gitignored {
            return Err(Error::Validation(format!(
                "Refusing to write secrets: '{}' is inside a git repository and is not gitignored.\n\n\
                 Restoring fed's .fed/.gitignore is enough — fed keeps it out of git \
                 automatically. If the file is already tracked by git, also run \
                 `git rm --cached {}`.",
                generated_env_key, generated_env_key
            )));
        }

        // Interactive confirmation when running in a TTY
        if self.is_interactive {
            eprint!(
                "Secret parameters need values. Generate and write to {}? [Y/n] ",
                analysis.env_path.display()
            );
            let mut input = String::new();
            if std::io::stdin().read_line(&mut input).is_ok() {
                let trimmed = input.trim().to_lowercase();
                if !trimmed.is_empty() && trimmed != "y" && trimmed != "yes" {
                    return Err(Error::Aborted);
                }
            }
        } else {
            tracing::info!("Generating secret values → {}", analysis.env_path.display());
        }

        crate::parameter::secret::write_env_file(&analysis.env_path, &generated)?;

        // Ensure the generated secrets file is in env_file now that it exists
        if !config.env_file.contains(&generated_env_key) {
            config.env_file.insert(0, generated_env_key.clone());
        }

        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    // ========================================================================
    // Secret resolution tests
    // ========================================================================

    #[test]
    fn secret_resolved_from_existing_env() {
        use crate::config::{Config, Parameter};
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let env_path = temp_dir.path().join(".env");
        fs::write(&env_path, "SESSION_KEY=existing_secret\n").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.parameters.insert(
            "SESSION_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                ..Default::default()
            },
        );
        config.env_file = vec![".env".to_string()];

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        assert_eq!(resolved.get("SESSION_KEY").unwrap(), "existing_secret");
    }

    #[test]
    fn missing_manual_secret_errors_with_description() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.parameters.insert(
            "GITHUB_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                description: Some("GitHub OAuth client secret".to_string()),
                ..Default::default()
            },
        );

        let err = resolver.resolve_parameters(&mut config).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("GITHUB_SECRET"),
            "Error should name the param: {}",
            msg
        );
        assert!(
            msg.contains("GitHub OAuth client secret"),
            "Error should include description: {}",
            msg
        );
    }

    #[test]
    fn optional_manual_secret_resolves_to_empty_string() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.parameters.insert(
            "STRIPE_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                optional: Some(true),
                description: Some("Stripe API key".to_string()),
                ..Default::default()
            },
        );

        // Should succeed, not error
        resolver.resolve_parameters(&mut config).unwrap();

        let param = config.parameters.get("STRIPE_KEY").unwrap();
        assert_eq!(param.value.as_deref(), Some(""));
    }

    #[test]
    fn non_optional_manual_secret_still_errors() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.parameters.insert(
            "REQUIRED_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        let err = resolver.resolve_parameters(&mut config).unwrap_err();
        assert!(err.to_string().contains("REQUIRED_SECRET"));
    }

    #[test]
    fn gitignore_gate_fires() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        git2::Repository::init(temp_dir.path()).unwrap();
        // A user-edited .fed/.gitignore that no longer ignores the generated
        // secrets file — the gate must still refuse to write into git's view.
        std::fs::create_dir_all(temp_dir.path().join(".fed")).unwrap();
        std::fs::write(temp_dir.path().join(".fed/.gitignore"), "").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.parameters.insert(
            "SESSION_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                ..Default::default()
            },
        );

        let err = resolver.resolve_parameters(&mut config).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains(".gitignore"),
            "Should mention .gitignore: {}",
            msg
        );
        assert!(
            msg.contains("git rm --cached"),
            "Should cover tracked files: {}",
            msg
        );
    }

    #[test]
    fn existing_secrets_file_loaded_on_subsequent_run() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        // Simulate a previous run that already generated the secrets file at
        // the default location.
        std::fs::create_dir_all(temp_dir.path().join(".fed")).unwrap();
        std::fs::write(
            temp_dir.path().join(".fed/secrets.generated.env"),
            "SESSION_KEY=previously_generated_value\n",
        )
        .unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.parameters.insert(
            "SESSION_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        assert_eq!(
            resolved.get("SESSION_KEY").unwrap(),
            "previously_generated_value",
            "Should load secret from existing .fed/secrets.generated.env on subsequent runs"
        );
    }

    #[test]
    fn generated_secret_defaults_to_fed_dir() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        // Git repo with NO root .gitignore — fed's self-managed .fed/.gitignore
        // must make the default location safe on its own.
        git2::Repository::init(temp_dir.path()).unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.parameters.insert(
            "SESSION_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        assert_eq!(resolved.get("SESSION_KEY").unwrap().len(), 32);

        let generated_env = temp_dir.path().join(".fed/secrets.generated.env");
        let content = std::fs::read_to_string(&generated_env).unwrap();
        assert!(content.contains("SESSION_KEY="));
        assert_eq!(config.env_file[0], ".fed/secrets.generated.env");

        // .fed/.gitignore was self-managed into existence
        let gi = std::fs::read_to_string(temp_dir.path().join(".fed/.gitignore")).unwrap();
        assert!(gi.contains("!cloud.yaml"));
    }

    #[test]
    fn fed_gitignore_not_clobbered_by_resolution() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join(".fed")).unwrap();
        std::fs::write(
            temp_dir.path().join(".fed/.gitignore"),
            "*\n!cloud.yaml\n!my-notes.md\n!.gitignore\n",
        )
        .unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.parameters.insert(
            "SESSION_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                ..Default::default()
            },
        );
        resolver.resolve_parameters(&mut config).unwrap();

        let gi = std::fs::read_to_string(temp_dir.path().join(".fed/.gitignore")).unwrap();
        assert!(gi.contains("!my-notes.md"), "user edits must survive");
    }

    #[test]
    fn optional_manual_secret_resolved_from_vault() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_test_vault_values(HashMap::from([(
            "STRIPE_KEY".to_string(),
            "sk_test_from_vault".to_string(),
        )]));

        let mut config = Config::default();
        config.parameters.insert(
            "STRIPE_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                optional: Some(true),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();

        let resolved = resolver.get_resolved_parameters();
        assert_eq!(
            resolved.get("STRIPE_KEY").unwrap(),
            "sk_test_from_vault",
            "Optional manual secret should take the vault value when available"
        );

        // Vault hit is cached in .fed/secrets.cache.env, not the generated file
        let cache = temp_dir.path().join(".fed/secrets.cache.env");
        let content = std::fs::read_to_string(&cache).unwrap();
        assert!(content.contains("STRIPE_KEY=sk_test_from_vault"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&cache).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "cache must be owner-only");
        }
    }

    #[test]
    fn optional_manual_secret_vault_miss_falls_back_to_empty() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        // Vault reachable but has no value for this name
        resolver.set_test_vault_values(HashMap::new());

        let mut config = Config::default();
        config.parameters.insert(
            "STRIPE_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                optional: Some(true),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();
        assert_eq!(
            config
                .parameters
                .get("STRIPE_KEY")
                .unwrap()
                .value
                .as_deref(),
            Some(""),
            "Vault miss on an optional secret must fall back to empty string"
        );
        assert!(
            !temp_dir.path().join(".fed/secrets.cache.env").exists(),
            "Nothing to cache on a vault miss"
        );
    }

    #[test]
    fn required_manual_secret_cached_then_resolved_offline() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        let make_config = || {
            let mut config = Config::default();
            config.parameters.insert(
                "API_KEY".to_string(),
                Parameter {
                    param_type: Some("secret".to_string()),
                    source: Some("manual".to_string()),
                    ..Default::default()
                },
            );
            config
        };

        // First run: online, vault supplies the value → cached.
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_test_vault_values(HashMap::from([(
            "API_KEY".to_string(),
            "vault_value".to_string(),
        )]));
        let mut config = make_config();
        resolver.resolve_parameters(&mut config).unwrap();
        assert!(temp_dir.path().join(".fed/secrets.cache.env").exists());

        // Second run: offline — the cache alone must resolve it.
        let mut offline_resolver = Resolver::new();
        offline_resolver.set_work_dir(temp_dir.path());
        offline_resolver.set_offline(true);
        let mut config = make_config();
        offline_resolver.resolve_parameters(&mut config).unwrap();
        assert_eq!(
            offline_resolver
                .get_resolved_parameters()
                .get("API_KEY")
                .unwrap(),
            "vault_value",
            "--offline must be served from .fed/secrets.cache.env"
        );
    }

    #[test]
    fn unignored_but_populated_does_not_error() {
        // Presence of the generated secrets file alone must not break a
        // previously-working setup — the gate fires only on actual writes.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        git2::Repository::init(temp_dir.path()).unwrap();
        std::fs::create_dir_all(temp_dir.path().join(".fed")).unwrap();
        // A user-edited .fed/.gitignore that no longer ignores the generated
        // secrets file.
        std::fs::write(temp_dir.path().join(".fed/.gitignore"), "").unwrap();
        std::fs::write(
            temp_dir.path().join(".fed/secrets.generated.env"),
            "SESSION_KEY=already_here\n",
        )
        .unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.parameters.insert(
            "SESSION_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();
        assert_eq!(
            resolver
                .get_resolved_parameters()
                .get("SESSION_KEY")
                .unwrap(),
            "already_here"
        );
    }

    #[test]
    fn write_attempt_to_unignored_path_errors() {
        // ...but as soon as fed would WRITE to the unsafe path, it refuses,
        // and the message covers the tracked-file case (git rm --cached).
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        git2::Repository::init(temp_dir.path()).unwrap();
        std::fs::create_dir_all(temp_dir.path().join(".fed")).unwrap();
        std::fs::write(temp_dir.path().join(".fed/.gitignore"), "").unwrap();
        std::fs::write(
            temp_dir.path().join(".fed/secrets.generated.env"),
            "SESSION_KEY=already_here\n",
        )
        .unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());

        let mut config = Config::default();
        config.parameters.insert(
            "SESSION_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                ..Default::default()
            },
        );
        // A second secret that has no value forces a write.
        config.parameters.insert(
            "NEW_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                ..Default::default()
            },
        );

        let err = resolver.resolve_parameters(&mut config).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("not gitignored"),
            "loud error expected: {}",
            msg
        );
        assert!(
            msg.contains("git rm --cached"),
            "must cover the tracked-file case: {}",
            msg
        );
    }

    #[test]
    fn cache_covers_fresh_requires_present_stamped_and_young() {
        let now = 1_000_000u64;
        let max_age = 3600u64; // 1h
        let names = vec!["A".to_string(), "B".to_string()];

        let mut values = HashMap::new();
        values.insert("A".to_string(), "va".to_string());
        values.insert("B".to_string(), "vb".to_string());

        // Both present, both fresh → covered.
        let mut stamps = HashMap::new();
        stamps.insert("A".to_string(), now - 10);
        stamps.insert("B".to_string(), now - 20);
        assert!(cache_covers_fresh(&names, &values, &stamps, now, max_age));

        // B too old → not covered.
        stamps.insert("B".to_string(), now - max_age - 1);
        assert!(!cache_covers_fresh(&names, &values, &stamps, now, max_age));

        // B present but unstamped (pre-upgrade) → treated as too old.
        stamps.remove("B");
        assert!(!cache_covers_fresh(&names, &values, &stamps, now, max_age));

        // B value missing entirely → not covered.
        stamps.insert("B".to_string(), now);
        values.remove("B");
        assert!(!cache_covers_fresh(&names, &values, &stamps, now, max_age));
    }

    /// A `max_age` of 0 naturally degrades to "never fresh": `now - stamped <
    /// 0` never holds for a `u64`, even for a stamp equal to `now`. This means
    /// `FED_VAULT_TTL=0` forces every run through `obtain_vault_outcome` even
    /// without the explicit `ttl.as_secs() > 0` guard at the call site — that
    /// guard is belt-and-suspenders for readability, not load-bearing.
    #[test]
    fn cache_covers_fresh_with_zero_max_age_is_never_fresh() {
        let now = 1_000_000u64;
        let names = vec!["A".to_string()];
        let mut values = HashMap::new();
        values.insert("A".to_string(), "va".to_string());
        let mut stamps = HashMap::new();
        stamps.insert("A".to_string(), now); // freshest possible stamp
        assert!(!cache_covers_fresh(&names, &values, &stamps, now, 0));
    }

    #[test]
    fn cache_covers_fresh_rejects_future_stamps() {
        // A stamp in the future must NOT count as fresh: saturating_sub would
        // report age 0 and pin a rotated value as fresh until wall time caught
        // up. Requiring stamped <= now closes that (clock skew / tampering).
        let now = 1_000_000u64;
        let max_age = 3600u64;
        let names = vec!["A".to_string()];
        let mut values = HashMap::new();
        values.insert("A".to_string(), "va".to_string());

        let mut stamps = HashMap::new();
        // Stamped one hour into the future.
        stamps.insert("A".to_string(), now + 3600);
        assert!(
            !cache_covers_fresh(&names, &values, &stamps, now, max_age),
            "a future-dated stamp must not be treated as fresh"
        );

        // Stamped exactly now → fresh (boundary).
        stamps.insert("A".to_string(), now);
        assert!(cache_covers_fresh(&names, &values, &stamps, now, max_age));
    }

    #[test]
    fn vault_failure_proceeds_on_cached_value_regardless_of_age() {
        // 02 done-when (airplane mode + cached values): when the vault is
        // unreachable, a required secret already in the cache resolves from it
        // regardless of the entry's age — offline work must keep working.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        crate::fed_dir::ensure_fed_dir(temp_dir.path()).unwrap();
        // Ancient, unstamped cache entry (would be "too old" for a refresh
        // decision) — but with the vault down we proceed on it anyway.
        std::fs::write(
            temp_dir.path().join(".fed/secrets.cache.env"),
            "API_KEY=cached_old\n",
        )
        .unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_test_vault_failure("cloud: cannot reach https://app.service-federation.com");

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        resolver
            .resolve_parameters(&mut config)
            .expect("cached value must satisfy the run when the vault is down");
        assert_eq!(
            resolver.get_resolved_parameters().get("API_KEY").unwrap(),
            "cached_old"
        );
        // The cache is NOT rewritten on failure — the value survives.
        let cache =
            std::fs::read_to_string(temp_dir.path().join(".fed/secrets.cache.env")).unwrap();
        assert!(cache.contains("API_KEY=cached_old"));
    }

    // ── FED_VAULT_TTL pre-network skip (04-vault-ttl-cache.md) ────────────
    //
    // These all use `test_vault_values` with a value DIFFERENT from the
    // pre-populated cache. That's deliberate: the only way the resolved value
    // can be the cache's rather than the stub's is if `obtain_vault_outcome`
    // (and the stub check inside it) was never reached — the sharpest
    // possible proof the network path was skipped.

    #[test]
    fn ttl_fresh_cache_skips_vault_entirely() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        crate::fed_dir::ensure_fed_dir(temp_dir.path()).unwrap();
        let fresh_stamp = unix_now() - 10; // well under the 300s default TTL
        std::fs::write(
            temp_dir.path().join(".fed/secrets.cache.env"),
            format!("# fetched-at API_KEY {fresh_stamp}\nAPI_KEY=cached-value\n"),
        )
        .unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_test_vault_values(HashMap::from([(
            "API_KEY".to_string(),
            "vault-value".to_string(),
        )]));

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();
        assert_eq!(
            resolver.get_resolved_parameters().get("API_KEY").unwrap(),
            "cached-value",
            "a fresh cache within FED_VAULT_TTL must skip the vault call entirely"
        );
    }

    #[test]
    fn ttl_stale_cache_still_queries_vault() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        crate::fed_dir::ensure_fed_dir(temp_dir.path()).unwrap();
        let stale_stamp = unix_now().saturating_sub(301); // just past the 300s default TTL
        std::fs::write(
            temp_dir.path().join(".fed/secrets.cache.env"),
            format!("# fetched-at API_KEY {stale_stamp}\nAPI_KEY=cached-value\n"),
        )
        .unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_test_vault_values(HashMap::from([(
            "API_KEY".to_string(),
            "vault-value".to_string(),
        )]));

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();
        assert_eq!(
            resolver.get_resolved_parameters().get("API_KEY").unwrap(),
            "vault-value",
            "a cache older than FED_VAULT_TTL must still query the vault"
        );
        // The vault answered, so the cache is rewritten with a fresh stamp.
        let cache =
            std::fs::read_to_string(temp_dir.path().join(".fed/secrets.cache.env")).unwrap();
        assert!(cache.contains("API_KEY=vault-value"));
    }

    #[test]
    fn ttl_partial_miss_forces_full_vault_query() {
        // Any single missing/stale name among the queried set forces a full
        // vault query for ALL queried names — no partial fetch.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        crate::fed_dir::ensure_fed_dir(temp_dir.path()).unwrap();
        let fresh_stamp = unix_now() - 10;
        // API_KEY is fresh in cache; OTHER_KEY is entirely absent.
        std::fs::write(
            temp_dir.path().join(".fed/secrets.cache.env"),
            format!("# fetched-at API_KEY {fresh_stamp}\nAPI_KEY=cached-value\n"),
        )
        .unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_test_vault_values(HashMap::from([
            ("API_KEY".to_string(), "vault-api-key".to_string()),
            ("OTHER_KEY".to_string(), "vault-other-key".to_string()),
        ]));

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "OTHER_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();
        let resolved = resolver.get_resolved_parameters();
        assert_eq!(
            resolved.get("API_KEY").unwrap(),
            "vault-api-key",
            "a miss anywhere in queried_names forces the full set through the vault, \
             not a mix of cache and vault answers"
        );
        assert_eq!(resolved.get("OTHER_KEY").unwrap(), "vault-other-key");
    }

    #[test]
    fn ttl_skip_does_not_rewrite_cache() {
        // Guards against a future refactor accidentally rewriting the cache on
        // the skip path — that would reset the very stamps the next run's TTL
        // check depends on, for no benefit.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        crate::fed_dir::ensure_fed_dir(temp_dir.path()).unwrap();
        let fresh_stamp = unix_now() - 10;
        let cache_path = temp_dir.path().join(".fed/secrets.cache.env");
        std::fs::write(
            &cache_path,
            format!("# fetched-at API_KEY {fresh_stamp}\nAPI_KEY=cached-value\n"),
        )
        .unwrap();
        let before = std::fs::read_to_string(&cache_path).unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_test_vault_values(HashMap::from([(
            "API_KEY".to_string(),
            "vault-value".to_string(),
        )]));

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();
        let after = std::fs::read_to_string(&cache_path).unwrap();
        assert_eq!(
            before, after,
            "the cache must be byte-for-byte unchanged on the TTL-skip path"
        );
    }

    #[test]
    fn successful_vault_run_stamps_the_cache() {
        // A successful online run writes a fetched-at stamp, so a later run can
        // apply the freshness bound.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_test_vault_values(HashMap::from([(
            "API_KEY".to_string(),
            "fresh".to_string(),
        )]));

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();
        let cache_path = temp_dir.path().join(".fed/secrets.cache.env");
        let stamps = crate::parameter::secret::load_cache_stamps(&cache_path);
        assert!(
            stamps.contains_key("API_KEY"),
            "a vault hit must be stamped with its fetched-at time"
        );
    }

    #[test]
    fn scoped_run_ignores_out_of_scope_secret_with_vault_down() {
        // 01 done-when: a run scoped to names it references makes zero cloud
        // requests for an unreferenced STRIPE_SECRET and succeeds even with the
        // vault unreachable.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        // Scope excludes STRIPE_SECRET entirely.
        resolver.set_required_names(Some(HashSet::new()));
        // If anything were queried, this failure would surface — it must not.
        resolver.set_test_vault_failure("cloud: cannot reach vault");

        let mut config = Config::default();
        config.parameters.insert(
            "STRIPE_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        // Must not error despite the required manual secret being unresolved:
        // it is out of scope for this run.
        resolver
            .resolve_parameters(&mut config)
            .expect("out-of-scope required secret must not fail the run");
        assert!(
            !temp_dir.path().join(".fed/secrets.cache.env").exists(),
            "no vault query means nothing cached"
        );
    }

    #[test]
    fn scoped_run_defers_out_of_scope_service_referencing_missing_secret() {
        // RB-1: a scoped run must not hard-fail during resolution because an
        // *unrelated* service references a manual secret this run never uses.
        // The service is out of the scanned closure, so it is dropped from the
        // resolved config rather than failing the run.
        use crate::config::{Config, Parameter, Service};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp.path());
        // Secret-free target → nothing in scope.
        resolver.set_required_names(Some(HashSet::new()));

        let mut config = Config::default();
        config.parameters.insert(
            "UNRELATED_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );
        config.services.insert(
            "unrelated".to_string(),
            Service {
                process: Some("serve {{UNRELATED_SECRET}}".to_string()),
                ..Default::default()
            },
        );

        resolver
            .resolve_parameters(&mut config)
            .expect("scoped parameter resolution must not fail");
        let resolved = resolver
            .resolve_config(&config)
            .expect("an out-of-scope service must not hard-fail the scoped run");
        assert!(
            !resolved.services.contains_key("unrelated"),
            "the out-of-scope service must be dropped from the resolved config"
        );
    }

    #[test]
    fn scoped_run_defers_derived_default_on_out_of_scope_secret() {
        // RB: a parameter whose default interpolates an out-of-scope missing
        // manual secret must be deferred, not fail the scoped run.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp.path());
        resolver.set_required_names(Some(HashSet::new())); // nothing in scope

        let mut config = Config::default();
        config.parameters.insert(
            "UNUSED_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "UNUSED_DERIVED".to_string(),
            Parameter {
                default: Some(serde_yaml::Value::String(
                    "prefix-{{UNUSED_SECRET}}".to_string(),
                )),
                ..Default::default()
            },
        );

        resolver
            .resolve_parameters(&mut config)
            .expect("a derived default over an out-of-scope secret must be deferred, not fatal");
    }

    #[test]
    fn scoped_run_defers_generate_referencing_out_of_scope_secret() {
        // RB (a): a non-secret generate that references an out-of-scope missing
        // manual secret must be deferred (its command not executed), not fatal.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp.path());
        resolver.set_required_names(Some(HashSet::new()));

        let mut config = Config::default();
        config.parameters.insert(
            "UNUSED_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "UNUSED_GEN".to_string(),
            Parameter {
                // Would fail if executed — `false` exits non-zero — but must be
                // deferred (skipped) entirely because it references a secret out
                // of scope.
                generate: Some("false {{UNUSED_SECRET}}".to_string()),
                ..Default::default()
            },
        );

        resolver
            .resolve_parameters(&mut config)
            .expect("a generate over an out-of-scope secret must be deferred, not executed");
        assert!(
            !resolver
                .get_resolved_parameters()
                .contains_key("UNUSED_GEN"),
            "a deferred generate must not produce a value"
        );
    }

    #[test]
    fn unscoped_run_still_fails_on_generate_referencing_missing_secret() {
        // Control for the test above: with no scoping the same generate runs and
        // fails on the missing secret — unscoped behavior is unchanged.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp.path());
        resolver.set_offline(true); // secret genuinely missing

        let mut config = Config::default();
        config.parameters.insert(
            "UNUSED_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "UNUSED_GEN".to_string(),
            Parameter {
                generate: Some("false {{UNUSED_SECRET}}".to_string()),
                ..Default::default()
            },
        );

        assert!(
            resolver.resolve_parameters(&mut config).is_err(),
            "unscoped run must still fail on the generate's missing secret"
        );
    }

    #[test]
    fn scoped_run_still_fails_on_in_scope_derived_missing_secret() {
        // Strictness preserved: a derived default over an IN-scope secret that is
        // missing must still fail — only out-of-scope dependencies are deferred.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp.path());
        resolver.set_offline(true); // in-scope secret is genuinely missing
        resolver.set_required_names(Some(HashSet::from(["IN_SCOPE_SECRET".to_string()])));

        let mut config = Config::default();
        config.parameters.insert(
            "IN_SCOPE_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "DERIVED".to_string(),
            Parameter {
                default: Some(serde_yaml::Value::String(
                    "prefix-{{IN_SCOPE_SECRET}}".to_string(),
                )),
                ..Default::default()
            },
        );

        assert!(
            resolver.resolve_parameters(&mut config).is_err(),
            "an in-scope missing secret must still fail the run"
        );
    }

    #[test]
    fn unscoped_run_still_fails_on_service_referencing_missing_secret() {
        // Control: with no scoping, the same missing manual secret still fails —
        // fed start stays exactly as strict as before.
        use crate::config::{Config, Parameter, Service};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp.path());
        resolver.set_offline(true); // no vault, no cache → genuinely missing

        let mut config = Config::default();
        config.parameters.insert(
            "UNRELATED_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );
        config.services.insert(
            "unrelated".to_string(),
            Service {
                process: Some("serve {{UNRELATED_SECRET}}".to_string()),
                ..Default::default()
            },
        );

        // Fails during secret resolution (the required secret is missing);
        // the service is never dropped because nothing is scoped.
        assert!(
            resolver.resolve_parameters(&mut config).is_err(),
            "unscoped run must still fail on the missing required secret"
        );
    }

    #[test]
    fn unscoped_run_still_fails_on_missing_required_secret() {
        // Control for the test above: with no scoping (None), the same missing
        // required manual secret still fails — scoping is what changes it.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_offline(true); // no vault, no cache → genuinely missing

        let mut config = Config::default();
        config.parameters.insert(
            "STRIPE_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        assert!(
            resolver.resolve_parameters(&mut config).is_err(),
            "unscoped run must still fail on a missing required secret"
        );
    }

    #[test]
    fn scoped_run_queries_only_in_scope_secret() {
        // A run scoped to API_KEY resolves it from the vault while leaving an
        // out-of-scope STRIPE_SECRET untouched (not queried, not failed).
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_required_names(Some(HashSet::from(["API_KEY".to_string()])));
        resolver.set_test_vault_values(HashMap::from([
            ("API_KEY".to_string(), "from_vault".to_string()),
            (
                "STRIPE_SECRET".to_string(),
                "should_not_be_used".to_string(),
            ),
        ]));

        let mut config = Config::default();
        for name in ["API_KEY", "STRIPE_SECRET"] {
            config.parameters.insert(
                name.to_string(),
                Parameter {
                    param_type: Some("secret".to_string()),
                    source: Some("manual".to_string()),
                    ..Default::default()
                },
            );
        }

        resolver.resolve_parameters(&mut config).unwrap();
        assert_eq!(
            resolver.get_resolved_parameters().get("API_KEY").unwrap(),
            "from_vault"
        );
        let cache =
            std::fs::read_to_string(temp_dir.path().join(".fed/secrets.cache.env")).unwrap();
        assert!(cache.contains("API_KEY=from_vault"));
        assert!(
            !cache.contains("STRIPE_SECRET"),
            "out-of-scope secret must not be queried or cached: {cache}"
        );
    }

    #[test]
    fn scoped_run_preserves_cache_entries_for_unqueried_secrets() {
        // 01 done-when: cache entries for secrets NOT queried this run survive.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        crate::fed_dir::ensure_fed_dir(temp_dir.path()).unwrap();
        std::fs::write(
            temp_dir.path().join(".fed/secrets.cache.env"),
            "API_KEY=old\nSTRIPE_SECRET=cached_stripe\n",
        )
        .unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_required_names(Some(HashSet::from(["API_KEY".to_string()])));
        resolver.set_test_vault_values(HashMap::from([(
            "API_KEY".to_string(),
            "rotated".to_string(),
        )]));

        let mut config = Config::default();
        for name in ["API_KEY", "STRIPE_SECRET"] {
            config.parameters.insert(
                name.to_string(),
                Parameter {
                    param_type: Some("secret".to_string()),
                    source: Some("manual".to_string()),
                    ..Default::default()
                },
            );
        }

        resolver.resolve_parameters(&mut config).unwrap();
        let cache =
            std::fs::read_to_string(temp_dir.path().join(".fed/secrets.cache.env")).unwrap();
        assert!(cache.contains("API_KEY=rotated"), "queried name refreshed");
        assert!(
            cache.contains("STRIPE_SECRET=cached_stripe"),
            "unqueried cache entry must survive the scoped run: {cache}"
        );
    }

    #[test]
    fn missing_secret_with_unreachable_vault_names_the_cloud() {
        // Step 0: a required secret that the vault could not supply because the
        // cloud was unreachable must produce an error naming that cause — not
        // "add it to your env_file", which misdirects the user away from the
        // real problem.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_test_vault_failure(
            "cloud: cannot reach https://app.service-federation.com: error sending request",
        );

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        let err = resolver.resolve_parameters(&mut config).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("team vault could not be reached"),
            "error must name the unreachable cloud as the cause: {msg}"
        );
        assert!(
            msg.contains("app.service-federation.com"),
            "error should carry the underlying reason: {msg}"
        );
    }

    #[test]
    fn missing_secret_without_vault_keeps_env_file_hint() {
        // With no vault failure (simply not provided anywhere), the classic
        // "add them to your env_file" guidance still applies.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_offline(true);

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        let err = resolver.resolve_parameters(&mut config).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("add them to your env_file"),
            "offline missing secret keeps the env_file guidance: {msg}"
        );
        assert!(
            !msg.contains("team vault could not be reached"),
            "no vault failure means no unreachable-cloud message: {msg}"
        );
        // Writes are dashboard-only since fed 7.0 — the hint points there and
        // must never mention the removed `fed secrets set` command.
        assert!(
            msg.contains("set them in the dashboard"),
            "hint should direct writes to the dashboard: {msg}"
        );
        assert!(
            !msg.contains("fed secrets set"),
            "the removed `fed secrets set` command must not appear: {msg}"
        );
    }

    #[test]
    fn vault_refetch_overwrites_stale_cache_when_online() {
        // P1-2: a cached value must not shadow the vault — online runs
        // re-query and the fresh value wins, in params and in the cache file.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        crate::fed_dir::ensure_fed_dir(temp_dir.path()).unwrap();
        std::fs::write(
            temp_dir.path().join(".fed/secrets.cache.env"),
            "API_KEY=stale_value\n",
        )
        .unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_test_vault_values(HashMap::from([(
            "API_KEY".to_string(),
            "rotated_value".to_string(),
        )]));

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();
        assert_eq!(
            resolver.get_resolved_parameters().get("API_KEY").unwrap(),
            "rotated_value",
            "vault must win over the cache when online"
        );
        let cache =
            std::fs::read_to_string(temp_dir.path().join(".fed/secrets.cache.env")).unwrap();
        assert!(cache.contains("API_KEY=rotated_value"));
        assert!(!cache.contains("stale_value"));
    }

    #[test]
    fn stale_cache_keys_filtered_and_pruned() {
        // P1-3: keys no longer declared must neither break resolution nor
        // survive the next successful cache write.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        crate::fed_dir::ensure_fed_dir(temp_dir.path()).unwrap();
        std::fs::write(
            temp_dir.path().join(".fed/secrets.cache.env"),
            "REMOVED_PARAM=leftover\nAPI_KEY=old\n",
        )
        .unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_test_vault_values(HashMap::from([(
            "API_KEY".to_string(),
            "fresh".to_string(),
        )]));

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        // Strict env loading must not choke on REMOVED_PARAM.
        resolver.resolve_parameters(&mut config).unwrap();

        let cache =
            std::fs::read_to_string(temp_dir.path().join(".fed/secrets.cache.env")).unwrap();
        assert!(cache.contains("API_KEY=fresh"));
        assert!(
            !cache.contains("REMOVED_PARAM"),
            "undeclared keys are pruned on write: {}",
            cache
        );
    }

    #[test]
    fn stale_cache_key_does_not_break_offline_resolution() {
        // P1-3 (load side): offline, the filtered cache satisfies declared
        // secrets and the leftover key is simply ignored.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        crate::fed_dir::ensure_fed_dir(temp_dir.path()).unwrap();
        std::fs::write(
            temp_dir.path().join(".fed/secrets.cache.env"),
            "REMOVED_PARAM=leftover\nAPI_KEY=cached\n",
        )
        .unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_offline(true);

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();
        assert_eq!(
            resolver.get_resolved_parameters().get("API_KEY").unwrap(),
            "cached"
        );
    }

    #[test]
    fn cache_write_declined_when_cache_path_not_gitignored() {
        // P1-4: a user-edited permissive .fed/.gitignore must not turn the
        // cache into a committable secrets file — fed declines to cache.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        git2::Repository::init(temp_dir.path()).unwrap();
        std::fs::create_dir_all(temp_dir.path().join(".fed")).unwrap();
        // Permissive: nothing ignored.
        std::fs::write(temp_dir.path().join(".fed/.gitignore"), "").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_test_vault_values(HashMap::from([(
            "API_KEY".to_string(),
            "vault_value".to_string(),
        )]));

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();
        assert_eq!(
            resolver.get_resolved_parameters().get("API_KEY").unwrap(),
            "vault_value",
            "resolution itself still succeeds"
        );
        assert!(
            !temp_dir.path().join(".fed/secrets.cache.env").exists(),
            "cache must not be written to a commit-eligible path"
        );
    }

    #[test]
    fn unsafe_cache_path_deletes_existing_cache_online() {
        // P1-4 residual: a pre-existing cache on a commit-eligible path is
        // removed (not kept and reused), its values are ignored, and online
        // resolution still succeeds from the vault.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        git2::Repository::init(temp_dir.path()).unwrap();
        std::fs::create_dir_all(temp_dir.path().join(".fed")).unwrap();
        // Permissive: nothing ignored → cache is commit-eligible.
        std::fs::write(temp_dir.path().join(".fed/.gitignore"), "").unwrap();
        std::fs::write(
            temp_dir.path().join(".fed/secrets.cache.env"),
            "API_KEY=stale_committable\n",
        )
        .unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_test_vault_values(HashMap::from([(
            "API_KEY".to_string(),
            "vault_value".to_string(),
        )]));

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();
        assert_eq!(
            resolver.get_resolved_parameters().get("API_KEY").unwrap(),
            "vault_value",
            "online resolution still succeeds"
        );
        assert!(
            !temp_dir.path().join(".fed/secrets.cache.env").exists(),
            "commit-eligible cache must be deleted and never rewritten"
        );
    }

    #[test]
    fn unsafe_cache_path_refuses_cached_values_offline() {
        // P1-4 residual: on an unsafe path the cache is deleted and its values
        // are refused — offline, a required secret it used to satisfy is
        // reported missing rather than served from a commit-eligible file.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        git2::Repository::init(temp_dir.path()).unwrap();
        std::fs::create_dir_all(temp_dir.path().join(".fed")).unwrap();
        std::fs::write(temp_dir.path().join(".fed/.gitignore"), "").unwrap();
        std::fs::write(
            temp_dir.path().join(".fed/secrets.cache.env"),
            "API_KEY=stale_committable\n",
        )
        .unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_offline(true);

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        let err = resolver.resolve_parameters(&mut config).unwrap_err();
        assert!(err.to_string().contains("API_KEY"));
        assert!(
            !temp_dir.path().join(".fed/secrets.cache.env").exists(),
            "unsafe cache file must be deleted"
        );
    }

    #[test]
    fn memory_cache_resolves_from_vault_without_writing_or_retaining_a_file() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        crate::fed_dir::ensure_fed_dir(temp_dir.path()).unwrap();
        let cache_path = temp_dir.path().join(".fed/secrets.cache.env");
        std::fs::write(&cache_path, "API_KEY=stale_file_value\n").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_secret_cache(crate::orchestrator::SecretCacheMode::Memory);
        resolver.set_test_vault_values(HashMap::from([(
            "API_KEY".to_string(),
            "fresh_memory_value".to_string(),
        )]));

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        resolver.resolve_parameters(&mut config).unwrap();
        assert_eq!(
            resolver.get_resolved_parameters().get("API_KEY").unwrap(),
            "fresh_memory_value"
        );
        assert!(
            !cache_path.exists(),
            "memory mode must remove the previous cache and never rewrite it"
        );
    }

    #[test]
    fn memory_cache_never_uses_an_existing_file_offline() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        crate::fed_dir::ensure_fed_dir(temp_dir.path()).unwrap();
        let cache_path = temp_dir.path().join(".fed/secrets.cache.env");
        std::fs::write(&cache_path, "API_KEY=must_not_be_used\n").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp_dir.path());
        resolver.set_offline(true);
        resolver.set_secret_cache(crate::orchestrator::SecretCacheMode::Memory);

        let mut config = Config::default();
        config.parameters.insert(
            "API_KEY".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );

        let err = resolver.resolve_parameters(&mut config).unwrap_err();
        assert!(
            err.to_string().contains("API_KEY"),
            "required manual secret must remain missing: {err}"
        );
        assert!(
            !cache_path.exists(),
            "memory mode must remove the previous cache even offline"
        );
        assert!(
            !err.to_string().contains("must_not_be_used"),
            "cached value must not leak through the error"
        );
    }

    // ── The round-3 rule: a deferred OR generatable secret is NEVER randomly
    //    generated (it must not fall through to the random-alphanumeric
    //    fallback and be persisted, where a later run would keep it). ─────

    #[test]
    fn deferred_or_generatable_secret_is_never_randomly_generated() {
        // SEED (manual) + DERIVED_SECRET (secret with a generator over SEED).
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let gen_path = temp.path().join(crate::fed_dir::GENERATED_SECRETS_REL);

        let make_config = |with_env: bool| {
            let mut config = Config::default();
            if with_env {
                config.env_file = vec!["seed.env".to_string()];
            }
            config.parameters.insert(
                "SEED".to_string(),
                Parameter {
                    param_type: Some("secret".to_string()),
                    source: Some("manual".to_string()),
                    ..Default::default()
                },
            );
            config.parameters.insert(
                "DERIVED_SECRET".to_string(),
                Parameter {
                    param_type: Some("secret".to_string()),
                    generate: Some("printf %s {{SEED}}".to_string()),
                    ..Default::default()
                },
            );
            config
        };

        // Scoped run: SEED out of scope → DERIVED_SECRET is deferred. It must
        // persist NOTHING — never a random value that a later run would keep
        // (generate.rs preserves existing secret values, which would poison it).
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp.path());
        resolver.set_offline(true);
        resolver.set_required_names(Some(HashSet::new()));
        let mut config = make_config(false);
        resolver
            .resolve_parameters(&mut config)
            .expect("a scoped run must defer the generated secret, not fail");
        assert!(
            !gen_path.exists(),
            "a deferred generated secret must persist nothing — no random value written"
        );

        // Unscoped run with SEED available: the generator runs but references
        // another secret, which under v6.2 semantics fails with ParameterNotFound.
        // It must surface that error, NOT silently fall back to a random value.
        std::fs::write(temp.path().join("seed.env"), "SEED=myseed\n").unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp.path());
        resolver.set_offline(true);
        let mut config = make_config(true);
        let err = resolver.resolve_parameters(&mut config).expect_err(
            "a generatable secret referencing another secret must fail, not be randomized",
        );
        assert!(
            matches!(err, Error::ParameterNotFound(ref n) if n == "SEED"),
            "must fail with ParameterNotFound(SEED), got: {err}"
        );
        assert!(
            !gen_path.exists(),
            "no random value must be persisted for the generatable secret"
        );
    }

    // ── RB-2: deferred params must skip port and `either` validation ─────

    #[test]
    fn scoped_run_skips_port_validation_and_allocation_for_deferred_param() {
        // A deferred port parameter (its default interpolates an out-of-scope
        // secret) must not be validated or allocated a port.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp.path());
        resolver.set_offline(true);
        resolver.set_required_names(Some(HashSet::new()));

        let mut config = Config::default();
        config.parameters.insert(
            "UNUSED_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "UNUSED_PORT".to_string(),
            Parameter {
                param_type: Some("port".to_string()),
                default: Some(serde_yaml::Value::String("{{UNUSED_SECRET}}".to_string())),
                ..Default::default()
            },
        );

        resolver
            .resolve_parameters(&mut config)
            .expect("a deferred port param must not be validated or allocated");
        assert!(
            !resolver
                .get_port_parameter_names()
                .contains(&"UNUSED_PORT".to_string()),
            "a deferred port param must not be tracked for allocation"
        );
        assert!(
            resolver
                .get_port_resolutions()
                .iter()
                .all(|r| r.param_name != "UNUSED_PORT"),
            "a deferred port param must allocate no port"
        );
        assert!(
            !resolver
                .get_resolved_parameters()
                .contains_key("UNUSED_PORT"),
            "a deferred port param must produce no resolved value"
        );
    }

    #[test]
    fn unscoped_run_still_fails_on_invalid_port_default() {
        // Control: with no scoping the same template port default is validated
        // and rejected — port strictness is unchanged.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        std::fs::write(temp.path().join("seed.env"), "UNUSED_SECRET=whatever\n").unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp.path());
        resolver.set_offline(true);

        let mut config = Config::default();
        config.env_file = vec!["seed.env".to_string()];
        config.parameters.insert(
            "UNUSED_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "UNUSED_PORT".to_string(),
            Parameter {
                param_type: Some("port".to_string()),
                default: Some(serde_yaml::Value::String("{{UNUSED_SECRET}}".to_string())),
                ..Default::default()
            },
        );

        let err = resolver
            .resolve_parameters(&mut config)
            .expect_err("an unscoped run must still reject the invalid port default");
        assert!(
            err.to_string().contains("invalid port default"),
            "got: {err}"
        );
    }

    #[test]
    fn scoped_run_skips_either_validation_for_deferred_param() {
        // A deferred `either`-constrained param (its default interpolates an
        // out-of-scope secret) must not be validated against its allowed values.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp.path());
        resolver.set_offline(true);
        resolver.set_required_names(Some(HashSet::new()));

        let mut config = Config::default();
        config.parameters.insert(
            "UNUSED_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "UNUSED_CHOICE".to_string(),
            Parameter {
                either: vec!["a".to_string(), "b".to_string()],
                default: Some(serde_yaml::Value::String("{{UNUSED_SECRET}}".to_string())),
                ..Default::default()
            },
        );

        resolver
            .resolve_parameters(&mut config)
            .expect("a deferred either-constrained param must not be validated");
    }

    #[test]
    fn unscoped_run_still_fails_on_either_constraint() {
        // Control: with no scoping the resolved value is validated against the
        // allowed set and rejected — `either` strictness is unchanged.
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        std::fs::write(temp.path().join("seed.env"), "UNUSED_SECRET=zzz\n").unwrap();
        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp.path());
        resolver.set_offline(true);

        let mut config = Config::default();
        config.env_file = vec!["seed.env".to_string()];
        config.parameters.insert(
            "UNUSED_SECRET".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "UNUSED_CHOICE".to_string(),
            Parameter {
                either: vec!["a".to_string(), "b".to_string()],
                default: Some(serde_yaml::Value::String("{{UNUSED_SECRET}}".to_string())),
                ..Default::default()
            },
        );

        let err = resolver
            .resolve_parameters(&mut config)
            .expect_err("an unscoped run must still reject the out-of-set value");
        assert!(
            err.to_string().contains("not in the allowed values"),
            "got: {err}"
        );
    }

    // ── Generated-secret interpolation semantics (v6.2) ─────────────────────────
    // The secret-generate DAG runs with an EMPTY resolved map: a reference-less
    // generator still generates and is preserved across runs; a generator that
    // references any parameter fails with ParameterNotFound. (A generator that
    // interpolates another secret — `derive {{SEED}}` — is a deferred follow-up.)

    fn generator_run_count(marker: &std::path::Path) -> usize {
        std::fs::read_to_string(marker)
            .map(|s| s.len())
            .unwrap_or(0)
    }

    #[test]
    fn referenceless_generated_secret_persists_and_is_preserved() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let marker = temp.path().join("gen-runs.log");
        let gen_path = temp.path().join(crate::fed_dir::GENERATED_SECRETS_REL);

        // A reference-less secret generator that records each execution via
        // `marker` and always prints the same value.
        let make_config = || {
            let mut config = Config::default();
            config.parameters.insert(
                "SECRET".to_string(),
                Parameter {
                    param_type: Some("secret".to_string()),
                    generate: Some(format!(
                        "printf x >> '{}'; printf %s fixedvalue",
                        marker.display()
                    )),
                    ..Default::default()
                },
            );
            config
        };

        // First run: generates and persists.
        let mut r1 = Resolver::new();
        r1.set_work_dir(temp.path());
        r1.set_offline(true);
        let mut c1 = make_config();
        r1.resolve_parameters(&mut c1)
            .expect("first run must succeed");
        assert_eq!(generator_run_count(&marker), 1, "first run generates");
        let persisted = std::fs::read_to_string(&gen_path).unwrap();
        assert!(
            persisted.contains("SECRET=fixedvalue"),
            "the reference-less secret must persist: {persisted}"
        );

        // Second run: the persisted value is preserved, the generator does NOT
        // rerun.
        let mut r2 = Resolver::new();
        r2.set_work_dir(temp.path());
        r2.set_offline(true);
        let mut c2 = make_config();
        r2.resolve_parameters(&mut c2)
            .expect("second run must succeed");
        assert_eq!(
            generator_run_count(&marker),
            1,
            "a reference-less generated secret must be preserved, not regenerated"
        );
    }

    #[test]
    fn secret_generator_referencing_param_fails_with_parameter_not_found() {
        use crate::config::{Config, Parameter};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        std::fs::write(temp.path().join("seed.env"), "SEED=abc\n").unwrap();

        let mut resolver = Resolver::new();
        resolver.set_work_dir(temp.path());
        resolver.set_offline(true);

        // DERIVED interpolates the manual secret SEED. With the empty DAG seed
        // (v6.2 semantics) it can never see SEED's value and must fail.
        let mut config = Config::default();
        config.env_file = vec!["seed.env".to_string()];
        config.parameters.insert(
            "SEED".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                source: Some("manual".to_string()),
                ..Default::default()
            },
        );
        config.parameters.insert(
            "DERIVED".to_string(),
            Parameter {
                param_type: Some("secret".to_string()),
                generate: Some("printf %s {{SEED}}".to_string()),
                ..Default::default()
            },
        );

        let err = resolver
            .resolve_parameters(&mut config)
            .expect_err("a secret generator referencing a parameter must fail (v6.2)");
        assert!(
            matches!(err, Error::ParameterNotFound(ref n) if n == "SEED"),
            "must fail with ParameterNotFound(SEED), got: {err}"
        );
    }
}
