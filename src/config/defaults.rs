//! Global fallback values, applied after extension resolution so template
//! values retain precedence. Infrastructure named in defaults.depends_on is
//! excluded, and explicit empty dependencies opt out of inheritance.

use super::Config;
use crate::error::Result;
use crate::package::ServiceMerger;
use std::collections::BTreeSet;

/// Services excluded from defaults, shared by merging and validate's report.
pub fn excluded_services(config: &Config) -> BTreeSet<String> {
    config
        .defaults
        .iter()
        .flat_map(|defaults| defaults.depends_on.iter())
        .map(|dep| dep.service_name().to_string())
        .filter(|name| config.services.contains_key(name))
        .collect()
}

/// Fill fields left unset after templates/packages, then expose the same
/// defaults on templates. Doing the template pass last prevents infrastructure
/// from indirectly inheriting defaults through an extended template.
pub fn apply(config: &mut Config) -> Result<()> {
    let Some(defaults) = config.defaults.clone() else {
        return Ok(());
    };
    let excluded = excluded_services(config);
    for (name, service) in &mut config.services {
        if excluded.contains(name) {
            continue;
        }
        // Applying defaults does not satisfy an unresolved extends reference.
        let extends = service.extends.clone();
        ServiceMerger::merge_service(service, &defaults)?;
        service.extends = extends;
    }
    for template in config.templates.values_mut() {
        let extends = template.extends.clone();
        ServiceMerger::merge_service(template, &defaults)?;
        template.extends = extends;
    }
    Ok(())
}
