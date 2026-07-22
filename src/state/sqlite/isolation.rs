// Split from sqlite.rs (see git history before this commit for pre-split blame).
use super::*;

impl SqliteStateTracker {
    /// Set isolation mode (enabled + optional isolation_id).
    /// If enabled and isolation_id is None, a new ID is generated.
    pub async fn set_isolation_mode(
        &self,
        enabled: bool,
        isolation_id: Option<String>,
    ) -> Result<()> {
        let id = if enabled {
            isolation_id.unwrap_or_else(|| format!("iso-{:08x}", rand::random::<u32>()))
        } else {
            String::new()
        };
        let enabled_str = enabled.to_string();
        self.conn
            .call(move |conn: &mut rusqlite::Connection| {
                let tx = conn.transaction()?;
                tx.execute(
                    "INSERT INTO project_settings (key, value, updated_at) VALUES ('isolation_enabled', ?1, datetime('now'))
                     ON CONFLICT(key) DO UPDATE SET value = ?1, updated_at = datetime('now')",
                    rusqlite::params![&enabled_str],
                )?;
                if enabled {
                    tx.execute(
                        "INSERT INTO project_settings (key, value, updated_at) VALUES ('isolation_id', ?1, datetime('now'))
                         ON CONFLICT(key) DO UPDATE SET value = ?1, updated_at = datetime('now')",
                        rusqlite::params![&id],
                    )?;
                } else {
                    tx.execute(
                        "DELETE FROM project_settings WHERE key = 'isolation_id'",
                        [],
                    )?;
                }
                tx.commit()?;
                Ok(())
            })
            .await
            .map_err(Error::from)
    }

    /// Get current isolation mode: (enabled, optional isolation_id).
    pub async fn get_isolation_mode(&self) -> (bool, Option<String>) {
        let result = self
            .conn
            .call(|conn: &mut rusqlite::Connection| {
                let enabled: Option<String> = conn
                    .query_row(
                        "SELECT value FROM project_settings WHERE key = 'isolation_enabled'",
                        [],
                        |row| row.get(0),
                    )
                    .optional()?;

                let isolation_id: Option<String> = conn
                    .query_row(
                        "SELECT value FROM project_settings WHERE key = 'isolation_id'",
                        [],
                        |row| row.get(0),
                    )
                    .optional()?;

                Ok((enabled, isolation_id))
            })
            .await;

        match result {
            Ok((Some(enabled_str), id)) => {
                let enabled = enabled_str == "true";
                (enabled, if enabled { id } else { None })
            }
            _ => (false, None),
        }
    }

    /// Clear isolation mode entirely.
    pub async fn clear_isolation_mode(&self) -> Result<()> {
        self.conn
            .call(|conn: &mut rusqlite::Connection| {
                conn.execute(
                    "DELETE FROM project_settings WHERE key IN ('isolation_enabled', 'isolation_id')",
                    [],
                )?;
                Ok(())
            })
            .await
            .map_err(Error::from)
    }
}
