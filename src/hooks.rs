use std::path::PathBuf;

use anyhow::Result;
use duckdb::Connection;

use crate::{compiler, config::Config};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookTarget {
    Main,
    Shadow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookPhase {
    Before,
    After,
}

pub fn run_hooks(
    connection: &Connection,
    config: &Config,
    phase: HookPhase,
    target: HookTarget,
) -> Result<()> {
    for hook_path in hook_paths(config, phase, target) {
        if hook_path.exists() {
            let sql = compiler::compile_file(config, &hook_path)?;
            if !sql.trim().is_empty() {
                connection.execute_batch(&sql)?;
            }
        }
    }

    Ok(())
}

fn hook_paths(config: &Config, phase: HookPhase, target: HookTarget) -> [PathBuf; 2] {
    let hooks_dir = config.migrations_dir.join("hooks");
    let phase_name = match phase {
        HookPhase::Before => "before",
        HookPhase::After => "after",
    };
    let target_name = match target {
        HookTarget::Main => "main",
        HookTarget::Shadow => "shadow",
    };

    [
        hooks_dir.join(format!("{phase_name}-all.sql")),
        hooks_dir.join(format!("{phase_name}-{target_name}.sql")),
    ]
}
