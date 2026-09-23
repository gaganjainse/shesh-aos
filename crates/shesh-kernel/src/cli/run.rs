//! `shesh run` — Submit a task for execution.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use tokio::sync::RwLock;
use tracing::info;

use crate::{
    config::{AppConfig, PolicyConfig},
    error::KernelError,
    model::{openai_compat::OpenAiCompatProvider, registry::ProviderRegistry},
    policy::{PolicyEngine, PolicyRule, TrustTier},
    runtime::kernel::Kernel,
    storage::SqliteEventStore,
    task::TaskInput,
    tools::{broker::ToolBroker, filesystem::FilesystemTool, git::GitTool, terminal::TerminalTool},
};

/// Execute a task through the kernel.
pub fn execute(
    config_path: &str,
    task: &str,
    background: bool,
    yes: bool,
) -> Result<(), KernelError> {
    info!(task = task, background = background, "Submitting task");

    let config = AppConfig::load(config_path)?;
    let data_dir = config.resolved_data_dir();

    let rt = tokio::runtime::Runtime::new().map_err(|e| {
        KernelError::Config(crate::error::ConfigError::Invalid { message: e.to_string() })
    })?;
    rt.block_on(async {
        // 1. Initialize Event Store
        let events_dir = data_dir.join("events");
        let store = Arc::new(SqliteEventStore::open(events_dir).await?);

        // 2. Initialize Policy Engine
        let rules = policy_rules(&config.policy, yes);

        let trust_tier = if yes { TrustTier::Autonomous } else { TrustTier::Basic };
        let policy = PolicyEngine::new(rules, trust_tier);
        let policy_arc = Arc::new(policy.clone());

        // 3. Initialize Model Registry
        let mut registry = ProviderRegistry::new();
        for p_cfg in &config.model_providers {
            if let Ok(provider) = OpenAiCompatProvider::new(p_cfg) {
                registry.register(Box::new(provider));
            }
        }
        let registry = Arc::new(registry);

        // 4. Initialize Tool Broker
        let mut broker = ToolBroker::new(policy_arc);
        let allowed_paths = filesystem_allowed_paths(&config, &data_dir);
        broker.register(Arc::new(FilesystemTool::new(
            allowed_paths,
            config.tools.filesystem.denied_patterns.clone(),
        )));
        if config.tools.git.enabled {
            broker.register(Arc::new(GitTool::new(data_dir.clone())));
        }
        broker.register(Arc::new(TerminalTool::new(
            config.tools.terminal.timeout_secs,
            config.tools.terminal.denied_prefixes.clone(),
        )));
        let broker = Arc::new(broker);

        // 5. Initialize Kernel
        let kernel = Kernel::new(
            store,
            Arc::new(RwLock::new(policy)),
            registry,
            broker,
            config.resource_limits.max_tool_output_size,
        )
        .await?;

        // 6. Submit Task
        println!("Submitting task: {}", task);
        let task_input = TaskInput::Text(task.to_string());
        let task_id = kernel.submit_task(task_input).await?;
        println!("Task ID: {}", task_id);

        if background {
            println!("Task submitted to background.");
            return Ok(());
        }

        // 7. Execute Task
        println!("Executing task...");
        match kernel.execute_task(&task_id).await {
            Ok(outcome) => {
                println!("\nTask Execution Summary:");
                println!("  Status:  {}", if outcome.success { "Success" } else { "Failed" });
                if let Some(out) = &outcome.output {
                    println!("  Output:  {}", out);
                }
                if let Some(err) = &outcome.error {
                    println!("  Error:   {}", err);
                }
                println!("  Time:    {}", outcome.completed_at);
            }
            Err(e) => {
                println!("\nTask execution failed: {}", e);
            }
        }

        // Note: Interactive confirmation for tools is handled by the kernel,
        // which transitions the task to AwaitingConfirmation state. The CLI
        // can then prompt the user and resume execution via confirm_task.

        Ok::<(), KernelError>(())
    })
}

/// Build the policy used by `shesh run` from the user's configuration.
///
/// Read-only operations are always allowed. Mutating operations require
/// confirmation unless the corresponding configuration setting disables it or
/// the user explicitly supplied `--yes` for this invocation. Actions not
/// listed here remain denied by the policy engine's deny-by-default fallback.
fn policy_rules(config: &PolicyConfig, yes: bool) -> Vec<PolicyRule> {
    let mut rules = vec![
        rule("allow-filesystem-read", "filesystem.read_*", "allow"),
        rule("allow-filesystem-list", "filesystem.list_*", "allow"),
        rule("allow-git-status", "git.status", "allow"),
        rule("allow-git-diff", "git.diff", "allow"),
        rule("allow-git-log", "git.log", "allow"),
    ];

    rules.push(rule(
        "filesystem-write",
        "filesystem.write_*",
        decision_for(config.confirm_writes, yes),
    ));
    rules.push(rule(
        "filesystem-delete",
        "filesystem.delete_*",
        decision_for(config.confirm_destructive, yes),
    ));
    rules.push(rule("git-add", "git.add", decision_for(config.confirm_destructive, yes)));
    rules.push(rule("git-commit", "git.commit", decision_for(config.confirm_git_commits, yes)));
    rules.push(rule(
        "terminal-execute",
        "terminal.execute",
        decision_for(config.confirm_terminal, yes),
    ));
    rules
}

fn decision_for(requires_confirmation: bool, yes: bool) -> &'static str {
    if requires_confirmation && !yes {
        "require_confirmation"
    } else {
        "allow"
    }
}

fn rule(name: &str, action_pattern: &str, decision: &str) -> PolicyRule {
    PolicyRule {
        name: name.to_string(),
        action_pattern: action_pattern.to_string(),
        decision: decision.to_string(),
        trust_tier: 0,
        description: None,
    }
}

/// Resolve filesystem roots once at startup so the tool receives absolute,
/// configuration-derived paths instead of an implicit data-directory-only scope.
fn filesystem_allowed_paths(config: &AppConfig, data_dir: &Path) -> Vec<PathBuf> {
    let configured = &config.tools.filesystem.allowed_paths;
    if configured.is_empty() {
        return vec![data_dir.to_path_buf()];
    }

    let cwd = std::env::current_dir().unwrap_or_else(|_| data_dir.to_path_buf());
    configured
        .iter()
        .map(|path| match path.strip_prefix("~/") {
            Some(relative) => std::env::var_os("HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| cwd.clone())
                .join(relative),
            None => {
                let path = PathBuf::from(path);
                if path.is_absolute() {
                    path
                } else {
                    cwd.join(path)
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn confirmation_config() -> PolicyConfig {
        PolicyConfig {
            confirm_destructive: true,
            confirm_writes: true,
            confirm_git_commits: true,
            confirm_terminal: true,
            dedup_window_secs: 5,
        }
    }

    #[test]
    fn default_run_policy_allows_reads_and_confirms_mutations() {
        let policy =
            PolicyEngine::new(policy_rules(&confirmation_config(), false), TrustTier::Basic);

        assert!(policy.evaluate("filesystem.read_file").is_allowed());
        assert!(policy.evaluate("git.status").is_allowed());
        assert!(policy.evaluate("filesystem.write_file").requires_confirmation());
        assert!(policy.evaluate("filesystem.delete_file").requires_confirmation());
        assert!(policy.evaluate("git.add").requires_confirmation());
        assert!(policy.evaluate("git.commit").requires_confirmation());
        assert!(policy.evaluate("terminal.execute").requires_confirmation());
        assert!(policy.evaluate("remote.execute").is_denied());
    }

    #[test]
    fn yes_flag_allows_only_known_mutating_actions() {
        let policy =
            PolicyEngine::new(policy_rules(&confirmation_config(), true), TrustTier::Autonomous);

        assert!(policy.evaluate("filesystem.write_file").is_allowed());
        assert!(policy.evaluate("filesystem.delete_file").is_allowed());
        assert!(policy.evaluate("git.commit").is_allowed());
        assert!(policy.evaluate("terminal.execute").is_allowed());
        assert!(policy.evaluate("unknown.action").is_denied());
    }

    #[test]
    fn filesystem_roots_default_to_data_directory_when_unconfigured() {
        let mut config = AppConfig::parse_toml(include_str!("../../../../configs/default.toml"))
            .expect("default configuration parses");
        config.tools.filesystem.allowed_paths.clear();
        let data_dir = PathBuf::from("/tmp/shesh-data");

        assert_eq!(filesystem_allowed_paths(&config, &data_dir), vec![data_dir]);
    }
}
