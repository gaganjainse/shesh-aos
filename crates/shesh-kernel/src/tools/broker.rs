use std::{collections::HashMap, sync::Arc};

use tracing::{error, info, warn};

use super::executor::{ToolExecutor, ToolRequest, ToolResult};
use crate::{
    error::ToolError,
    policy::{PolicyDecision, PolicyEngine},
};

/// The result of a broker dispatch.
#[derive(Debug)]
pub enum BrokerResult {
    /// Tool executed successfully.
    Completed(ToolResult),
    /// Tool requires confirmation before execution.
    RequiresConfirmation(String),
    /// Tool was denied by policy.
    Denied(String),
}

/// Dispatches tool calls through policy checks and logging.
pub struct ToolBroker {
    executors: HashMap<String, Arc<dyn ToolExecutor>>,
    policy: Arc<PolicyEngine>,
}

impl ToolBroker {
    pub fn new(policy: Arc<PolicyEngine>) -> Self {
        Self { executors: HashMap::new(), policy }
    }

    /// Register a tool executor.
    pub fn register(&mut self, executor: Arc<dyn ToolExecutor>) {
        let name = executor.name().to_string();
        self.executors.insert(name, executor);
    }

    /// Execute a tool call with policy checks.
    /// Returns PolicyDecision if confirmation is needed, or executes if allowed.
    pub async fn execute(&self, request: &ToolRequest) -> Result<BrokerResult, ToolError> {
        let action = request
            .arguments
            .get("action")
            .and_then(|v| v.as_str())
            .map(|a| format!("{}.{}", request.tool_name, a))
            .unwrap_or_else(|| format!("{}.execute", request.tool_name));
        let decision = self.policy.evaluate(&action);

        info!(tool = %request.tool_name, action = %action, "Tool execution requested");

        match decision {
            PolicyDecision::Deny(reason) => {
                warn!(tool = %request.tool_name, reason = %reason, "Tool execution denied by policy");
                Ok(BrokerResult::Denied(reason))
            }
            PolicyDecision::RequireConfirmation(reason) => {
                info!(tool = %request.tool_name, reason = %reason, "Tool execution requires confirmation");
                Ok(BrokerResult::RequiresConfirmation(reason))
            }
            PolicyDecision::Allow => {
                let executor = self.executors.get(&request.tool_name).ok_or_else(|| {
                    error!(tool = %request.tool_name, "Tool not found");
                    ToolError::NotFound { name: request.tool_name.clone() }
                })?;

                info!(tool = %request.tool_name, "Executing tool");
                let result = executor.execute(request).await?;
                info!(tool = %request.tool_name, success = %result.success, "Tool execution completed");
                Ok(BrokerResult::Completed(result))
            }
        }
    }

    /// Execute a request after an explicit approval.
    ///
    /// The policy is evaluated again and the executor is reached only when
    /// the policy still requires confirmation. This makes approval a separate
    /// governed path rather than a direct executor bypass.
    pub async fn execute_confirmed(&self, request: &ToolRequest) -> Result<ToolResult, ToolError> {
        let action = request
            .arguments
            .get("action")
            .and_then(|value| value.as_str())
            .map(|action| format!("{}.{}", request.tool_name, action))
            .unwrap_or_else(|| format!("{}.execute", request.tool_name));

        match self.policy.evaluate(&action) {
            PolicyDecision::RequireConfirmation(_) => {
                let executor = self
                    .executors
                    .get(&request.tool_name)
                    .ok_or_else(|| ToolError::NotFound { name: request.tool_name.clone() })?;
                info!(tool = %request.tool_name, action = %action, "Executing confirmed tool request");
                executor.execute(request).await
            }
            PolicyDecision::Allow => Err(ToolError::ExecutionFailed {
                name: request.tool_name.clone(),
                reason: "Tool does not require confirmation".to_string(),
            }),
            PolicyDecision::Deny(reason) => Err(ToolError::ExecutionFailed {
                name: request.tool_name.clone(),
                reason: format!("Tool denied: {}", reason),
            }),
        }
    }

    /// List available tools.
    pub fn available_tools(&self) -> Vec<String> {
        self.executors.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use serde_json::json;

    use super::*;
    use crate::policy::{PolicyRule, TrustTier};

    struct DummyTool;
    #[async_trait]
    impl ToolExecutor for DummyTool {
        fn name(&self) -> &str {
            "dummy"
        }
        fn description(&self) -> &str {
            "dummy"
        }
        fn is_destructive(&self) -> bool {
            false
        }
        async fn execute(&self, _req: &ToolRequest) -> Result<ToolResult, ToolError> {
            Ok(ToolResult { success: true, output: "ok".to_string(), data: None })
        }
    }

    #[tokio::test]
    async fn test_broker() {
        let rule = PolicyRule {
            name: "allow-dummy".to_string(),
            action_pattern: "dummy.execute".to_string(),
            decision: "allow".to_string(),
            trust_tier: 0,
            description: None,
        };
        let policy = PolicyEngine::new(vec![rule], TrustTier::Basic);

        let mut broker = ToolBroker::new(Arc::new(policy));
        broker.register(Arc::new(DummyTool));

        let req = ToolRequest { tool_name: "dummy".to_string(), arguments: json!({}) };

        let res = broker.execute(&req).await.unwrap();
        match res {
            BrokerResult::Completed(tr) => assert_eq!(tr.output, "ok"),
            _ => panic!("Expected Completed"),
        }
    }

    #[tokio::test]
    async fn test_broker_deny_policy() {
        let rule = PolicyRule {
            name: "deny-dummy".to_string(),
            action_pattern: "dummy.execute".to_string(),
            decision: "deny".to_string(),
            trust_tier: 0,
            description: None,
        };
        let policy = PolicyEngine::new(vec![rule], TrustTier::Basic);
        let mut broker = ToolBroker::new(Arc::new(policy));
        broker.register(Arc::new(DummyTool));

        let req = ToolRequest { tool_name: "dummy".to_string(), arguments: json!({}) };
        let res = broker.execute(&req).await.unwrap();
        match res {
            BrokerResult::Denied(reason) => {
                assert!(reason.contains("deny") || reason.contains("Deny"))
            }
            _ => panic!("Expected Denied"),
        }
    }

    #[tokio::test]
    async fn test_broker_require_confirmation_policy() {
        let rule = PolicyRule {
            name: "confirm-dummy".to_string(),
            action_pattern: "dummy.execute".to_string(),
            decision: "require_confirmation".to_string(),
            trust_tier: 0,
            description: None,
        };
        let policy = PolicyEngine::new(vec![rule], TrustTier::Basic);
        let mut broker = ToolBroker::new(Arc::new(policy));
        broker.register(Arc::new(DummyTool));

        let req = ToolRequest { tool_name: "dummy".to_string(), arguments: json!({}) };
        let res = broker.execute(&req).await.unwrap();
        match res {
            BrokerResult::RequiresConfirmation(reason) => assert!(!reason.is_empty()),
            _ => panic!("Expected RequiresConfirmation"),
        }
    }

    #[tokio::test]
    async fn confirmed_request_executes_only_after_confirmation() {
        let rule = PolicyRule {
            name: "confirm-dummy".to_string(),
            action_pattern: "dummy.execute".to_string(),
            decision: "require_confirmation".to_string(),
            trust_tier: 0,
            description: None,
        };
        let mut broker = ToolBroker::new(Arc::new(PolicyEngine::new(vec![rule], TrustTier::Basic)));
        broker.register(Arc::new(DummyTool));
        let request = ToolRequest { tool_name: "dummy".to_string(), arguments: json!({}) };

        assert!(matches!(
            broker.execute(&request).await.unwrap(),
            BrokerResult::RequiresConfirmation(_)
        ));
        assert_eq!(broker.execute_confirmed(&request).await.unwrap().output, "ok");
    }

    #[tokio::test]
    async fn test_broker_unknown_tool() {
        let rule = PolicyRule {
            name: "allow-all".to_string(),
            action_pattern: "*".to_string(),
            decision: "allow".to_string(),
            trust_tier: 0,
            description: None,
        };
        let policy = PolicyEngine::new(vec![rule], TrustTier::Basic);
        let mut broker = ToolBroker::new(Arc::new(policy));
        broker.register(Arc::new(DummyTool));

        let req = ToolRequest { tool_name: "nonexistent".to_string(), arguments: json!({}) };
        let err = broker.execute(&req).await.unwrap_err();
        match err {
            ToolError::NotFound { name } => assert_eq!(name, "nonexistent"),
            _ => panic!("Expected NotFound"),
        }
    }

    #[tokio::test]
    async fn test_broker_available_tools() {
        let policy = PolicyEngine::deny_all();
        let mut broker = ToolBroker::new(Arc::new(policy));
        broker.register(Arc::new(DummyTool));

        struct AnotherTool;
        #[async_trait]
        impl ToolExecutor for AnotherTool {
            fn name(&self) -> &str {
                "another"
            }
            fn description(&self) -> &str {
                "another tool"
            }
            fn is_destructive(&self) -> bool {
                false
            }
            async fn execute(&self, _req: &ToolRequest) -> Result<ToolResult, ToolError> {
                Ok(ToolResult { success: true, output: "ok".into(), data: None })
            }
        }

        broker.register(Arc::new(AnotherTool));
        let tools = broker.available_tools();
        assert_eq!(tools.len(), 2);
        assert!(tools.contains(&"dummy".to_string()));
        assert!(tools.contains(&"another".to_string()));
    }

    #[tokio::test]
    async fn test_broker_no_registered_tools() {
        let policy = PolicyEngine::deny_all();
        let broker = ToolBroker::new(Arc::new(policy));
        assert!(broker.available_tools().is_empty());
    }

    #[tokio::test]
    async fn test_broker_deny_all_engine() {
        let policy = PolicyEngine::deny_all();
        let mut broker = ToolBroker::new(Arc::new(policy));
        broker.register(Arc::new(DummyTool));

        let req = ToolRequest { tool_name: "dummy".to_string(), arguments: json!({}) };
        let res = broker.execute(&req).await.unwrap();
        match res {
            BrokerResult::Denied(_) => {}
            _ => panic!("Expected Denied with deny_all policy"),
        }
    }
}
