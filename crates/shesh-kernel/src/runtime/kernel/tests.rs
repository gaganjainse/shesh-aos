use std::path::PathBuf;
use std::sync::Mutex;

use async_trait::async_trait;

use super::*;
use crate::{
    model::{
        provider::ModelProvider,
        types::{CompletionRequest, CompletionResponse},
    },
    policy::TrustTier,
    tools::executor::{ToolExecutor, ToolRequest, ToolResult},
};

struct ConfirmationTool;

#[async_trait]
impl ToolExecutor for ConfirmationTool {
    fn name(&self) -> &str {
        "dummy"
    }
    fn description(&self) -> &str {
        "confirmation test tool"
    }
    fn is_destructive(&self) -> bool {
        true
    }
    async fn execute(&self, _request: &ToolRequest) -> Result<ToolResult, crate::error::ToolError> {
        Ok(ToolResult { success: true, output: "executed".to_string(), data: None })
    }
}

struct MockProvider {
    role: crate::state::ModelRole,
    content: String,
}
#[async_trait]
impl ModelProvider for MockProvider {
    fn name(&self) -> &str {
        "mock"
    }
    fn role(&self) -> crate::state::ModelRole {
        self.role
    }
    fn max_context(&self) -> usize {
        100
    }
    fn supports_vision(&self) -> bool {
        false
    }
    async fn health_check(&self) -> Result<bool, crate::error::ProviderError> {
        Ok(true)
    }
    async fn complete(
        &self,
        _r: CompletionRequest,
    ) -> Result<CompletionResponse, crate::error::ProviderError> {
        Ok(CompletionResponse {
            content: self.content.clone(),
            finish_reason: None,
            prompt_tokens: None,
            completion_tokens: None,
            model: "mock".into(),
        })
    }
    async fn cancel(&self) -> Result<(), crate::error::ProviderError> {
        Ok(())
    }
}

struct MockEventStore {
    events: Mutex<Vec<Event>>,
}

impl MockEventStore {
    fn new() -> Self {
        Self { events: Mutex::new(Vec::new()) }
    }
}

#[async_trait]
impl EventStore for MockEventStore {
    async fn append(&self, event: Event) -> Result<(), KernelError> {
        self.events.lock().unwrap().push(event);
        Ok(())
    }
    async fn get_all_events(&self) -> Result<Vec<Event>, KernelError> {
        Ok(self.events.lock().unwrap().clone())
    }
    async fn get_task_events(&self, task_id: &TaskId) -> Result<Vec<Event>, KernelError> {
        Ok(self
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.task_id == Some(*task_id))
            .cloned()
            .collect())
    }
    async fn read_since(&self, _sequence: u64) -> Result<Vec<Event>, KernelError> {
        Ok(self.events.lock().unwrap().clone())
    }
}

#[tokio::test]
async fn test_submit_task_allowed() {
    let store = Arc::new(MockEventStore::new());
    let rule = crate::policy::PolicyRule {
        name: "allow".into(),
        action_pattern: "*".into(),
        decision: "allow".into(),
        trust_tier: 0,
        description: None,
    };
    let policy = PolicyEngine::new(vec![rule], TrustTier::Autonomous);
    let registry = Arc::new(ProviderRegistry::new());
    let broker = Arc::new(ToolBroker::new(Arc::new(policy.clone())));
    let kernel = Kernel::new(store, Arc::new(RwLock::new(policy)), registry, broker, 1_048_576)
        .await
        .unwrap();

    let id = kernel.submit_task(TaskInput::Text("test".into())).await.unwrap();
    let state = kernel.task_state(&id).await.unwrap();
    assert_eq!(state, TaskState::Classified);
}

#[tokio::test]
async fn test_submit_task_denied() {
    let store = Arc::new(MockEventStore::new());
    let policy = PolicyEngine::deny_all();
    let registry = Arc::new(ProviderRegistry::new());
    let broker = Arc::new(ToolBroker::new(Arc::new(policy.clone())));
    let kernel = Kernel::new(store, Arc::new(RwLock::new(policy)), registry, broker, 1_048_576)
        .await
        .unwrap();

    let result = kernel.submit_task(TaskInput::Text("test".into())).await;
    assert!(matches!(result, Err(KernelError::Policy(_))));
}

#[tokio::test]
async fn confirmation_executes_the_pending_tool_after_approval() {
    let store = Arc::new(MockEventStore::new());
    let policy = PolicyEngine::new(
        vec![
            crate::policy::PolicyRule {
                name: "allow-task".into(),
                action_pattern: "task.create".into(),
                decision: "allow".into(),
                trust_tier: 0,
                description: None,
            },
            crate::policy::PolicyRule {
                name: "confirm-tool".into(),
                action_pattern: "dummy.execute".into(),
                decision: "require_confirmation".into(),
                trust_tier: 0,
                description: None,
            },
        ],
        TrustTier::Basic,
    );
    let mut registry = ProviderRegistry::new();
    registry.register(Box::new(MockProvider {
        role: crate::state::ModelRole::Planner,
        content: "TOOL: dummy {}".into(),
    }));
    let mut broker = ToolBroker::new(Arc::new(policy.clone()));
    broker.register(Arc::new(ConfirmationTool));
    let kernel = Kernel::new(
        store,
        Arc::new(RwLock::new(policy)),
        Arc::new(registry),
        Arc::new(broker),
        1_048_576,
    )
    .await
    .unwrap();

    let task_id = kernel.submit_task(TaskInput::Text("hello".into())).await.unwrap();
    let pending = kernel.execute_task(&task_id).await.unwrap();
    assert!(pending.requires_confirmation, "unexpected outcome: {pending:?}");
    assert_eq!(kernel.task_state(&task_id).await.unwrap(), TaskState::AwaitingConfirmation);

    let completed = kernel.confirm_task(&task_id).await.unwrap();
    assert!(completed.success);
    assert_eq!(kernel.task_state(&task_id).await.unwrap(), TaskState::Completed);
}

#[tokio::test]
async fn test_task_transition() {
    let store = Arc::new(MockEventStore::new());
    let rule = crate::policy::PolicyRule {
        name: "allow".into(),
        action_pattern: "*".into(),
        decision: "allow".into(),
        trust_tier: 0,
        description: None,
    };
    let policy = PolicyEngine::new(vec![rule], TrustTier::Autonomous);
    let registry = Arc::new(ProviderRegistry::new());
    let broker = Arc::new(ToolBroker::new(Arc::new(policy.clone())));
    let kernel = Kernel::new(store, Arc::new(RwLock::new(policy)), registry, broker, 1_048_576)
        .await
        .unwrap();

    let id = kernel.submit_task(TaskInput::Text("test".into())).await.unwrap();
    kernel.transition_task(&id, TaskState::Planned).await.unwrap();
    assert_eq!(kernel.task_state(&id).await.unwrap(), TaskState::Planned);
}

#[tokio::test]
async fn test_invalid_task_transition() {
    let store = Arc::new(MockEventStore::new());
    let rule = crate::policy::PolicyRule {
        name: "allow".into(),
        action_pattern: "*".into(),
        decision: "allow".into(),
        trust_tier: 0,
        description: None,
    };
    let policy = PolicyEngine::new(vec![rule], TrustTier::Autonomous);
    let registry = Arc::new(ProviderRegistry::new());
    let broker = Arc::new(ToolBroker::new(Arc::new(policy.clone())));
    let kernel = Kernel::new(store, Arc::new(RwLock::new(policy)), registry, broker, 1_048_576)
        .await
        .unwrap();

    let id = kernel.submit_task(TaskInput::Text("test".into())).await.unwrap();
    // Classified -> Completed is invalid
    let result = kernel.transition_task(&id, TaskState::Completed).await;
    assert!(matches!(result, Err(KernelError::Task(_))));
}

#[tokio::test]
async fn test_execute_task() {
    let store = Arc::new(MockEventStore::new());
    let rule = crate::policy::PolicyRule {
        name: "allow".into(),
        action_pattern: "*".into(),
        decision: "allow".into(),
        trust_tier: 0,
        description: None,
    };
    let policy = PolicyEngine::new(vec![rule], TrustTier::Autonomous);

    let mut registry = ProviderRegistry::new();
    registry.register(Box::new(MockProvider {
        role: crate::state::ModelRole::Planner,
        content: "Need to write some code to fix this. TOOL: dummy".into(),
    }));
    registry.register(Box::new(MockProvider {
        role: crate::state::ModelRole::Coder,
        content: "Here is the code.".into(),
    }));
    registry.register(Box::new(MockProvider {
        role: crate::state::ModelRole::Reviewer,
        content: "Looks good.".into(),
    }));

    let broker = Arc::new(ToolBroker::new(Arc::new(policy.clone())));
    let kernel =
        Kernel::new(store, Arc::new(RwLock::new(policy)), Arc::new(registry), broker, 1_048_576)
            .await
            .unwrap();

    let id = kernel.submit_task(TaskInput::Text("fix this".into())).await.unwrap();

    // Execute task should run Planner -> Coder -> Reviewer
    let outcome = kernel.execute_task(&id).await.unwrap();
    assert!(outcome.success);
    let final_output = outcome.output.unwrap();
    assert!(final_output.contains("Here is the code."));
    assert!(final_output.contains("Review: Looks good."));

    let state = kernel.task_state(&id).await.unwrap();
    assert_eq!(state, TaskState::Completed);
}

#[tokio::test]
async fn test_task_state_not_found() {
    let store = Arc::new(MockEventStore::new());
    let rule = crate::policy::PolicyRule {
        name: "allow".into(),
        action_pattern: "*".into(),
        decision: "allow".into(),
        trust_tier: 0,
        description: None,
    };
    let policy = PolicyEngine::new(vec![rule], TrustTier::Autonomous);
    let registry = Arc::new(ProviderRegistry::new());
    let broker = Arc::new(ToolBroker::new(Arc::new(policy.clone())));
    let kernel = Kernel::new(store, Arc::new(RwLock::new(policy)), registry, broker, 1_048_576)
        .await
        .unwrap();

    let fake_id = TaskId::new();
    let result = kernel.task_state(&fake_id).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        KernelError::Task(TaskError::NotFound { .. }) => {}
        _ => panic!("Expected TaskNotFound"),
    }
}

#[tokio::test]
async fn test_tasks_in_state() {
    let store = Arc::new(MockEventStore::new());
    let rule = crate::policy::PolicyRule {
        name: "allow".into(),
        action_pattern: "*".into(),
        decision: "allow".into(),
        trust_tier: 0,
        description: None,
    };
    let policy = PolicyEngine::new(vec![rule], TrustTier::Autonomous);
    let registry = Arc::new(ProviderRegistry::new());
    let broker = Arc::new(ToolBroker::new(Arc::new(policy.clone())));
    let kernel = Kernel::new(store, Arc::new(RwLock::new(policy)), registry, broker, 1_048_576)
        .await
        .unwrap();

    let id1 = kernel.submit_task(TaskInput::Text("task1".into())).await.unwrap();
    let id2 = kernel.submit_task(TaskInput::Text("task2".into())).await.unwrap();

    let classified = kernel.tasks_in_state(&TaskState::Classified).await;
    assert_eq!(classified.len(), 2);
    assert!(classified.contains(&id1));
    assert!(classified.contains(&id2));

    let received = kernel.tasks_in_state(&TaskState::Received).await;
    assert!(received.is_empty());
}

#[tokio::test]
async fn test_task_count() {
    let store = Arc::new(MockEventStore::new());
    let rule = crate::policy::PolicyRule {
        name: "allow".into(),
        action_pattern: "*".into(),
        decision: "allow".into(),
        trust_tier: 0,
        description: None,
    };
    let policy = PolicyEngine::new(vec![rule], TrustTier::Autonomous);
    let registry = Arc::new(ProviderRegistry::new());
    let broker = Arc::new(ToolBroker::new(Arc::new(policy.clone())));
    let kernel = Kernel::new(store, Arc::new(RwLock::new(policy)), registry, broker, 1_048_576)
        .await
        .unwrap();

    assert_eq!(kernel.task_count().await, 0);
    kernel.submit_task(TaskInput::Text("t1".into())).await.unwrap();
    assert_eq!(kernel.task_count().await, 1);
    kernel.submit_task(TaskInput::Text("t2".into())).await.unwrap();
    assert_eq!(kernel.task_count().await, 2);
}

#[tokio::test]
async fn test_transition_task_not_found() {
    let store = Arc::new(MockEventStore::new());
    let rule = crate::policy::PolicyRule {
        name: "allow".into(),
        action_pattern: "*".into(),
        decision: "allow".into(),
        trust_tier: 0,
        description: None,
    };
    let policy = PolicyEngine::new(vec![rule], TrustTier::Autonomous);
    let registry = Arc::new(ProviderRegistry::new());
    let broker = Arc::new(ToolBroker::new(Arc::new(policy.clone())));
    let kernel = Kernel::new(store, Arc::new(RwLock::new(policy)), registry, broker, 1_048_576)
        .await
        .unwrap();

    let fake_id = TaskId::new();
    let result = kernel.transition_task(&fake_id, TaskState::Planned).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_execute_task_no_planner() {
    let store = Arc::new(MockEventStore::new());
    let rule = crate::policy::PolicyRule {
        name: "allow".into(),
        action_pattern: "*".into(),
        decision: "allow".into(),
        trust_tier: 0,
        description: None,
    };
    let policy = PolicyEngine::new(vec![rule], TrustTier::Autonomous);
    let registry = Arc::new(ProviderRegistry::new());
    let broker = Arc::new(ToolBroker::new(Arc::new(policy.clone())));
    let kernel = Kernel::new(store, Arc::new(RwLock::new(policy)), registry, broker, 1_048_576)
        .await
        .unwrap();

    let id = kernel.submit_task(TaskInput::Text("do something".into())).await.unwrap();
    let result = kernel.execute_task(&id).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        KernelError::Provider(crate::error::ProviderError::Unavailable { .. }) => {}
        _ => panic!("Expected Provider Unavailable"),
    }
}

#[tokio::test]
async fn test_submit_task_events_emitted() {
    let store = Arc::new(MockEventStore::new());
    let rule = crate::policy::PolicyRule {
        name: "allow".into(),
        action_pattern: "*".into(),
        decision: "allow".into(),
        trust_tier: 0,
        description: None,
    };
    let policy = PolicyEngine::new(vec![rule], TrustTier::Autonomous);
    let registry = Arc::new(ProviderRegistry::new());
    let broker = Arc::new(ToolBroker::new(Arc::new(policy.clone())));
    let kernel =
        Kernel::new(store.clone(), Arc::new(RwLock::new(policy)), registry, broker, 1_048_576)
            .await
            .unwrap();

    let _id = kernel.submit_task(TaskInput::Text("test".into())).await.unwrap();

    let events = store.get_all_events().await.unwrap();
    // Should have at least TaskCreated and TaskClassified events
    assert!(events.len() >= 2);
    let kinds: Vec<_> = events.iter().map(|e| &e.kind).collect();
    assert!(kinds.contains(&&EventKind::TaskCreated));
    assert!(kinds.contains(&&EventKind::TaskClassified));
}

#[tokio::test]
async fn test_execute_task_planner_only_no_code() {
    let store = Arc::new(MockEventStore::new());
    let rule = crate::policy::PolicyRule {
        name: "allow".into(),
        action_pattern: "*".into(),
        decision: "allow".into(),
        trust_tier: 0,
        description: None,
    };
    let policy = PolicyEngine::new(vec![rule], TrustTier::Autonomous);

    let mut registry = ProviderRegistry::new();
    registry.register(Box::new(MockProvider {
        role: crate::state::ModelRole::Planner,
        content: "Here is the architectural plan. No implementation required.".into(),
    }));

    let broker = Arc::new(ToolBroker::new(Arc::new(policy.clone())));
    let kernel = Kernel::new(
        store.clone(),
        Arc::new(RwLock::new(policy)),
        Arc::new(registry),
        broker,
        1_048_576,
    )
    .await
    .unwrap();

    let id = kernel.submit_task(TaskInput::Text("plan something".into())).await.unwrap();
    let outcome = kernel.execute_task(&id).await.unwrap();
    assert!(outcome.success);
    assert!(outcome.output.unwrap().contains("architectural plan"));

    let state = kernel.task_state(&id).await.unwrap();
    assert_eq!(state, TaskState::Completed);

    // Verify reviewer was skipped: no ModelRequest events for "Reviewer"
    let events = store.get_all_events().await.unwrap();
    let reviewer_events: Vec<_> = events
        .iter()
        .filter(|e| {
            if let EventPayload::ModelRequest { role, .. } = &e.payload {
                role == "Reviewer"
            } else {
                false
            }
        })
        .collect();
    assert!(
        reviewer_events.is_empty(),
        "Reviewer should be skipped when only planner is registered"
    );
}

#[tokio::test]
async fn test_kernel_new() {
    let store = Arc::new(MockEventStore::new());
    let policy = PolicyEngine::deny_all();
    let registry = Arc::new(ProviderRegistry::new());
    let broker = Arc::new(ToolBroker::new(Arc::new(policy.clone())));
    let kernel = Kernel::new(store, Arc::new(RwLock::new(policy)), registry, broker, 1_048_576)
        .await
        .unwrap();
    assert_eq!(kernel.task_count().await, 0);
}

#[tokio::test]
async fn test_transition_through_multiple_states() {
    let store = Arc::new(MockEventStore::new());
    let rule = crate::policy::PolicyRule {
        name: "allow".into(),
        action_pattern: "*".into(),
        decision: "allow".into(),
        trust_tier: 0,
        description: None,
    };
    let policy = PolicyEngine::new(vec![rule], TrustTier::Autonomous);
    let registry = Arc::new(ProviderRegistry::new());
    let broker = Arc::new(ToolBroker::new(Arc::new(policy.clone())));
    let kernel = Kernel::new(store, Arc::new(RwLock::new(policy)), registry, broker, 1_048_576)
        .await
        .unwrap();

    let id = kernel.submit_task(TaskInput::Text("test".into())).await.unwrap();
    assert_eq!(kernel.task_state(&id).await.unwrap(), TaskState::Classified);

    kernel.transition_task(&id, TaskState::Planned).await.unwrap();
    assert_eq!(kernel.task_state(&id).await.unwrap(), TaskState::Planned);

    kernel.transition_task(&id, TaskState::AwaitingConfirmation).await.unwrap();
    assert_eq!(kernel.task_state(&id).await.unwrap(), TaskState::AwaitingConfirmation);

    kernel.transition_task(&id, TaskState::Executing).await.unwrap();
    assert_eq!(kernel.task_state(&id).await.unwrap(), TaskState::Executing);

    kernel.transition_task(&id, TaskState::Blocked).await.unwrap();
    assert_eq!(kernel.task_state(&id).await.unwrap(), TaskState::Blocked);

    kernel.transition_task(&id, TaskState::Executing).await.unwrap();
    kernel.transition_task(&id, TaskState::Completed).await.unwrap();
    assert_eq!(kernel.task_state(&id).await.unwrap(), TaskState::Completed);
}

#[tokio::test]
async fn test_vision_task_input() {
    let store = Arc::new(MockEventStore::new());
    let rule = crate::policy::PolicyRule {
        name: "allow".into(),
        action_pattern: "*".into(),
        decision: "allow".into(),
        trust_tier: 0,
        description: None,
    };
    let policy = PolicyEngine::new(vec![rule], TrustTier::Autonomous);
    let registry = Arc::new(ProviderRegistry::new());
    let broker = Arc::new(ToolBroker::new(Arc::new(policy.clone())));
    let kernel = Kernel::new(store, Arc::new(RwLock::new(policy)), registry, broker, 1_048_576)
        .await
        .unwrap();

    let id = kernel
        .submit_task(TaskInput::Vision {
            text: "describe this image".into(),
            image_paths: vec![PathBuf::from("/tmp/img.png")],
        })
        .await
        .unwrap();
    assert_eq!(kernel.task_state(&id).await.unwrap(), TaskState::Classified);
}

#[tokio::test]
async fn test_multi_task_input() {
    let store = Arc::new(MockEventStore::new());
    let rule = crate::policy::PolicyRule {
        name: "allow".into(),
        action_pattern: "*".into(),
        decision: "allow".into(),
        trust_tier: 0,
        description: None,
    };
    let policy = PolicyEngine::new(vec![rule], TrustTier::Autonomous);
    let registry = Arc::new(ProviderRegistry::new());
    let broker = Arc::new(ToolBroker::new(Arc::new(policy.clone())));
    let kernel = Kernel::new(store, Arc::new(RwLock::new(policy)), registry, broker, 1_048_576)
        .await
        .unwrap();

    let input = TaskInput::Multi {
        parts: vec![TaskInput::Text("part1".into()), TaskInput::Text("part2".into())],
    };
    let id = kernel.submit_task(input).await.unwrap();
    assert_eq!(kernel.task_state(&id).await.unwrap(), TaskState::Classified);
}

#[tokio::test]
async fn test_submit_task_creates_record_with_correct_state_history() {
    let store = Arc::new(MockEventStore::new());
    let rule = crate::policy::PolicyRule {
        name: "allow".into(),
        action_pattern: "*".into(),
        decision: "allow".into(),
        trust_tier: 0,
        description: None,
    };
    let policy = PolicyEngine::new(vec![rule], TrustTier::Autonomous);
    let registry = Arc::new(ProviderRegistry::new());
    let broker = Arc::new(ToolBroker::new(Arc::new(policy.clone())));
    let kernel =
        Kernel::new(store.clone(), Arc::new(RwLock::new(policy)), registry, broker, 1_048_576)
            .await
            .unwrap();

    let id = kernel.submit_task(TaskInput::Text("test".into())).await.unwrap();

    // The projection should have the task with state history
    let events = store.get_task_events(&id).await.unwrap();
    assert!(!events.is_empty());
}
