use std::sync::Arc;

use chrono::Utc;
use tokio::sync::{Mutex, RwLock};

use crate::{
    error::{KernelError, TaskError},
    events::{Event, EventKind, EventPayload},
    model::{
        registry::ProviderRegistry,
        types::{ChatMessage, ChatRole, CompletionRequest},
    },
    policy::PolicyEngine,
    router::TaskRouter,
    state::{TaskRecord, TaskState},
    storage::{EventStore, TaskProjection},
    task::{TaskId, TaskInput, TaskRequest},
    tools::{broker::ToolBroker, executor::ToolRequest},
};

struct PendingConfirmation {
    request: ToolRequest,
    output: String,
}

/// The SheshAOS kernel — owns task lifecycle, policy, and state.
pub struct Kernel {
    event_store: Arc<dyn EventStore>,
    projection: Arc<RwLock<TaskProjection>>,
    policy: Arc<RwLock<PolicyEngine>>,
    provider_registry: Arc<ProviderRegistry>,
    tool_broker: Arc<ToolBroker>,
    pending_confirmations: Mutex<std::collections::HashMap<TaskId, PendingConfirmation>>,
    max_tool_output_size: usize,
}

impl Kernel {
    /// Create a new kernel with the given components.
    pub async fn new(
        event_store: Arc<dyn EventStore>,
        policy: Arc<RwLock<PolicyEngine>>,
        provider_registry: Arc<ProviderRegistry>,
        tool_broker: Arc<ToolBroker>,
        max_tool_output_size: usize,
    ) -> Result<Self, KernelError> {
        let kernel = Self {
            event_store,
            projection: Arc::new(RwLock::new(TaskProjection::new())),
            policy,
            provider_registry,
            tool_broker,
            pending_confirmations: Mutex::new(std::collections::HashMap::new()),
            max_tool_output_size,
        };
        Ok(kernel)
    }

    /// Submit a new task. Returns the TaskId.
    pub async fn submit_task(&self, input: TaskInput) -> Result<TaskId, KernelError> {
        let task_id = TaskId::new();
        let request = TaskRequest::new(input.clone());

        // Policy check for task creation
        let decision = {
            let policy = self.policy.read().await;
            policy.evaluate(crate::policy::actions::TASK_CREATE)
        };

        if decision.is_denied() {
            return Err(KernelError::Policy(crate::error::PolicyError::Denied {
                reason: "Task creation denied by policy".into(),
            }));
        }

        // Emit TaskCreated event
        let event_payload = EventPayload::TaskCreated {
            request: serde_json::to_value(&request).map_err(KernelError::Serde)?,
        };
        let event =
            Event::new(task_id, EventKind::TaskCreated, event_payload, "kernel".to_string());
        self.emit_event(event).await?;

        // Initialize state in projection
        let record = TaskRecord {
            task_id,
            request,
            current_state: TaskState::Received,
            assigned_role: None,
            state_history: vec![(TaskState::Received, Utc::now())],
        };

        {
            let mut proj = self.projection.write().await;
            proj.tasks.insert(task_id, record);
        }

        // Classify via router
        let has_images = matches!(input, TaskInput::Vision { .. });
        let input_text = input.text();

        let route_decision = TaskRouter::route(&input_text, has_images);

        // Update state to Classified
        let class_payload = EventPayload::StateChanged {
            from: "Received".to_string(),
            to: "Classified".to_string(),
        };
        let class_event =
            Event::new(task_id, EventKind::TaskClassified, class_payload, "router".to_string());
        self.emit_event(class_event).await?;

        {
            let mut proj = self.projection.write().await;
            if let Some(task) = proj.tasks.get_mut(&task_id) {
                task.current_state = TaskState::Classified;
                task.assigned_role = Some(route_decision.primary_role);
                task.state_history.push((TaskState::Classified, Utc::now()));
            }
        }

        Ok(task_id)
    }

    /// Get the current state of a task.
    pub async fn task_state(&self, id: &TaskId) -> Result<TaskState, KernelError> {
        let proj = self.projection.read().await;
        if let Some(task) = proj.tasks.get(id) {
            Ok(task.current_state)
        } else {
            Err(KernelError::Task(TaskError::NotFound { id: id.to_string() }))
        }
    }

    /// Get all tasks in a given state.
    pub async fn tasks_in_state(&self, state: &TaskState) -> Vec<TaskId> {
        let proj = self.projection.read().await;
        proj.tasks
            .iter()
            .filter(|(_, record)| record.current_state == *state)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Transition a task to a new state (with validation).
    pub async fn transition_task(
        &self,
        task_id: &TaskId,
        new_state: TaskState,
    ) -> Result<(), KernelError> {
        let mut proj = self.projection.write().await;
        let task = proj
            .tasks
            .get_mut(task_id)
            .ok_or_else(|| KernelError::Task(TaskError::NotFound { id: task_id.to_string() }))?;

        let current_state = task.current_state;

        if !current_state.can_transition_to(&new_state) {
            return Err(KernelError::Task(TaskError::InvalidTransition {
                from: current_state.to_string(),
                to: new_state.to_string(),
            }));
        }

        let event_payload = EventPayload::StateChanged {
            from: current_state.to_string(),
            to: new_state.to_string(),
        };
        let event =
            Event::new(*task_id, EventKind::TaskStateChanged, event_payload, "kernel".to_string());
        self.emit_event(event).await?;

        task.current_state = new_state;
        task.state_history.push((new_state, Utc::now()));

        Ok(())
    }

    /// Get task count.
    pub async fn task_count(&self) -> usize {
        let proj = self.projection.read().await;
        proj.tasks.len()
    }

    /// Execute a task through the multi-model workflow (Planner -> Coder -> Reviewer -> Tool Broker).
    pub async fn execute_task(
        &self,
        task_id: &TaskId,
    ) -> Result<crate::task::TaskOutcome, KernelError> {
        // 1. Get request and verify state
        let task = {
            let proj = self.projection.read().await;
            proj.tasks
                .get(task_id)
                .cloned()
                .ok_or_else(|| KernelError::Task(TaskError::NotFound { id: task_id.to_string() }))?
        };

        if task.current_state != TaskState::Classified {
            return Err(KernelError::Task(TaskError::InvalidTransition {
                from: task.current_state.to_string(),
                to: TaskState::Planned.to_string(),
            }));
        }

        let input_text = task.request.input.text();

        let planner =
            self.provider_registry.get(&crate::state::ModelRole::Planner).ok_or_else(|| {
                KernelError::Provider(crate::error::ProviderError::Unavailable {
                    name: "Planner".into(),
                })
            })?;

        let req = CompletionRequest::new(
            vec![
                ChatMessage {
                    role: ChatRole::System,
                    content: "You are a planner.".to_string(),
                    images: None,
                },
                ChatMessage { role: ChatRole::User, content: input_text.clone(), images: None },
            ],
            planner.name(),
            planner.max_context(),
        );

        self.emit_model_requested(*task_id, "Planner", planner.max_context()).await?;

        let plan_resp = match planner.complete(req).await {
            Ok(resp) => resp,
            Err(e) => {
                let err_msg = format!("Planner failed: {}", e);
                return self
                    .emit_failure_and_return(*task_id, err_msg, Some(input_text.clone()))
                    .await;
            }
        };

        self.emit_model_responded(
            *task_id,
            "Planner",
            plan_resp.completion_tokens.unwrap_or(0),
            &plan_resp.content,
        )
        .await?;

        self.transition_task(task_id, TaskState::Planned).await?;

        let plan = plan_resp.content.to_lowercase();
        let requires_coder = plan.contains("write code")
            || plan.contains("implement ")
            || plan.contains("edit ")
            || plan.contains("fix bug")
            || plan.contains("refactor")
            || task.assigned_role == Some(crate::state::ModelRole::Coder);

        let mut final_output = plan_resp.content;

        if requires_coder {
            let Some(coder) = self.provider_registry.get(&crate::state::ModelRole::Coder) else {
                let err_msg = "Coder provider not available".to_string();
                return self.emit_failure_and_return(*task_id, err_msg, Some(final_output)).await;
            };

            self.transition_task(task_id, TaskState::Executing).await?;

            let code_req = CompletionRequest::new(
                vec![
                    ChatMessage {
                        role: ChatRole::System,
                        content: "You are a coder.".to_string(),
                        images: None,
                    },
                    ChatMessage {
                        role: ChatRole::User,
                        content: final_output.clone(),
                        images: None,
                    },
                ],
                coder.name(),
                coder.max_context(),
            );

            self.emit_model_requested(*task_id, "Coder", coder.max_context()).await?;

            let code_resp = match coder.complete(code_req).await {
                Ok(resp) => resp,
                Err(e) => {
                    let err_msg = format!("Coder failed: {}", e);
                    return self
                        .emit_failure_and_return(*task_id, err_msg, Some(final_output))
                        .await;
                }
            };

            self.emit_model_responded(
                *task_id,
                "Coder",
                code_resp.completion_tokens.unwrap_or(0),
                &code_resp.content,
            )
            .await?;

            final_output = code_resp.content.clone();

            // Reviewer
            if let Some(reviewer) = self.provider_registry.get(&crate::state::ModelRole::Reviewer) {
                let rev_req = CompletionRequest::new(
                    vec![
                        ChatMessage {
                            role: ChatRole::System,
                            content: "You are a reviewer.".to_string(),
                            images: None,
                        },
                        ChatMessage {
                            role: ChatRole::User,
                            content: final_output.clone(),
                            images: None,
                        },
                    ],
                    reviewer.name(),
                    reviewer.max_context(),
                );

                self.emit_model_requested(*task_id, "Reviewer", reviewer.max_context()).await?;

                let rev_resp = match reviewer.complete(rev_req).await {
                    Ok(resp) => resp,
                    Err(e) => {
                        let err_msg = format!("Reviewer failed: {}", e);
                        return self
                            .emit_failure_and_return(*task_id, err_msg, Some(final_output.clone()))
                            .await;
                    }
                };

                self.emit_model_responded(
                    *task_id,
                    "Reviewer",
                    rev_resp.completion_tokens.unwrap_or(0),
                    &rev_resp.content,
                )
                .await?;

                final_output = format!("{}\nReview: {}", final_output, rev_resp.content);
            }
        }

        let mut requires_confirmation = false;

        if let Some(tool_call) =
            final_output.strip_prefix("TOOL:").map(|s| s.trim()).filter(|s| !s.is_empty())
        {
            let tool_name_end =
                tool_call.find(|c: char| c.is_whitespace()).unwrap_or(tool_call.len());
            let tool_name = &tool_call[..tool_name_end];
            let args_str = tool_call[tool_name_end..].trim();

            if tool_name.is_empty() {
                let err_msg = "Tool name is empty".to_string();
                return self
                    .emit_failure_and_return(*task_id, err_msg, Some(final_output.clone()))
                    .await;
            }

            let arguments: serde_json::Value = if args_str.is_empty() {
                serde_json::json!({})
            } else {
                match serde_json::from_str(args_str) {
                    Ok(val) => val,
                    Err(e) => {
                        let err_msg = format!("Invalid tool arguments JSON: {}", e);
                        return self
                            .emit_failure_and_return(*task_id, err_msg, Some(final_output.clone()))
                            .await;
                    }
                }
            };

            let tool_req = crate::tools::executor::ToolRequest {
                tool_name: tool_name.to_string(),
                arguments: arguments.clone(),
            };
            self.emit_tool_requested(*task_id, tool_name, arguments).await?;

            match self.tool_broker.execute(&tool_req).await {
                Ok(crate::tools::broker::BrokerResult::Completed(res)) => {
                    self.emit_tool_result(
                        *task_id,
                        EventKind::ToolCompleted,
                        tool_name,
                        res.success,
                        &res.output,
                    )
                    .await?;
                }
                Ok(crate::tools::broker::BrokerResult::Denied(reason)) => {
                    self.emit_tool_result(
                        *task_id,
                        EventKind::ToolFailed,
                        tool_name,
                        false,
                        &format!("Denied: {}", reason),
                    )
                    .await?;
                    let err_msg = format!("Tool denied: {}", reason);
                    return self
                        .emit_failure_and_return(*task_id, err_msg, Some(final_output.clone()))
                        .await;
                }
                Ok(crate::tools::broker::BrokerResult::RequiresConfirmation(reason)) => {
                    self.emit_tool_result(
                        *task_id,
                        EventKind::ToolFailed,
                        tool_name,
                        false,
                        &format!("Requires confirmation: {}", reason),
                    )
                    .await?;
                    requires_confirmation = true;
                    self.pending_confirmations.lock().await.insert(
                        *task_id,
                        PendingConfirmation { request: tool_req, output: final_output.clone() },
                    );
                }
                Err(e) => {
                    self.emit_tool_result(
                        *task_id,
                        EventKind::ToolFailed,
                        tool_name,
                        false,
                        &e.to_string(),
                    )
                    .await?;
                    return self
                        .emit_failure_and_return(*task_id, e.to_string(), Some(final_output))
                        .await;
                }
            }
        }

        if requires_confirmation {
            let current_state = self.task_state(task_id).await?;
            match current_state {
                TaskState::Planned => {
                    self.transition_task(task_id, TaskState::AwaitingConfirmation).await?;
                }
                TaskState::Executing => {
                    self.transition_task(task_id, TaskState::Blocked).await?;
                }
                _ => {}
            }
            return Ok(crate::task::TaskOutcome {
                task_id: *task_id,
                success: false,
                output: Some(final_output),
                error: Some("Requires confirmation".to_string()),
                completed_at: Utc::now(),
                requires_confirmation: true,
            });
        }

        let current_state = self.task_state(task_id).await?;
        if current_state == TaskState::Planned {
            self.transition_task(task_id, TaskState::Executing).await?;
        }
        self.transition_task(task_id, TaskState::Completed).await?;

        Ok(crate::task::TaskOutcome {
            task_id: *task_id,
            success: true,
            output: Some(final_output),
            error: None,
            completed_at: Utc::now(),
            requires_confirmation: false,
        })
    }

    /// Execute the pending tool request for a task after explicit approval.
    pub async fn confirm_task(
        &self,
        task_id: &TaskId,
    ) -> Result<crate::task::TaskOutcome, KernelError> {
        let pending = self.pending_confirmations.lock().await.remove(task_id).ok_or_else(|| {
            KernelError::Task(TaskError::InvalidTransition {
                from: "No pending confirmation".to_string(),
                to: "Executing".to_string(),
            })
        })?;
        let state = self.task_state(task_id).await?;
        if !matches!(state, TaskState::AwaitingConfirmation | TaskState::Blocked) {
            return Err(KernelError::Task(TaskError::InvalidTransition {
                from: state.to_string(),
                to: TaskState::Executing.to_string(),
            }));
        }
        self.transition_task(task_id, TaskState::Executing).await?;

        match self.tool_broker.execute_confirmed(&pending.request).await {
            Ok(result) => {
                self.emit_tool_result(
                    *task_id,
                    EventKind::ToolCompleted,
                    &pending.request.tool_name,
                    result.success,
                    &result.output,
                )
                .await?;
                self.transition_task(task_id, TaskState::Completed).await?;
                Ok(crate::task::TaskOutcome {
                    task_id: *task_id,
                    success: result.success,
                    output: Some(pending.output),
                    error: if result.success { None } else { Some(result.output) },
                    completed_at: Utc::now(),
                    requires_confirmation: false,
                })
            }
            Err(error) => {
                self.emit_failure_and_return(*task_id, error.to_string(), Some(pending.output))
                    .await
            }
        }
    }

    // Helper: emit an event
    async fn emit_event(&self, event: Event) -> Result<(), KernelError> {
        self.event_store.append(event).await
    }

    async fn emit_model_requested(
        &self,
        task_id: TaskId,
        role: &str,
        context_budget: usize,
    ) -> Result<(), KernelError> {
        self.emit_event(Event::new(
            task_id,
            EventKind::ModelRequested,
            EventPayload::ModelRequest { role: role.to_string(), prompt_tokens: 0, context_budget },
            "kernel".to_string(),
        ))
        .await
    }

    async fn emit_model_responded(
        &self,
        task_id: TaskId,
        role: &str,
        response_tokens: usize,
        content: &str,
    ) -> Result<(), KernelError> {
        self.emit_event(Event::new(
            task_id,
            EventKind::ModelResponded,
            EventPayload::ModelResponse {
                role: role.to_string(),
                response_tokens,
                content: content.to_string(),
            },
            "kernel".to_string(),
        ))
        .await
    }

    async fn emit_tool_requested(
        &self,
        task_id: TaskId,
        tool_name: &str,
        arguments: serde_json::Value,
    ) -> Result<(), KernelError> {
        self.emit_event(Event::new(
            task_id,
            EventKind::ToolRequested,
            EventPayload::ToolCall { tool_name: tool_name.to_string(), arguments },
            "kernel".to_string(),
        ))
        .await
    }

    async fn emit_tool_result(
        &self,
        task_id: TaskId,
        kind: EventKind,
        tool_name: &str,
        success: bool,
        output: &str,
    ) -> Result<(), KernelError> {
        let max_size = self.max_tool_output_size;
        let truncated = if output.len() > max_size {
            // Step back to the nearest UTF-8 character boundary.
            let mut boundary = max_size;
            while boundary > 0 && !output.is_char_boundary(boundary) {
                boundary -= 1;
            }
            // Step back to the nearest newline boundary to preserve line structure.
            let cut_point = &output[..boundary];
            if let Some(last_newline) = cut_point.rfind('\n') {
                &output[..last_newline + 1]
            } else {
                cut_point
            }
        } else {
            output
        };
        self.emit_event(Event::new(
            task_id,
            kind,
            EventPayload::ToolResult {
                tool_name: tool_name.to_string(),
                success,
                output: truncated.to_string(),
            },
            "kernel".to_string(),
        ))
        .await
    }

    async fn emit_failure_and_return(
        &self,
        task_id: TaskId,
        error_message: String,
        output: Option<String>,
    ) -> Result<crate::task::TaskOutcome, KernelError> {
        self.emit_event(Event::new(
            task_id,
            EventKind::Error,
            EventPayload::ErrorEvent { message: error_message.clone(), details: None },
            "kernel".to_string(),
        ))
        .await?;
        self.transition_task(&task_id, TaskState::Failed).await?;
        Ok(crate::task::TaskOutcome {
            task_id,
            success: false,
            output,
            error: Some(error_message),
            completed_at: Utc::now(),
            requires_confirmation: false,
        })
    }
}

#[cfg(test)]
mod tests;
