//! Claude-backed translator (see "Natural language" in ARCHITECTURE.md).
//!
//! The model is given the retrieved schema context and a *tool* whose input
//! schema is [`PlanRequest::json_schema`], and is forced to call it. So the model
//! never emits an executable operation. It emits structured data that then goes
//! through exactly the same validator as a hand-written plan. A hallucinated
//! column comes back as `not_found`, not as a wrong answer.
//!
//! Enabled only when `ANTHROPIC_API_KEY` is set; otherwise the engine uses
//! [`crate::rule::RuleTranslator`], so nothing here is on the default path.

use std::time::Duration;

use adb_core::{AdbError, Result};
use serde_json::{json, Value as Json};

use crate::plan::PlanRequest;
use crate::retrieval::schema_context;
use crate::translate::{IntentTranslator, Translation, TranslationContext};

/// Default model. Structured tool output with a closed schema is exactly the
/// shape Sonnet is good at, and it keeps translation latency low.
pub const DEFAULT_MODEL: &str = "claude-sonnet-5";
const DEFAULT_BASE_URL: &str = "https://api.anthropic.com";
const API_VERSION: &str = "2023-06-01";
const TOOL_NAME: &str = "emit_query_plan";

pub struct AnthropicTranslator {
    api_key: String,
    model: String,
    base_url: String,
    max_tokens: u32,
    client: reqwest::blocking::Client,
}

impl std::fmt::Debug for AnthropicTranslator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print the key.
        f.debug_struct("AnthropicTranslator")
            .field("model", &self.model)
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

impl AnthropicTranslator {
    pub fn new(api_key: impl Into<String>, model: impl Into<String>) -> Result<Self> {
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| AdbError::internal(format!("http client: {e}")))?;
        Ok(Self {
            api_key: api_key.into(),
            model: model.into(),
            base_url: DEFAULT_BASE_URL.to_string(),
            max_tokens: 2048,
            client,
        })
    }

    /// Build from the environment, or `None` when no key is configured.
    ///
    /// `ADB_NL_MODEL` overrides the model; `ADB_ANTHROPIC_BASE_URL` overrides the
    /// endpoint (used by tests and proxies).
    pub fn from_env() -> Option<Self> {
        let key = std::env::var("ANTHROPIC_API_KEY")
            .ok()
            .filter(|k| !k.trim().is_empty())?;
        let model = std::env::var("ADB_NL_MODEL").unwrap_or_else(|_| DEFAULT_MODEL.to_string());
        let mut translator = Self::new(key, model).ok()?;
        if let Ok(base) = std::env::var("ADB_ANTHROPIC_BASE_URL") {
            translator.base_url = base.trim_end_matches('/').to_string();
        }
        Some(translator)
    }

    pub fn model(&self) -> &str {
        &self.model
    }

    /// The system prompt: schema context plus the rules the plan must obey.
    pub fn system_prompt(context: &TranslationContext) -> String {
        format!(
            "You translate a user's question into a query plan for AgenticDB by calling the \
             `{TOOL_NAME}` tool. Rules:\n\
             - Use only the tables and columns listed below. Never invent names.\n\
             - Use `operation: \"aggregate\"` when the answer is a metric or is grouped; \
             otherwise `\"select\"`.\n\
             - Put every condition in `filters`. Write dates as ISO-8601 strings.\n\
             - Respect each column's semantic type: never sum an identifier or an email.\n\
             - Prefer a column's stated default aggregation when the user names a measure \
             without a function.\n\
             - Today is {today}. Resolve relative dates against it.\n\
             - Joins are not supported: one table per plan. If the question needs two tables, \
             call the tool with the single table that best answers it and nothing invented.\n\n\
             Schema:\n{schema}",
            today = context.now.format("%Y-%m-%d"),
            schema = schema_context(&context.tables)
        )
    }

    fn request_body(&self, request: &str, context: &TranslationContext) -> Json {
        json!({
            "model": self.model,
            "max_tokens": self.max_tokens,
            "system": Self::system_prompt(context),
            "tools": [{
                "name": TOOL_NAME,
                "description": "Emit the query plan that answers the user's question.",
                "input_schema": PlanRequest::json_schema(),
            }],
            // Force the tool call: prose is never a valid answer here.
            "tool_choice": { "type": "tool", "name": TOOL_NAME },
            "messages": [{ "role": "user", "content": request }],
        })
    }
}

/// Extract the plan from a Messages API response.
///
/// Separated from the HTTP call so the parsing rules are testable without a
/// network or a key.
pub fn plan_from_response(response: &Json) -> Result<PlanRequest> {
    if let Some(error) = response.get("error") {
        let message = error
            .get("message")
            .and_then(Json::as_str)
            .unwrap_or("unknown error");
        return Err(AdbError::Internal(format!(
            "Anthropic API error: {message}"
        )));
    }
    let content = response
        .get("content")
        .and_then(Json::as_array)
        .ok_or_else(|| AdbError::Internal("Anthropic response has no content".to_string()))?;

    let tool_use = content
        .iter()
        .find(|block| {
            block.get("type").and_then(Json::as_str) == Some("tool_use")
                && block.get("name").and_then(Json::as_str) == Some(TOOL_NAME)
        })
        .ok_or_else(|| {
            // The model answered in prose instead of calling the tool: surface
            // what it said, since it is usually a refusal or a clarification.
            let text = content
                .iter()
                .filter_map(|block| block.get("text").and_then(Json::as_str))
                .collect::<Vec<_>>()
                .join(" ");
            if text.is_empty() {
                AdbError::Internal("the model returned no query plan".to_string())
            } else {
                AdbError::bad_request(format!("the model did not produce a plan: {text}"))
            }
        })?;

    let input = tool_use
        .get("input")
        .ok_or_else(|| AdbError::Internal("tool call has no input".to_string()))?;
    serde_json::from_value(input.clone()).map_err(|e| {
        AdbError::bad_request(format!(
            "the model produced an unusable plan ({e}): {input}"
        ))
    })
}

impl IntentTranslator for AnthropicTranslator {
    fn name(&self) -> &'static str {
        "anthropic"
    }

    fn translate(&self, request: &str, context: &TranslationContext) -> Result<Translation> {
        context.require_tables()?;
        let body = self.request_body(request, context);
        let response = self
            .client
            .post(format!("{}/v1/messages", self.base_url))
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", API_VERSION)
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .map_err(|e| AdbError::Internal(format!("calling the Anthropic API failed: {e}")))?;

        let status = response.status();
        let payload: Json = response
            .json()
            .map_err(|e| AdbError::Internal(format!("unreadable Anthropic response: {e}")))?;
        if !status.is_success() {
            let message = payload
                .get("error")
                .and_then(|e| e.get("message"))
                .and_then(Json::as_str)
                .unwrap_or("no message");
            return Err(AdbError::Internal(format!(
                "Anthropic API returned {status}: {message}"
            )));
        }
        let plan = plan_from_response(&payload)?;
        Ok(Translation {
            plan,
            translator: "anthropic",
            interpretation: format!("translated by {}", self.model),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adb_core::{Aggregation, ColumnSchema, DataType, SemanticType, TableName, TableSchema};
    use chrono::{TimeZone, Utc};
    use std::sync::Arc;

    fn context() -> TranslationContext {
        let schema = TableSchema::new(
            TableName::new("orders").unwrap(),
            vec![
                ColumnSchema::new("id", DataType::Int64)
                    .required()
                    .semantic(SemanticType::Id),
                ColumnSchema::new("amount", DataType::Float64)
                    .semantic(SemanticType::Currency)
                    .aggregated_by(Aggregation::Sum),
                ColumnSchema::new("secret", DataType::Utf8).sensitive(),
            ],
        );
        TranslationContext::new(vec![Arc::new(schema)])
            .at(Utc.with_ymd_and_hms(2026, 9, 9, 12, 0, 0).unwrap())
    }

    #[test]
    fn the_prompt_carries_schema_semantics_and_todays_date() {
        let prompt = AnthropicTranslator::system_prompt(&context());
        assert!(prompt.contains("table orders"), "{prompt}");
        assert!(prompt.contains("aggregate with sum"), "{prompt}");
        assert!(prompt.contains("2026-09-09"), "{prompt}");
        assert!(prompt.contains("Joins are not supported"), "{prompt}");
        assert!(
            !prompt.contains("secret"),
            "sensitive column leaked into the prompt"
        );
    }

    #[test]
    fn the_tool_is_forced_so_prose_is_not_an_option() {
        let translator = AnthropicTranslator::new("test-key", "claude-sonnet-5").unwrap();
        let body = translator.request_body("how many orders", &context());
        assert_eq!(body["tool_choice"]["type"], json!("tool"));
        assert_eq!(body["tool_choice"]["name"], json!(TOOL_NAME));
        assert_eq!(body["tools"][0]["input_schema"], PlanRequest::json_schema());
        assert_eq!(body["model"], json!("claude-sonnet-5"));
        assert_eq!(body["messages"][0]["content"], json!("how many orders"));
    }

    #[test]
    fn a_tool_call_is_parsed_into_a_plan() {
        let response = json!({
            "content": [
                { "type": "text", "text": "Let me query that." },
                {
                    "type": "tool_use",
                    "name": TOOL_NAME,
                    "input": {
                        "operation": "aggregate",
                        "table": "orders",
                        "metrics": [{ "function": "sum", "column": "amount", "alias": "revenue" }]
                    }
                }
            ]
        });
        let plan = plan_from_response(&response).unwrap();
        assert_eq!(plan.table, "orders");
        assert_eq!(plan.metrics.len(), 1);
        assert_eq!(plan.metrics[0].alias.as_deref(), Some("revenue"));
    }

    #[test]
    fn prose_instead_of_a_plan_is_reported_with_what_the_model_said() {
        let response = json!({
            "content": [{ "type": "text", "text": "I need to know which quarter you mean." }]
        });
        let err = plan_from_response(&response).unwrap_err();
        assert_eq!(err.code(), "bad_request");
        assert!(err.to_string().contains("which quarter"), "{err}");
    }

    #[test]
    fn an_unusable_tool_input_names_the_problem() {
        let response = json!({
            "content": [{
                "type": "tool_use",
                "name": TOOL_NAME,
                "input": { "operation": "aggregate" }
            }]
        });
        let err = plan_from_response(&response).unwrap_err();
        assert!(err.to_string().contains("unusable plan"), "{err}");
    }

    #[test]
    fn api_errors_are_surfaced_not_swallowed() {
        let response = json!({ "error": { "message": "overloaded" } });
        let err = plan_from_response(&response).unwrap_err();
        assert!(err.to_string().contains("overloaded"), "{err}");
    }

    #[test]
    fn debug_output_never_contains_the_api_key() {
        let translator = AnthropicTranslator::new("sk-secret-value", DEFAULT_MODEL).unwrap();
        let debug = format!("{translator:?}");
        assert!(!debug.contains("sk-secret-value"), "{debug}");
    }

    #[test]
    fn from_env_is_none_without_a_key() {
        // Only assert the negative case: the positive one depends on the
        // ambient environment, which tests must not depend on.
        if std::env::var("ANTHROPIC_API_KEY").is_err() {
            assert!(AnthropicTranslator::from_env().is_none());
        }
    }
}
