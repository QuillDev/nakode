use prost::Message;

#[derive(Clone, PartialEq, Message)]
pub struct Metadata {
    #[prost(string, tag = "1")]
    pub ide_name: String,
    #[prost(string, tag = "7")]
    pub ide_version: String,
    #[prost(string, tag = "12")]
    pub extension_name: String,
    #[prost(string, tag = "2")]
    pub extension_version: String,
    #[prost(string, tag = "3")]
    pub api_key: String,
    #[prost(string, tag = "4")]
    pub locale: String,
    #[prost(string, tag = "21")]
    pub user_jwt: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct GetUserJwtRequest {
    #[prost(message, optional, tag = "1")]
    pub metadata: Option<Metadata>,
}

#[derive(Clone, PartialEq, Message)]
pub struct GetUserJwtResponse {
    #[prost(string, tag = "1")]
    pub user_jwt: String,
    #[prost(string, tag = "2")]
    pub custom_api_server_url: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct GetCliModelConfigsRequest {
    #[prost(message, optional, tag = "1")]
    pub metadata: Option<Metadata>,
}

#[derive(Clone, PartialEq, Message)]
pub struct GetCliModelConfigsResponse {
    #[prost(message, repeated, tag = "1")]
    pub client_model_configs: Vec<ClientModelConfig>,
    #[prost(message, optional, tag = "3")]
    pub default_override_model_config: Option<DefaultOverrideModelConfig>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ClientModelConfig {
    #[prost(string, tag = "1")]
    pub label: String,
    #[prost(string, tag = "22")]
    pub model_uid: String,
    #[prost(bool, tag = "4")]
    pub disabled: bool,
    #[prost(bool, tag = "5")]
    pub supports_images: bool,
    #[prost(int32, tag = "18")]
    pub max_tokens: i32,
    #[prost(string, optional, tag = "27")]
    pub description: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct DefaultOverrideModelConfig {
    #[prost(string, tag = "3")]
    pub model_uid: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct GetChatMessageRequest {
    #[prost(message, optional, tag = "1")]
    pub metadata: Option<Metadata>,
    #[prost(string, tag = "2")]
    pub prompt: String,
    #[prost(message, repeated, tag = "3")]
    pub chat_message_prompts: Vec<ChatMessagePrompt>,
    #[prost(string, tag = "21")]
    pub chat_model_uid: String,
    #[prost(int32, tag = "7")]
    pub request_type: i32,
    #[prost(message, optional, tag = "8")]
    pub configuration: Option<CompletionConfiguration>,
    #[prost(message, repeated, tag = "10")]
    pub tools: Vec<ChatToolDefinition>,
    #[prost(bool, tag = "11")]
    pub disable_parallel_tool_calls: bool,
    #[prost(message, optional, tag = "12")]
    pub tool_choice: Option<ChatToolChoice>,
    #[prost(message, optional, tag = "13")]
    pub system_prompt_cache_options: Option<PromptCacheOptions>,
    #[prost(string, tag = "16")]
    pub cascade_id: String,
    #[prost(int32, tag = "20")]
    pub planner_mode: i32,
    #[prost(string, tag = "22")]
    pub execution_id: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct GetChatMessageResponse {
    #[prost(string, tag = "1")]
    pub message_id: String,
    #[prost(string, tag = "3")]
    pub delta_text: String,
    #[prost(int32, tag = "5")]
    pub stop_reason: i32,
    #[prost(message, repeated, tag = "6")]
    pub delta_tool_calls: Vec<ChatToolCall>,
    #[prost(string, tag = "9")]
    pub delta_thinking: String,
    #[prost(string, tag = "10")]
    pub delta_signature: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct CompletionConfiguration {
    #[prost(uint64, tag = "1")]
    pub num_completions: u64,
    #[prost(uint64, tag = "2")]
    pub max_tokens: u64,
    #[prost(uint64, tag = "3")]
    pub max_newlines: u64,
    #[prost(double, tag = "5")]
    pub temperature: f64,
    #[prost(double, tag = "6")]
    pub first_temperature: f64,
    #[prost(uint64, tag = "7")]
    pub top_k: u64,
    #[prost(double, tag = "8")]
    pub top_p: f64,
    #[prost(string, repeated, tag = "9")]
    pub stop_patterns: Vec<String>,
    #[prost(double, tag = "11")]
    pub fim_eot_prob_threshold: f64,
}

#[derive(Clone, PartialEq, Message)]
pub struct ChatMessagePrompt {
    #[prost(string, tag = "1")]
    pub message_id: String,
    #[prost(int32, tag = "2")]
    pub source: i32,
    #[prost(string, tag = "3")]
    pub prompt: String,
    #[prost(message, repeated, tag = "6")]
    pub tool_calls: Vec<ChatToolCall>,
    #[prost(string, tag = "7")]
    pub tool_call_id: String,
    #[prost(bool, tag = "9")]
    pub tool_result_is_error: bool,
    #[prost(string, tag = "11")]
    pub thinking: String,
    #[prost(string, tag = "12")]
    pub signature: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct ChatToolCall {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(string, tag = "2")]
    pub name: String,
    #[prost(string, tag = "3")]
    pub arguments_json: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct ChatToolDefinition {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub description: String,
    #[prost(string, tag = "3")]
    pub json_schema_string: String,
    #[prost(bool, tag = "12")]
    pub strict: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct ChatToolChoice {
    #[prost(oneof = "chat_tool_choice::Choice", tags = "1, 2")]
    pub choice: Option<chat_tool_choice::Choice>,
}

pub mod chat_tool_choice {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Choice {
        #[prost(string, tag = "1")]
        OptionName(String),
        #[prost(string, tag = "2")]
        ToolName(String),
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct PromptCacheOptions {
    #[prost(int32, tag = "1")]
    pub r#type: i32,
}
