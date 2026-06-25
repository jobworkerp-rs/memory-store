//! CLI for LLM Memory Plugin: store and `find_all` commands.

use anyhow::Result;
use clap::Parser;
use jobworkerp_client::plugins::MultiMethodPluginRunner;
use llm_memory_plugin::LlmMemoryPlugin;
use llm_memory_plugin::protobuf::llm::LlmStoreArgs;
use llm_memory_plugin::protobuf::llm::llm_store_args::{
    ChatMessage, LlmGenerationOptions, MessageRole,
};
use llm_memory_plugin::protobuf::{FindAllArgs, FindAllResult, LlmMemoryPluginSettings};
use prost::Message;
use std::collections::HashMap;
use tracing::Level;

#[derive(Parser, Debug)]
#[clap(name = "llm-memory-plugin", version)]
struct Opts {
    #[clap(short, long, default_value = "http://127.0.0.1:9010")]
    server_url: String,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Parser, Debug)]
enum Command {
    /// Store memory data
    Store(StoreArgs),
    /// Find all memories
    FindAll(FindAllCli),
}

#[derive(Parser, Debug)]
struct StoreArgs {
    /// Content to store
    #[clap(long)]
    prompt: String,
    /// User ID
    #[clap(long)]
    user_id: i64,
}

#[derive(Parser, Debug)]
struct FindAllCli {
    /// Limit number of results
    #[clap(long)]
    limit: Option<i32>,
    /// Offset for pagination
    #[clap(long)]
    offset: Option<i64>,
}

fn build_llm_store_args(prompt: &str, user_id: i64) -> Vec<u8> {
    LlmStoreArgs {
        prompt: prompt.to_string(),
        user_id,
        options: Some(LlmGenerationOptions {
            sample_len: Some(10),
            temperature: Some(0.5),
            top_p: Some(0.9),
            seed: Some(0),
            ..Default::default()
        }),
        override_system_prompt: None,
        use_chat: true,
        histories: vec![ChatMessage {
            content: "You are helpful agent.".to_string(),
            role: MessageRole::System as i32,
            ..Default::default()
        }],
        refresh_history: false,
        schema_json: None,
        divide_think_tag: false,
        think: None,
        system_prompt_id: None,
    }
    .encode_to_vec()
}

fn main() -> Result<()> {
    command_utils::util::tracing::tracing_init_test(Level::DEBUG);
    dotenvy::dotenv().ok();

    let opts = Opts::parse();

    let mut plugin = LlmMemoryPlugin::new();
    let settings = LlmMemoryPluginSettings {
        server_url: Some(opts.server_url),
    };
    plugin.load(settings.encode_to_vec())?;

    match opts.command {
        Command::Store(args) => {
            let arg = build_llm_store_args(&args.prompt, args.user_id);
            let (result, _metadata) = plugin.run(arg, HashMap::new(), Some("store"));
            result?;
            println!("Store completed successfully.");
        }
        Command::FindAll(args) => {
            let find_args = FindAllArgs {
                limit: args.limit,
                offset: args.offset,
            };
            let (result, _metadata) =
                plugin.run(find_args.encode_to_vec(), HashMap::new(), Some("find_all"));
            let bytes = result?;
            let find_result = FindAllResult::decode(&mut std::io::Cursor::new(bytes))?;
            for memory in &find_result.memories {
                println!("{memory:?}");
            }
            println!("Total: {} memories", find_result.memories.len());
        }
    }

    Ok(())
}
