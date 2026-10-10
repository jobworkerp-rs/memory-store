//! `embedding inspect` / `embedding plan`: read-only status and planning
//! of the embedding space. Both RDB backends; no memories server or
//! jobworkerp is needed, and nothing is changed.

use super::open_target_pool;
use clap::{Args, Subcommand};
use grpc_admin::db_migrate::embedding::{
    inspect,
    observe::{EmbeddingEnv, observe},
    output::{
        Command, ErrorCode, FailureLine, InspectLine, NextAction, Resolution, Stage, State,
        UnavailableReason,
    },
    plan,
};
use infra::infra::embedding_space::SpaceComponents;

#[derive(Debug, Subcommand)]
pub enum EmbeddingCommand {
    /// Report the state of the embedding space and the next action.
    Inspect,
    /// Report what moving to another embedding space requires.
    Plan(PlanArgs),
    /// Start (or `--resume`) moving the vector tables to another space.
    Switch(super::embedding_migrate::SwitchArgs),
    /// End an attempt before data changed, or discard a no-backup one.
    Abandon(super::embedding_migrate::AttemptArgs),
    /// Verify the rebuilt tables and end the attempt in the target space.
    Finalize(super::embedding_finish::FinalizeArgs),
    /// Put the backup of an attempt back (cancel a backup attempt).
    Restore(super::embedding_migrate::AttemptArgs),
}

#[derive(Debug, Args)]
pub struct PlanArgs {
    #[arg(long)]
    model_id: String,
    /// Empty when the model's own tokenizer is used.
    #[arg(long, default_value = "")]
    tokenizer_model_id: String,
    /// Defaults to `unversioned`, like MEMORY_EMBEDDING_MODEL_REVISION.
    #[arg(long, default_value = "unversioned")]
    revision: String,
    #[arg(long)]
    dimension: u32,
}

/// Returns the process exit code.
pub async fn run_embedding(command: EmbeddingCommand) -> i32 {
    match command {
        EmbeddingCommand::Inspect => run_inspect().await,
        EmbeddingCommand::Plan(args) => run_plan(args).await,
        EmbeddingCommand::Switch(args) => finish(super::embedding_migrate::run_switch(args).await),
        EmbeddingCommand::Abandon(args) => {
            finish(super::embedding_migrate::run_abandon(args).await)
        }
        EmbeddingCommand::Finalize(args) => {
            finish(super::embedding_finish::run_finalize(args).await)
        }
        EmbeddingCommand::Restore(args) => finish(super::embedding_finish::run_restore(args).await),
    }
}

/// Print the final line and return the exit code.
fn finish(outcome: super::embedding_migrate::Outcome) -> i32 {
    match outcome {
        Ok(line) => {
            println!("{line}");
            0
        }
        Err(super::embedding_migrate::Failed(line)) => {
            println!("{line}");
            1
        }
    }
}

fn unavailable_line() -> InspectLine {
    InspectLine {
        state: State::Unavailable,
        unavailable_reason: Some(UnavailableReason::ResourceUnavailable),
        next_action: NextAction::CheckEnvironment,
        ..Default::default()
    }
}

/// Opening failures are a state, not a command failure (spec §3.9).
async fn run_inspect() -> i32 {
    let (Ok(env), Ok(pool)) = (EmbeddingEnv::from_env(), open_target_pool().await) else {
        println!("{}", unavailable_line());
        return 0;
    };
    let pool: &'static _ = Box::leak(Box::new(pool));
    let obs = observe(pool, &env).await;
    println!("{}", inspect::derive(&obs));
    0
}

async fn run_plan(args: PlanArgs) -> i32 {
    let fail = |code: ErrorCode, resolution: Resolution| {
        println!(
            "{}",
            FailureLine::new(Command::Plan, Stage::Open, code, resolution)
        );
        1
    };
    let (Ok(env), Ok(pool)) = (EmbeddingEnv::from_env(), open_target_pool().await) else {
        return fail(ErrorCode::ResourceUnavailable, Resolution::CheckEnvironment);
    };
    let Some(current) = &env.current else {
        return fail(ErrorCode::ResourceUnavailable, Resolution::CheckEnvironment);
    };
    let pool: &'static _ = Box::leak(Box::new(pool));
    let obs = observe(pool, &env).await;
    match obs.unavailable {
        Some(UnavailableReason::ResourceUnavailable) => {
            return fail(ErrorCode::ResourceUnavailable, Resolution::CheckEnvironment);
        }
        Some(UnavailableReason::ResourceCorrupt) => {
            return fail(ErrorCode::ResourceCorrupt, Resolution::ManualRecovery);
        }
        None => {}
    }
    let target = SpaceComponents {
        model_id: args.model_id,
        tokenizer_model_id: args.tokenizer_model_id,
        revision: args.revision,
        dimension: args.dimension,
        distance: current.distance.clone(),
    };
    let inspected = inspect::derive(&obs).state;
    println!(
        "{}",
        plan::derive(&obs, inspected, target.space_id().as_str())
    );
    0
}
