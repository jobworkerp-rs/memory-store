use crate::error::LlmMemoryError;
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::redis::{RedisPool, UseRedisPool};
use prost::Message;
use protobuf::llm_memory::data::{Memory, MemoryData, MemoryId};
use redis::AsyncCommands;
use std::collections::BTreeMap;
use std::io::Cursor;

// TODO use if you need (not using in default)
#[async_trait]
pub trait RedisMemoryRepository: UseRedisPool + Sync + 'static
where
    Self: Send + 'static,
{
    const CACHE_KEY: &'static str = "MEMORY_DEF";

    async fn create(&self, id: &MemoryId, memory: &MemoryData) -> Result<()> {
        let res: Result<bool> = self
            .redis_pool()
            .get()
            .await?
            .hset_nx(Self::CACHE_KEY, id.value, Self::serialize_memory(memory))
            .await
            .map_err(|e| LlmMemoryError::RedisError(e).into());
        match res {
            Ok(r) => {
                if r {
                    Ok(())
                } else {
                    Err(LlmMemoryError::AlreadyExists(format!(
                        "memory creation error: already exists id={}",
                        id.value
                    ))
                    .into())
                }
            }
            Err(e) => Err(e),
        }
    }

    async fn upsert(&self, id: &MemoryId, memory: &MemoryData) -> Result<bool> {
        let m = Self::serialize_memory(memory);

        let res: Result<bool> = self
            .redis_pool()
            .get()
            .await?
            .hset(Self::CACHE_KEY, id.value, m)
            .await
            .map_err(|e| LlmMemoryError::RedisError(e).into());
        res
    }

    async fn delete(&self, id: &MemoryId) -> Result<bool> {
        self.redis_pool()
            .get()
            .await?
            .hdel(Self::CACHE_KEY, id.value)
            .await
            .map_err(|e| LlmMemoryError::RedisError(e).into())
    }

    async fn find(&self, id: &MemoryId) -> Result<Option<Memory>> {
        match self
            .redis_pool()
            .get()
            .await?
            .hget(Self::CACHE_KEY, id.value)
            .await
        {
            Ok(Some(v)) => Self::deserialize_to_memory(&v).map(|d| {
                Some(Memory {
                    id: Some(*id),
                    data: Some(d),
                    media: None,
                })
            }),
            Ok(None) => Ok(None),
            Err(e) => Err(LlmMemoryError::RedisError(e).into()),
        }
    }

    async fn find_all(&self) -> Result<Vec<Memory>> {
        let res: Result<BTreeMap<i64, Vec<u8>>> = self
            .redis_pool()
            .get()
            .await?
            .hgetall(Self::CACHE_KEY)
            .await
            .map_err(|e| LlmMemoryError::RedisError(e).into());
        res.map(|tree| {
            tree.iter()
                .flat_map(|(id, v)| {
                    Self::deserialize_to_memory(v).map(|d| Memory {
                        id: Some(MemoryId { value: *id }),
                        data: Some(d),
                        media: None,
                    })
                })
                .collect()
        })
    }

    async fn count(&self) -> Result<i64> {
        self.redis_pool()
            .get()
            .await?
            .hlen(Self::CACHE_KEY)
            .await
            .map_err(|e| LlmMemoryError::RedisError(e).into())
    }

    fn serialize_memory(w: &MemoryData) -> Vec<u8> {
        let mut buf = Vec::with_capacity(w.encoded_len());
        w.encode(&mut buf).unwrap();
        buf
    }

    fn deserialize_to_memory(buf: &Vec<u8>) -> Result<MemoryData> {
        MemoryData::decode(&mut Cursor::new(buf)).map_err(|e| LlmMemoryError::CodecError(e).into())
    }
    fn deserialize_bytes_to_memory(buf: &[u8]) -> Result<MemoryData> {
        MemoryData::decode(&mut Cursor::new(buf)).map_err(|e| LlmMemoryError::CodecError(e).into())
    }
}

impl<T: UseRedisPool + Send + Sync + 'static> RedisMemoryRepository for T {}

pub struct RedisMemoryRepositoryImpl {
    pub redis_pool: &'static RedisPool,
}

impl UseRedisPool for RedisMemoryRepositoryImpl {
    fn redis_pool(&self) -> &'static RedisPool {
        self.redis_pool
    }
}

pub trait UseRedisMemoryRepository {
    fn redis_memory_repository(&self) -> &RedisMemoryRepositoryImpl;
}

#[tokio::test]
async fn redis_test() -> Result<()> {
    use protobuf::llm_memory::data::{MemoryData, MemoryId, UserId};
    let pool = infra_utils::infra::test::setup_test_redis_pool().await;

    let repo = RedisMemoryRepositoryImpl { redis_pool: pool };
    let id = MemoryId { value: 1 };
    let memory = &MemoryData {
        parent_ids: vec![MemoryId { value: 3 }],
        user_id: Some(UserId { value: 4 }),
        content: "hoge4".to_string(),
        content_type: 6,
        params: Some("hoge7".to_string()),
        metadata: Some("hoge8".to_string()),
        created_at: 9,
        updated_at: 10,
        role: 0,
        external_id: None,
        media_object_id: None,
        thread_ids: Vec::new(),
        memory_kind: 0,
    };
    // clear first
    repo.delete(&id).await?;

    // create and find
    repo.create(&id, memory).await?;
    assert!(repo.create(&id, memory).await.err().is_some()); // already exists
    let res = repo.find(&id).await?;
    assert_eq!(res.and_then(|r| r.data).as_ref(), Some(memory));

    let mut memory2 = memory.clone();
    memory2.parent_ids = vec![MemoryId { value: 8 }];
    memory2.user_id = Some(UserId { value: 5 });
    memory2.content = "fuga4".to_string();
    memory2.content_type = 7;
    memory2.params = Some("fuga7".to_string());
    memory2.metadata = Some("fuga8".to_string());
    memory2.created_at = 10;
    memory2.updated_at = 11;
    // update and find
    assert!(!repo.upsert(&id, &memory2).await?);
    let res2 = repo.find(&id).await?;
    assert_eq!(res2.and_then(|r| r.data).as_ref(), Some(&memory2));

    // delete and not found
    assert!(repo.delete(&id).await?);
    assert_eq!(repo.find(&id).await?, None);

    Ok(())
}
