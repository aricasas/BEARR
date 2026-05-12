use std::{path::PathBuf, time::Duration};

use bearr_error::DbError;
use hashbrown::HashMap;

use crate::{
    DbConfiguration, DbRequest, DbResponse,
    executor::{DbOperation, DbRet, pool::WorkerPool},
};

pub struct Connection {
    pool: WorkerPool,
    completion_registry: HashMap<u64, tokio::sync::Notify>,
    completed_responses: HashMap<u64, DbResponse>,
    id_counter: u64,
}

impl Connection {
    pub fn new(pool: WorkerPool) -> Self {
        Self {
            pool,
            completed_responses: HashMap::new(),
            id_counter: 0,
            completion_registry: HashMap::new(),
        }
    }

    fn get_next_id(&mut self) -> u64 {
        let id = self.id_counter;
        self.id_counter += 1;
        id
    }

    async fn send_and_await(&mut self, request: DbRequest) -> DbResponse {
        let req_id = request.request_id;

        let notify = tokio::sync::Notify::new();
        self.completion_registry.insert(req_id, notify);

        self.pool.send_request_async(request).await;

        loop {
            // See if we can receive some responses without waiting (and notify their owners)
            while let Ok(response) = self.pool.try_recv_response() {
                let recvd_req_id = response.request_id;
                self.completed_responses.insert(recvd_req_id, response);

                if let Some(notify) = self.completion_registry.get(&recvd_req_id) {
                    notify.notify_one();
                } else {
                    panic!("Received response for unknown request");
                }
            }

            // Wait until we are notified, or 1 ms
            let our_notification = self.completion_registry[&req_id].notified();
            match tokio::time::timeout(Duration::from_millis(1), our_notification).await {
                Ok(()) => {
                    self.completion_registry.remove(&req_id);
                    break;
                }
                Err(_) => continue,
            }
        }

        self.completed_responses
            .remove(&req_id)
            .expect("We should have been notified of our response")
    }

    pub async fn create(
        &'_ mut self,
        name: PathBuf,
        configuration: DbConfiguration,
    ) -> Result<DbHandle<'_>, DbError> {
        let request = DbRequest {
            request_id: self.get_next_id(),
            request: DbOperation::Create {
                name,
                configuration,
            },
        };

        let res = self.send_and_await(request).await;

        let database = match res.response? {
            DbRet::DbHandle(db) => db,
            _ => panic!("Unexpected response"),
        };

        self.pool.register_db_async(database).await;

        Ok(DbHandle { connection: self })
    }
    pub async fn open(&'_ mut self, name: PathBuf) -> Result<DbHandle<'_>, DbError> {
        let request = DbRequest {
            request_id: self.get_next_id(),
            request: DbOperation::Open { name },
        };

        let res = self.send_and_await(request).await;

        let database = match res.response? {
            DbRet::DbHandle(db) => db,
            _ => panic!("Unexpected response"),
        };

        self.pool.register_db_async(database).await;

        Ok(DbHandle { connection: self })
    }
}

pub struct DbHandle<'c> {
    connection: &'c mut Connection,
}

impl<'c> DbHandle<'c> {
    pub async fn get(&mut self, key: u64) -> Result<Option<u64>, DbError> {
        let request = DbRequest {
            request_id: self.connection.get_next_id(),
            request: DbOperation::Get { key },
        };

        let res = self.connection.send_and_await(request).await;

        match res.response? {
            DbRet::Value(val) => Ok(val),
            _ => panic!("Unexpected response"),
        }
    }
    pub async fn put(&mut self, key: u64, value: u64) -> Result<(), DbError> {
        let request = DbRequest {
            request_id: self.connection.get_next_id(),
            request: DbOperation::Put { key, value },
        };

        let res = self.connection.send_and_await(request).await;

        match res.response? {
            DbRet::None => Ok(()),
            _ => panic!("Unexpected response"),
        }
    }
    pub async fn delete(&mut self, key: u64) -> Result<(), DbError> {
        let request = DbRequest {
            request_id: self.connection.get_next_id(),
            request: DbOperation::Delete { key },
        };

        let res = self.connection.send_and_await(request).await;

        match res.response? {
            DbRet::None => Ok(()),
            _ => panic!("Unexpected response"),
        }
    }
    pub async fn flush(&mut self) -> Result<(), DbError> {
        let request = DbRequest {
            request_id: self.connection.get_next_id(),
            request: DbOperation::Flush,
        };

        let res = self.connection.send_and_await(request).await;

        match res.response? {
            DbRet::None => Ok(()),
            _ => panic!("Unexpected response"),
        }
    }
}

mod tests {
    use crate::LsmConfiguration;

    use super::*;

    #[tokio::test]
    async fn test_tokio_executor() {
        let pool = WorkerPool::new(3, 50).unwrap();
        let mut connection = Connection::new(pool);

        let mut database = connection
            .create(
                PathBuf::from("testdb"),
                DbConfiguration {
                    lsm_configuration: LsmConfiguration {
                        size_ratio: 4,
                        memtable_capacity: 100,
                        bloom_filter_bits: 5,
                    },
                    buffer_pool_capacity: 20,
                    write_buffering: 1,
                    readahead_buffering: 1,
                    wal_buffer_size: None,
                },
            )
            .await
            .unwrap();

        database.put(10, 25).await.unwrap();
        database.put(20, 35).await.unwrap();
        database.put(30, 45).await.unwrap();

        assert_eq!(database.get(0).await.unwrap(), None);
        assert_eq!(database.get(10).await.unwrap(), Some(25));
        assert_eq!(database.get(20).await.unwrap(), Some(35));
        assert_eq!(database.get(30).await.unwrap(), Some(45));

        database.flush().await.unwrap();
    }
}
