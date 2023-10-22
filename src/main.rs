use std::{env, pin::Pin, str::FromStr};

use mobc::Pool;
use mobc_postgres::PgConnectionManager;
use rand::Rng;
use tokio::sync::mpsc::Receiver;
use tokio_postgres::{
    binary_copy::BinaryCopyInWriter,
    types::{ToSql, Type},
    Config, NoTls,
};

struct MagicSink {
    db: DBPool,
    writer: Option<Pin<Box<BinaryCopyInWriter>>>,
    types: Vec<tokio_postgres::types::Type>,
    stmt: String,
}

#[derive(Debug)]
enum MagicSinkError {
    CouldNotGetDatabaseConnection,
    CouldNotCopy,
}

impl MagicSink {
    async fn write(&mut self, data: &[&(dyn ToSql + Sync)]) -> Result<(), MagicSinkError> {
        if self.writer.is_none() {
            tracing::info!(msg = "trying to create connection", stmt = ?self.stmt);
            let connection = match self.db.get().await {
                Ok(con) => con,
                Err(e) => {
                    tracing::error!(e = ?e);
                    return Err(MagicSinkError::CouldNotGetDatabaseConnection);
                }
            };

            let Ok(sink) = connection.copy_in(&self.stmt).await else {
                return Err(MagicSinkError::CouldNotCopy);
            };
            let writer = BinaryCopyInWriter::new(sink, &self.types);
            self.writer = Some(Box::pin(writer));
        }

        if let Some(writer) = self.writer.as_mut() {
            if let Err(e) = writer.as_mut().write(&data).await {
                tracing::error!(e = ?e);
            }
        }

        Ok(())
    }

    async fn finish(mut self) {
        if let Some(writer) = self.writer.as_mut() {
            if let Err(e) = writer.as_mut().finish().await {
                tracing::error!(e = ?e);
            }
        }
    }

    fn new(db: DBPool, stmt: String, types: &[tokio_postgres::types::Type]) -> MagicSink {
        MagicSink {
            db,
            stmt,
            writer: None,
            types: types.to_vec(),
        }
    }
}

enum DbLogEvent {
    A { c1: i32, c2: i32 },
    B { c1: i32, c2: i32 },
    C { c1: i32, c2: i32 },
}

fn spawn_db_worker(mut rx_db: Receiver<DbLogEvent>, db_pool: DBPool) {
    tokio::spawn(async move {
        loop {
            let mut sink1 = MagicSink::new(
                db_pool.clone(),
                //host, sid, ip_address, blocked
                "COPY a(c1, c2) from stdin binary".to_string(),
                &[Type::INT4, Type::INT4],
            );
            let mut sink2 = MagicSink::new(
                db_pool.clone(),
                //host, sid, ip_address, blocked
                "COPY b(c1, c2) from stdin binary".to_string(),
                &[Type::INT4, Type::INT4],
            );
            let mut sink3 = MagicSink::new(
                db_pool.clone(),
                //host, sid, ip_address, blocked
                "COPY c(c1, c2) from stdin binary".to_string(),
                &[Type::INT4, Type::INT4],
            );
            for _ in 0..256 {
                if let Some(event) = rx_db.recv().await {
                    match event {
                        DbLogEvent::A { c1, c2 } => {
                            if let Err(e) = sink1.write(&[&c1, &c2]).await {
                                tracing::error!(e = ?e)
                            }
                        }
                        DbLogEvent::B { c1, c2 } => {
                            if let Err(e) = sink2.write(&[&c1, &c2]).await {
                                tracing::error!(e = ?e)
                            }
                        }
                        DbLogEvent::C { c1, c2 } => {
                            if let Err(e) = sink3.write(&[&c1, &c2]).await {
                                tracing::error!(e = ?e)
                            }
                        }
                    }
                }
            }

            let t = std::time::Instant::now();
            let _ =
                futures::future::join_all([sink1.finish(), sink2.finish(), sink3.finish()]).await;
            tracing::trace!(msg = "finishing copy", t = ?(std::time::Instant::now()-t))
        }
    });
}

type DBPool = Pool<PgConnectionManager<NoTls>>;
fn create_pool() -> std::result::Result<DBPool, mobc::Error<tokio_postgres::Error>> {
    let database_url = "postgres://postgres:password@localhost:5555";

    let config = Config::from_str(&database_url)?;

    let manager = PgConnectionManager::new(config, NoTls);
    Ok(Pool::builder().max_open(10).build(manager))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let db_pool = create_pool().expect("database pool can be created");
    let (tx_db, rx_db) = tokio::sync::mpsc::channel(4096);
    spawn_db_worker(rx_db, db_pool.clone());

    let _ = tokio::spawn(async move {
        loop {
            let chance = rand::thread_rng().gen_range(0..(1000 as u32));
            let _ = tx_db.send(DbLogEvent::A { c1: 1, c2: 2 }).await;
            if chance < 100 {
                let _ = tx_db.send(DbLogEvent::B { c1: 3, c2: 4 }).await;
            }
            if chance < 10 {
                let _ = tx_db.send(DbLogEvent::C { c1: 3, c2: 4 }).await;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await;
    Ok(())
}
