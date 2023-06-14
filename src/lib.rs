use std::path::Path;
use std::time::Duration;

use futures::{StreamExt, TryStreamExt};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Row, SqlitePool};

const DB_INIT_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS my_table (
    id INTEGER PRIMARY KEY,
    batch_id INTEGER
);

CREATE INDEX IF NOT EXISTS my_table_name ON my_table (
    batch_id ASC,
    id ASC
);
"#;

struct DatabaseAccess {
    pool: SqlitePool,
}

impl DatabaseAccess {
    async fn connect(db_file: impl AsRef<Path>) -> DatabaseAccess {
        let pool = SqlitePoolOptions::new()
            .acquire_timeout(Duration::from_secs(600))
            .max_connections(50)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(db_file)
                    .create_if_missing(true)
                    .synchronous(SqliteSynchronous::Full)
                    .journal_mode(SqliteJournalMode::Wal)
                    .busy_timeout(Duration::from_secs(600)),
            )
            .await
            .unwrap();

        DatabaseAccess::init_db(&pool).await;

        DatabaseAccess { pool }
    }

    async fn init_db(pool: &SqlitePool) {
        let mut results = sqlx::query(DB_INIT_SQL).execute_many(pool).await;
        while results.try_next().await.unwrap().is_some() {}
    }

    async fn insert(&self, batch_id: i64) -> i64 {
        let result = sqlx::query("INSERT INTO my_table (batch_id) VALUES (?) RETURNING id")
            .bind(batch_id)
            .fetch_one(&self.pool)
            .await
            .unwrap();
        result.get(0)
    }

    async fn full_insert(&self, batch_id: i64) -> i64 {
        let mut results = sqlx::query("INSERT INTO my_table (batch_id) VALUES (?) RETURNING id")
            .bind(batch_id)
            .fetch(&self.pool)
            .fuse();

        let Some(row) = results.try_next().await.unwrap() else {
            panic!("insert failed");
        };

        while results.next().await.is_some() {}
        row.get(0)
    }

    async fn select(&self, batch_id: i64) -> Vec<i64> {
        let mut r = sqlx::query("SELECT id FROM my_table where batch_id = ? ORDER BY id")
            .bind(batch_id)
            .fetch(&self.pool);

        let mut result = Vec::new();
        while let Some(row) = r.try_next().await.unwrap() {
            result.push(row.get(0));
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use rstest::{fixture, rstest};
    use tempfile::TempDir;

    use super::*;

    struct Fixture {
        _tempdir: TempDir,
        db_access: DatabaseAccess,
    }

    #[fixture]
    async fn test_db() -> Fixture {
        let tempdir = tempfile::tempdir().unwrap();
        let db_file = tempdir.path().join("catalog.db");
        let db_access = DatabaseAccess::connect(db_file).await;

        Fixture {
            _tempdir: tempdir,
            db_access,
        }
    }

    #[rstest]
    #[awt]
    #[tokio::test]
    async fn flaky(#[future] test_db: Fixture) {
        for i in 0..10000 {
            let id1 = test_db.db_access.insert(i).await;
            let id2 = test_db.db_access.insert(i).await;
            assert_eq!(test_db.db_access.select(i).await, vec![id1, id2])
        }
    }

    #[rstest]
    #[awt]
    #[tokio::test]
    async fn works(#[future] test_db: Fixture) {
        for i in 0..10000 {
            let id1 = test_db.db_access.full_insert(i).await;
            let id2 = test_db.db_access.full_insert(i).await;
            assert_eq!(test_db.db_access.select(i).await, vec![id1, id2])
        }
    }
}
