use std::env;
use clickhouse::Client;
use dotenv::dotenv;

pub struct ClickhouseConnection {
    url: String,
    user: String,
    password: String,
    database: String,
}

pub fn clickhouse_connection() -> Result<ClickhouseConnection, env::VarError> {
    dotenv().ok();

    let connection = ClickhouseConnection {
        url: env::var("CLICKHOUSE_URL")?,
        user: env::var("CLICKHOUSE_USER")?,
        password: env::var("CLICKHOUSE_PASSWORD")?,
        database: env::var("CLICKHOUSE_DATABASE=")?,
    };

    Ok(connection)
}

#[derive(thiserror::Error, Debug)]
pub enum ClickhouseConnectionError {
    #[error("The clickhouse env vars are wrong please check your environment: {0}")]
    ClickhouseConnectionConfigWrong(#[from] env::VarError),
}

pub struct ClickhouseClient {
    conn: Client
}

impl ClickhouseClient {
    pub async fn new() -> Result<Self, ClickhouseConnectionError> {
        let connection = clickhouse_connection()?;

        let client = Client::default()
            .with_url(connection.url)
            .with_user(connection.user)
            .with_password(connection.password)
            .with_database(connection.database);

        Ok(ClickhouseClient { conn: client })
    }
}