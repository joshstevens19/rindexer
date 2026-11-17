//! Requires Docker. The public test key/certificates in fixtures are only for this test.
//! One test keeps DATABASE_URL / SSL_CERT_FILE changes isolated from other cases.
use rindexer::PostgresClient;
use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::postgres::Postgres;

#[tokio::test]
async fn verifies_absolute_dns_names_and_rejects_invalid_tls() {
    let pg = Postgres::default()
        .with_copy_to("/tmp/server.pem", include_bytes!("fixtures/postgres_tls/server.pem").to_vec())
        .with_copy_to("/tmp/server.key", include_bytes!("fixtures/postgres_tls/server.key").to_vec())
        .with_cmd([
            "sh", "-c",
            "chown postgres /tmp/server.key && chmod 600 /tmp/server.key && exec docker-entrypoint.sh postgres -c ssl=on -c ssl_cert_file=/tmp/server.pem -c ssl_key_file=/tmp/server.key",
        ])
        .start().await.unwrap();
    let port = pg.get_host_port_ipv4(5432).await.unwrap();
    let host = tokio::net::lookup_host((pg.get_host().await.unwrap().to_string(), port))
        .await
        .unwrap()
        .find(|addr| addr.is_ipv4())
        .unwrap()
        .ip();
    std::env::set_var(
        "SSL_CERT_FILE",
        concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/postgres_tls/ca.pem"),
    );
    std::env::set_var("DATABASE_POOL_SIZE", "1");
    let url = format!("postgresql://postgres:postgres@postgres.test.:{port}/postgres?hostaddr={host}&sslmode=require");
    std::env::set_var("DATABASE_URL", &url);
    let client = PostgresClient::new().await.expect("trusted trailing-dot SAN must work");
    let row = client
        .query_one("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()", &[])
        .await
        .unwrap();
    assert!(row.get::<_, bool>(0));
    drop(client);

    // Both failures must stay failures even though this server also accepts plaintext.
    std::env::set_var("DATABASE_URL", url.replace("postgres.test.", "wrong.test."));
    assert!(PostgresClient::new().await.is_err(), "wrong hostname accepted");
    // Percent-encoding must not bypass sslmode=require and trigger plaintext fallback.
    std::env::set_var(
        "DATABASE_URL",
        url.replace("postgres.test.", "wrong.test.")
            .replace("sslmode=require", "sslmode=%72equire"),
    );
    assert!(PostgresClient::new().await.is_err(), "required TLS downgraded to plaintext");

    std::env::set_var("DATABASE_URL", &url);
    let empty_roots = tempfile::tempdir().unwrap();
    let ca_file = empty_roots.path().join("empty.pem");
    std::fs::write(&ca_file, "").unwrap();
    std::env::set_var("SSL_CERT_FILE", ca_file);
    std::env::set_var("SSL_CERT_DIR", empty_roots.path());
    assert!(PostgresClient::new().await.is_err(), "untrusted certificate accepted");

    std::env::set_var("DATABASE_URL", url.replace("sslmode=require", "sslmode=disable"));
    let client = PostgresClient::new().await.expect("explicit local plaintext must still work");
    let row = client
        .query_one("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()", &[])
        .await
        .unwrap();
    assert!(!row.get::<_, bool>(0));
}
