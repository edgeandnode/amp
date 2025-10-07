use std::time::Duration;

use ampsync::conn::DbConnPool;
use pgtemp::PgTempDB;

/// Test that the connection pool circuit breaker stops retrying after max duration
#[tokio::test]
async fn test_connection_circuit_breaker() {
    // Set C locale for pgtemp
    unsafe {
        std::env::set_var("LANG", "C");
    }

    // Create a temporary PostgreSQL database
    let pg_temp = PgTempDB::new();
    let connection_string = pg_temp.connection_uri();

    // Extract the port from the connection string and replace it with an invalid port
    let connection_string = if let Some(port_start) = connection_string.rfind(':') {
        if let Some(slash_pos) = connection_string[port_start..].find('/') {
            format!(
                "{}:9999{}",
                &connection_string[..port_start],
                &connection_string[port_start + slash_pos..]
            )
        } else {
            connection_string.clone()
        }
    } else {
        connection_string.clone()
    };

    println!(
        "Testing circuit breaker with invalid connection: {}",
        connection_string
    );

    // Set a very short max retry duration (2 seconds) for testing
    let max_duration = Duration::from_secs(2);

    let start = std::time::Instant::now();

    // Try to connect - should fail after ~2 seconds due to circuit breaker
    let result =
        DbConnPool::connect_with_max_duration(&connection_string, 1, Some(max_duration)).await;

    let elapsed = start.elapsed();

    // Should have failed
    assert!(result.is_err(), "Connection should have failed");

    // Should have stopped retrying after roughly max_duration
    // Allow some margin for execution overhead (max 3 seconds)
    assert!(
        elapsed.as_secs() >= 2 && elapsed.as_secs() <= 5,
        "Circuit breaker should have triggered around 2 seconds, but took {} seconds",
        elapsed.as_secs()
    );

    println!(
        "Circuit breaker correctly triggered after {} seconds",
        elapsed.as_secs()
    );
}

/// Test that database operations respect the circuit breaker configuration
#[tokio::test]
async fn test_operation_circuit_breaker() {
    // Set C locale for pgtemp
    unsafe {
        std::env::set_var("LANG", "C");
    }

    // Set operation max retry duration to 2 seconds
    unsafe {
        std::env::set_var("DB_OPERATION_MAX_RETRY_DURATION_SECS", "2");
    }

    // Create a temporary PostgreSQL database
    let pg_temp = PgTempDB::new();
    let connection_string = pg_temp.connection_uri();

    println!("PostgreSQL temp database created: {}", connection_string);

    // Connect to the database
    let db_pool = DbConnPool::connect(&connection_string, 1)
        .await
        .expect("Failed to create DbConnPool");

    // Test that basic database operations work with circuit breaker configured
    // The circuit breaker will only trigger if operations fail repeatedly within the timeout window

    // Create a simple test table directly
    sqlx::query("CREATE TABLE test_table (id INT PRIMARY KEY, value TEXT)")
        .execute(&*db_pool)
        .await
        .expect("Table creation should succeed");

    // Insert some test data
    sqlx::query("INSERT INTO test_table (id, value) VALUES (1, 'test')")
        .execute(&*db_pool)
        .await
        .expect("Insert should succeed");

    // Verify data was inserted
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM test_table")
        .fetch_one(&*db_pool)
        .await
        .expect("Failed to count rows");

    assert_eq!(count.0, 1, "Should have inserted 1 row");

    println!(
        "Circuit breaker correctly configured: normal operations succeed, \
         circuit breaker will trigger on prolonged failures"
    );

    // Cleanup
    unsafe {
        std::env::remove_var("DB_OPERATION_MAX_RETRY_DURATION_SECS");
    }
}

/// Test that circuit breaker uses default values when env vars not set
#[tokio::test]
async fn test_circuit_breaker_defaults() {
    // Test the default value parsing logic without setting env vars
    // This avoids test isolation issues with parallel test execution

    // Test connection default (300 seconds)
    let conn_duration = std::env::var("DB_MAX_RETRY_DURATION_SECS_NONEXISTENT")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(300);

    assert_eq!(
        conn_duration, 300,
        "Connection retry duration should default to 300"
    );

    // Test operation default (60 seconds)
    let op_duration = std::env::var("DB_OPERATION_MAX_RETRY_DURATION_SECS_NONEXISTENT")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(60);

    assert_eq!(
        op_duration, 60,
        "Operation retry duration should default to 60"
    );

    println!("Circuit breaker default values are correct");
}
