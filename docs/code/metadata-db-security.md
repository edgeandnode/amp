---
name: "metadata-db-security"
description: "Security patterns for database operations, SQL injection prevention. Load when modifying metadata-db crate code"
type: crate
scope: "crate:metadata-db"
---

# Security Guidelines for metadata-db

**CRITICAL: This document contains mandatory security requirements for all code contributions to the `metadata-db` crate. These guidelines ensure secure database operations and prevent common security vulnerabilities.**

## 🔐 Security Checklist

### Database Security Requirements

**Connection Security:**
- [ ] **🔐 Encryption in Transit**: All database connections MUST use TLS/SSL encryption. Never allow unencrypted database traffic
- [ ] **🔐 Connection Pooling**: Use `sqlx::PgPool` for connection management - never create direct connections without pooling 
- [ ] **🔐 Connection String Security**: Database connection strings MUST NOT contain credentials in code. Use environment variables only
- [ ] **🔐 Connection Timeouts**: Set appropriate connection and query timeout values to prevent resource exhaustion
- [ ] **🔐 Connection Limits**: Configure maximum connection pool size based on expected load and database capacity

**SQL Injection Prevention:**
- [ ] **🚫 Parameter Binding**: ALL SQL queries MUST use parameter binding (`$1`, `$2`, etc.) - NEVER string concatenation or formatting
- [ ] **🚫 Dynamic Query Construction**: Avoid building SQL queries from user input. Use static queries with parameters
- [ ] **🔍 Input Validation**: Validate all input parameters before binding to SQL queries (length, type, format)
- [ ] **🔍 SQL Review**: Every SQL query must be reviewed for injection vulnerabilities before merging
- [ ] **✅ Use `sqlx::query_as`**: Always use `sqlx::query_as` with proper parameter binding, never raw `execute` with formatted strings
- [ ] **✅ Use `sqlx::QueryBuilder`**: For dynamic queries, use `QueryBuilder` with `.push_bind()` method for safe parameter binding

```rust
// ✅ SECURE: Proper parameter binding
async fn secure_query_with_parameter_binding() -> Result<Vec<Job>, sqlx::Error> {
    let query = "SELECT id FROM jobs WHERE node_id = $1 AND status = $2";
    sqlx::query_as(query).bind(node_id).bind(status).fetch_all(exe).await
}

// 🚫 INSECURE: String formatting (SQL injection risk)
async fn vulnerable_query_with_string_formatting() -> Result<Vec<Job>, sqlx::Error> {
    let query = format!("SELECT id FROM jobs WHERE node_id = '{}'", node_id);
    sqlx::query(query).fetch_all(exe).await
}
```

### Access Control and Authorization

**Database Permissions:**
- [ ] **🔒 Principle of Least Privilege**: Database user must have ONLY the minimum permissions required (no admin/superuser rights)
- [ ] **🔒 Schema Isolation**: Use dedicated database schemas for isolation from other applications
- [ ] **🔒 Table-Level Permissions**: Grant permissions only on required tables, not database-wide
- [ ] **🔒 Function Restrictions**: Restrict access to system functions and administrative procedures
- [ ] **🔒 Role-Based Access**: Use PostgreSQL roles for permission management, not individual user grants

**Runtime Security:**
- [ ] **🛡️ Error Information Disclosure**: Database errors MUST NOT leak sensitive information in production responses
- [ ] **🛡️ Query Logging**: Ensure sensitive data is not logged in query logs or tracing output  
- [ ] **🛡️ Connection String Logging**: Never log database connection strings or credentials
- [ ] **📊 Audit Trail**: Log all significant database operations (inserts, updates, deletes) for audit purposes
- [ ] **⏰ Query Timeouts**: Implement query timeouts to prevent long-running queries from causing denial of service

### Data Protection

**Sensitive Data Handling:**
- [ ] **🔐 Data at Rest**: Ensure database encryption at rest is enabled (outside application scope but verify with infrastructure)
- [ ] **🔐 PII Identification**: Identify any Personally Identifiable Information (PII) and ensure appropriate protection
- [ ] **🔐 Secrets Management**: Never store API keys, passwords, or other secrets in database fields without encryption
- [ ] **🔐 Data Masking**: Implement data masking for sensitive fields in non-production environments
- [ ] **🗑️ Data Retention**: Implement appropriate data retention policies with automated cleanup where required

**Transaction Security:**
- [ ] **⚛️ Transaction Boundaries**: Ensure all multistep operations use proper database transactions for consistency
- [ ] **⚛️ Isolation Levels**: Use appropriate PostgreSQL isolation levels for concurrent operations
- [ ] **⚛️ Deadlock Prevention**: Design transaction patterns to prevent deadlocks in high-concurrency scenarios
- [ ] **🔄 Retry Logic**: Implement safe retry patterns for transient failures without creating duplicate data

### Security Testing and Validation

**Input Validation Testing:**
- [ ] **🧪 Boundary Testing**: Test input validation with edge cases (empty strings, nulls, maximum lengths, special characters)
- [ ] **🧪 SQL Injection Testing**: Include specific tests that attempt SQL injection on all input parameters
- [ ] **🧪 Parameter Type Testing**: Verify that incorrect parameter types are properly rejected
- [ ] **🧪 Unicode Testing**: Test input handling with various Unicode characters and encodings
- [ ] **🧪 Concurrent Access Testing**: Test behavior under concurrent access scenarios

**Security Code Review:**
- [ ] **👁️ SQL Query Review**: Every SQL query must be manually reviewed by another developer for security issues
- [ ] **👁️ Parameter Binding Review**: Verify that all queries use proper parameter binding without concatenation
- [ ] **👁️ Error Handling Review**: Ensure error messages don't expose sensitive database information
- [ ] **👁️ Transaction Review**: Verify transaction boundaries are appropriate and handle all error conditions
- [ ] **👁️ Permission Review**: Verify that database operations respect intended permission boundaries

### Security Monitoring and Logging

**Development-Time Security Checks:**
- [ ] **🔍 Static Analysis**: Run security linters and static analysis tools on all code changes
- [ ] **🔍 Dependency Scanning**: Check for known vulnerabilities in dependencies before merging
- [ ] **🔍 Secret Detection**: Scan commits for accidentally included credentials or secrets
- [ ] **🔍 Query Analysis**: Review all SQL queries for potential injection vulnerabilities

**Runtime Security Monitoring:**
- [ ] **📊 Database Error Logging**: Log database errors without exposing sensitive details to clients
- [ ] **📊 Failed Authentication Tracking**: Monitor and log failed database connection attempts
- [ ] **📊 Query Performance Tracking**: Log slow queries that might indicate attack patterns
- [ ] **📊 Connection Pool Monitoring**: Track connection pool usage to detect potential abuse

### OWASP Top 10 Mitigation

**Injection Prevention (A03:2021):**
- [ ] **🛡️ Parameterized Queries**: All SQL queries use parameterized statements, never dynamic query construction
- [ ] **🛡️ Input Validation**: Validate all inputs at the application boundary before database operations
- [ ] **🛡️ Escape Special Characters**: When parameterization isn't possible, properly escape all special characters
- [ ] **🛡️ Stored Procedures**: If using stored procedures, ensure they also use parameterized statements internally

**Security Misconfiguration (A05:2021):**
- [ ] **⚙️ Default Credentials**: Verify no default or weak database credentials are used
- [ ] **⚙️ Error Messages**: Configure database to return minimal error information to applications
- [ ] **⚙️ Database Hardening**: Follow PostgreSQL security hardening guidelines
- [ ] **⚙️ Network Security**: Ensure database is not accessible from untrusted networks

**Vulnerable Components (A06:2021):**
- [ ] **🔄 Dependency Updates**: Regularly update `sqlx` and other database-related dependencies
- [ ] **🔄 Security Patches**: Monitor and apply PostgreSQL security patches promptly  
- [ ] **🔄 Vulnerability Scanning**: Use automated tools to scan for known vulnerabilities in dependencies

### Secure Development Practices

**Code Development:**
- [ ] **🔒 Security-First Code Review**: Review all database code for security vulnerabilities before merging
- [ ] **🔒 Parameter Binding Validation**: Verify every SQL query uses proper parameter binding (`$1`, `$2`, etc.)
- [ ] **🔒 Error Handling Review**: Ensure error messages don't leak sensitive database schema or data
- [ ] **🔒 Input Validation**: Validate all parameters for type, length, and format before database operations

**Security Testing:**
- [ ] **🧪 SQL Injection Tests**: Write tests that attempt SQL injection on all input parameters
- [ ] **🧪 Error Handling Tests**: Verify error messages are safe for client consumption
- [ ] **🧪 Input Boundary Tests**: Test edge cases (nulls, empty strings, oversized inputs, special characters)
- [ ] **🧪 Concurrency Tests**: Test database operations under concurrent access scenarios

## Security Anti-Patterns and Best Practices

**❌ Security Anti-Patterns to Avoid:**

```rust
// 🚫 NEVER: String interpolation in SQL queries
fn vulnerable_string_interpolation(user_id: i64) {
    let query = format!("SELECT * FROM users WHERE id = {}", user_id);
}

// 🚫 NEVER: Concatenating user input
fn vulnerable_string_concatenation(user_name: &str) {
    let query = "SELECT * FROM users WHERE name = '".to_string() + user_name + "'";
}

// 🚫 NEVER: Logging sensitive data
fn vulnerable_logging_secrets(db_password: &str) {
    tracing::info!("Database password: {}", db_password);
}

// 🚫 NEVER: Returning raw database errors to clients
fn vulnerable_error_exposure(sqlx_error: sqlx::Error) -> Result<(), String> {
    Err(sqlx_error.to_string()) // May leak schema information
}

// 🚫 NEVER: Using production data in tests
fn vulnerable_test_data_usage() {
    let test_db = production_connection_string; // Use temp databases only
}
```

**✅ Secure Coding Patterns:**

```rust
// ✅ SECURE: Proper parameter binding (single line)
async fn secure_parameter_binding_simple<'c, E>(exe: E, user_id: i64) -> Result<Option<User>, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let query = "SELECT * FROM users WHERE id = $1";
    sqlx::query_as(query).bind(user_id).fetch_optional(exe).await
}

// ✅ SECURE: Proper parameter binding (multiline)
async fn secure_parameter_binding_multiline<'c, E>(exe: E, user_id: i64, status: &str) -> Result<Option<User>, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT u.id, u.name, u.email, u.created_at
        FROM users u
        WHERE u.id = $1 AND u.status = $2
    "#};
    sqlx::query_as(query).bind(user_id).bind(status).fetch_optional(exe).await
}

// ✅ SECURE: Input validation before database operations  
fn secure_input_validation(user_name: &str) -> Result<(), Error> {
    if user_name.len() > 255 {
        return Err(Error::InvalidInput("Name too long".to_string()));
    }
    Ok(())
}

// ✅ SECURE: Safe error handling without information disclosure
fn secure_error_handling(sqlx_error: sqlx::Error) -> Result<(), Error> {
    match sqlx_error {
        sqlx::Error::RowNotFound => Err(Error::ResourceNotFound),
        _ => {
            tracing::error!("Database error: {:?}", sqlx_error);
            Err(Error::DatabaseError)
        }
    }
}
```

## PII (Personally Identifiable Information) in metadata-db Context

**🔐 CRITICAL: Understand what constitutes PII in the metadata-db crate to ensure proper data protection.**

### What is NOT PII in metadata-db

The metadata-db crate primarily handles blockchain infrastructure metadata, which is generally **NOT considered PII**:

- **📊 Job metadata**: Job IDs, status, descriptions, creation timestamps
- **🗄️ Location metadata**: Dataset locations, file paths, block ranges, table names
- **⚙️ Worker metadata**: Worker node IDs (when they are system-generated identifiers), heartbeat timestamps
- **📁 File metadata**: Parquet file paths, block ranges, file sizes, checksums
- **🔗 Blockchain data references**: Block numbers, transaction hashes, dataset names

### What COULD be PII in metadata-db

**⚠️ Be cautious with these data types that might contain PII:**

- **📝 Job descriptions**: If they contain human-readable names, email addresses, or personal references
- **🏷️ Custom metadata fields**: JSONB fields that might store user-provided information
- **📋 Worker node identifiers**: If they contain hostnames, IP addresses, or usernames
- **🔗 External references**: URLs, file paths, or identifiers that might contain personal information
- **📧 Any user-provided strings**: Comments, labels, or descriptions entered by users

### PII Protection Requirements

**If PII is identified in metadata-db:**

- [ ] **🔐 Data Classification**: Clearly document which fields contain PII
- [ ] **🔒 Access Control**: Implement stricter access controls for PII-containing tables
- [ ] **🗑️ Data Retention**: Establish retention policies and automated cleanup for PII
- [ ] **🎭 Data Masking**: Mask PII in non-production environments
- [ ] **📊 Audit Logging**: Enhanced logging for operations involving PII
- [ ] **🔍 Query Restrictions**: Additional validation for queries accessing PII fields

### PII Detection Guidelines

**During development, check for PII in:**

1. **Database schema changes** - Review new columns for potential PII storage
2. **Input validation** - Identify if user input might contain personal information  
3. **Logging and tracing** - Ensure PII is not logged in debug output
4. **Error messages** - Verify error responses don't expose PII
5. **External integrations** - Check if external data sources introduce PII

### Non-PII Assumptions

**The following are generally safe assumptions for metadata-db:**

- System-generated IDs (job IDs, location IDs) are not PII
- Blockchain-specific data (block numbers, hashes) are not PII
- File system paths (when they don't contain usernames) are not PII
- Timestamps and counters are not PII
- Database performance metrics are not PII

**💡 Remember: When in doubt, treat data as potentially containing PII and apply appropriate protections. The cost of over-protection is much lower than the cost of a data breach.**

## Security Review Process

**🔍 Security Review Questions:**

Before merging any metadata-db changes, ask yourself:

1. **Could this code be vulnerable to SQL injection?**
2. **Does this code handle all input validation properly?**
3. **Are error messages safe to expose to users?**
4. **Could this code leak sensitive information in logs?**
5. **Are all database operations properly transactional?**
6. **Does this code follow the principle of least privilege?**
7. **Is all sensitive data properly protected?**
8. **Could this code be exploited for privilege escalation?**

## Security Incident Response

**🚨 Security Incident Response:**

If a security vulnerability is discovered:

1. **Immediate**: Stop any deployment of affected code
2. **Assessment**: Evaluate the scope and impact of the vulnerability  
3. **Mitigation**: Implement and deploy fixes as quickly as possible
4. **Communication**: Notify relevant stakeholders about the issue and resolution
5. **Prevention**: Update security practices to prevent similar issues

## Security Standards Reference

This security framework follows established industry standards:

- **OWASP Application Security Verification Standard (ASVS)** - Web application security requirements
- **PostgreSQL Security** - Database-specific security best practices  
- **Rust Security Guidelines** - Memory safety and secure coding patterns

**🛡️ Remember: Security is not optional. These requirements protect user data, maintain system integrity, and prevent security vulnerabilities.**
