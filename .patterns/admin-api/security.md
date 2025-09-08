🛡️ `admin-api` Security Guidelines
==================================

**CRITICAL: This document contains mandatory security requirements for all code contributions to the `admin-api` crate. These guidelines ensure secure HTTP API operations and prevent common web application security vulnerabilities.**

## 🚨 CRITICAL NETWORK SECURITY WARNING

**⛔ NEVER EXPOSE ADMIN-API TO PUBLIC NETWORKS**

The `admin-api` crate provides **CRITICAL ADMINISTRATIVE CAPABILITIES** that can control and modify the entire Nozzle system. This API must NEVER be exposed directly to public networks or untrusted clients.

**🔐 Mandatory Network Isolation:**
- [ ] **🔒 Internal Networks Only**: Admin-API MUST only be accessible from trusted internal networks
- [ ] **🔒 VPN/Private Networks**: Access MUST be restricted through VPN or private network infrastructure  
- [ ] **🔒 Firewall Protection**: Implement strict firewall rules preventing external access
- [ ] **🔒 Network Segmentation**: Isolate admin-API in a dedicated management network segment

**🚧 Public Exposure Requirements:**
If admin functionality needs public exposure, you MUST:
- [ ] **🔐 Add Authentication Layer**: Implement a separate authentication/authorization service
- [ ] **🔐 Add Rate Limiting**: Implement aggressive rate limiting and DDoS protection
- [ ] **🔐 Add Audit Layer**: Add comprehensive request logging and monitoring
- [ ] **🔐 Add API Gateway**: Use an API gateway with security policies and request validation
- [ ] **🔐 Minimal Exposure**: Only expose the minimum required subset of functionality

**⚠️ WARNING: Direct public exposure of `admin-api` endpoints could result in:**
- Complete system compromise
- Unauthorized data access and modification
- Service disruption and denial of service
- Compliance violations and regulatory issues

## 🔐 Security Checklist

### HTTP API Security Requirements

**Plain HTTP Server:**
- [ ] **🌐 Plain HTTP Only**: `admin-api` serves plain HTTP and does NOT implement any transport security
- [ ] **🔒 Network Trust Assumption**: `admin-api` assumes it operates in a completely trusted network environment
- [ ] **🛡️ No Transport Security**: `admin-api` has NO built-in transport security mechanisms

**Input Validation and Injection Prevention:**
- [ ] **🚫 Parameter Validation**: ALL path parameters MUST be validated before use (type, format, range)
- [ ] **🚫 Query Parameter Validation**: Validate all query parameters for type safety and business logic constraints
- [ ] **🚫 Request Body Validation**: Use strongly-typed structs with serde validation for JSON payloads
- [ ] **🔍 Path Traversal Prevention**: Validate file paths and resource identifiers to prevent directory traversal
- [ ] **🔍 Header Validation**: Validate and sanitize all HTTP headers used in application logic
- [ ] **✅ Use Axum Extractors**: Always use Axum's type-safe extractors (`Path`, `Query`, `Json`) with proper error handling

```rust
// ✅ SECURE: Proper input validation and type safety
#[derive(Debug, serde::Deserialize)]
pub struct QueryParams {
    #[serde(default = "default_limit")]
    pub limit: usize,
}

pub async fn secure_handler(
    Path(id): Path<u64>,
    Query(params): Query<QueryParams>,
) -> Result<Json<Response>, BoxRequestError> {
    // Input validation happens automatically via Axum extractors
    if params.limit > MAX_LIMIT {
        return Err(Error::LimitTooLarge { limit: params.limit, max: MAX_LIMIT }.into());
    }
    Ok(Json(response))
}

// 🚫 INSECURE: String manipulation without validation
pub async fn vulnerable_handler(
    raw_path: String,
) -> Result<Json<Response>, BoxRequestError> {
    // Direct string manipulation opens injection vectors
    let file_path = format!("/data/{}", raw_path); // Path traversal risk
    std::fs::read_to_string(file_path) // Directory traversal vulnerability
}
```

### Network-Level Security (No Built-In Authentication)

**🚨 CRITICAL: `admin-api` is a plain HTTP server with NO built-in authentication or authorization mechanisms.**

**Network Security Requirements:**
- [ ] **🔒 Network-Level Access Control**: ALL authentication and authorization MUST be handled by network infrastructure or wrapper services
- [ ] **🔒 Trusted Network Assumption**: `admin-api` assumes ALL incoming requests come from trusted, authenticated sources
- [ ] **🔒 Operator Responsibility**: Service operators MUST provide authentication/authorization through external layers (reverse proxy, API gateway, VPN, etc.)
- [ ] **🔒 No Public Exposure**: `admin-api` MUST NEVER be directly accessible from untrusted networks
- [ ] **🔒 Infrastructure Security**: Network segmentation, firewall rules, and access controls are the ONLY security boundaries

**Request/Response Security:**
- [ ] **🛡️ Error Information Disclosure**: API errors MUST NOT leak internal system details or sensitive data
- [ ] **🛡️ Request Logging**: Ensure sensitive data (tokens, credentials) is not logged in request/response logs
- [ ] **🛡️ Response Headers**: Include security headers (Content-Type, X-Content-Type-Options, etc.)
- [ ] **📊 Audit Trail**: Log all significant administrative operations with user context for audit purposes
- [ ] **⏰ Request Timeouts**: Implement request timeouts to prevent resource exhaustion attacks
- [ ] **🛡️ CORS Policy**: Configure restrictive CORS policies for cross-origin requests

### Data Protection and Privacy

**Sensitive Data Handling:**
- [ ] **🔐 Data in Transit**: All API communications MUST use HTTPS with proper TLS configuration
- [ ] **🔐 PII Protection**: Identify and protect any Personally Identifiable Information in API responses
- [ ] **🔐 Secrets Management**: Never include secrets, API keys, or credentials in API responses or logs
- [ ] **🔐 Data Sanitization**: Sanitize all data before including in API responses (remove internal identifiers)
- [ ] **🗑️ Request Data Cleanup**: Clear sensitive request data from memory after processing
- [ ] **🔐 Response Filtering**: Ensure API responses only contain data the client is authorized to access

**HTTP Security Headers:**
- [ ] **🛡️ Content-Type**: Always set explicit Content-Type headers (application/json for API responses)
- [ ] **🛡️ X-Content-Type-Options**: Set "nosniff" to prevent MIME type confusion attacks
- [ ] **🛡️ X-Frame-Options**: Set "DENY" or "SAMEORIGIN" to prevent clickjacking
- [ ] **🛡️ X-XSS-Protection**: Enable XSS protection in browsers (though modern CSP is preferred)
- [ ] **🛡️ Content-Security-Policy**: Implement restrictive CSP for any HTML responses
- [ ] **🔄 Cache-Control**: Set appropriate cache headers for sensitive administrative data

### Security Testing and Validation

**API Security Testing:**
- [ ] **🧪 Input Boundary Testing**: Test all endpoints with edge cases (empty values, oversized inputs, invalid types)
- [ ] **🧪 Injection Testing**: Test for path traversal, command injection, and other injection attacks
- [ ] **🧪 Network Isolation Testing**: Test that endpoints are not accessible from untrusted networks
- [ ] **🧪 Error Handling Testing**: Ensure error responses don't leak sensitive information

**Security Code Review:**
- [ ] **👁️ Endpoint Security Review**: Every API endpoint must be manually reviewed for security vulnerabilities
- [ ] **👁️ Input Validation Review**: Verify that all inputs are properly validated and sanitized
- [ ] **👁️ Error Handling Review**: Ensure error responses don't expose sensitive system information
- [ ] **👁️ Network Security Review**: Verify endpoints assume trusted network environment
- [ ] **👁️ Data Exposure Review**: Verify no sensitive data is unnecessarily exposed in responses

### Security Monitoring and Logging

**Development-Time Security Checks:**
- [ ] **🔍 Static Analysis**: Run security linters and static analysis tools on all code changes
- [ ] **🔍 Dependency Scanning**: Check for known vulnerabilities in web framework dependencies
- [ ] **🔍 Secret Detection**: Scan commits for accidentally included API keys, tokens, or credentials
- [ ] **🔍 Endpoint Analysis**: Review all API endpoints for potential security vulnerabilities

**Runtime Security Monitoring:**
- [ ] **📊 API Error Logging**: Log API errors without exposing sensitive details to clients
- [ ] **📊 Request Performance Tracking**: Log slow requests that might indicate attack patterns
- [ ] **📊 Unusual Request Detection**: Monitor for unusual request patterns or payload sizes
- [ ] **📊 Administrative Operation Logging**: Log all significant administrative operations for audit trails

### OWASP Top 10 Mitigation

**Injection Prevention (A03:2021):**
- [ ] **🛡️ Input Validation**: Validate all API inputs using type-safe extractors and validation rules
- [ ] **🛡️ Path Traversal Prevention**: Sanitize file paths and prevent directory traversal attacks
- [ ] **🛡️ Command Injection Prevention**: Never execute system commands with user-provided input
- [ ] **🛡️ Template Injection Prevention**: Avoid dynamic template rendering with user input

**Security Misconfiguration (A05:2021):**
- [ ] **⚙️ Error Messages**: Configure API to return minimal error information without system details
- [ ] **⚙️ Server Hardening**: Follow HTTP server security hardening guidelines
- [ ] **⚙️ Network Security**: Ensure admin API is not accessible from untrusted networks
- [ ] **⚙️ Security Headers**: Implement recommended HTTP security headers where applicable

**Vulnerable Components (A06:2021):**
- [ ] **🔄 Dependency Updates**: Regularly update Axum, Tokio, and other HTTP/async dependencies
- [ ] **🔄 Security Patches**: Monitor and apply Rust ecosystem security patches promptly
- [ ] **🔄 Vulnerability Scanning**: Use automated tools to scan for known vulnerabilities in dependencies

**Identification and Authentication Failures (A07:2021) - Operator Responsibility:**
- [ ] **🔐 External Authentication**: Service operators MUST implement authentication at the infrastructure layer
- [ ] **🔐 Network-Level Protection**: Operators MUST use network controls to prevent unauthorized access
- [ ] **🔐 Infrastructure Security**: All authentication and authorization is handled outside `admin-api`

### Secure Development Practices

**Code Development:**
- [ ] **🔒 Security-First Code Review**: Review all API endpoint code for security vulnerabilities before merging
- [ ] **🔒 Input Validation**: Verify every endpoint properly validates and sanitizes all input parameters
- [ ] **🔒 Error Handling Review**: Ensure error messages don't leak sensitive system information or data
- [ ] **🔒 Network Trust Validation**: Validate endpoints assume requests come from trusted sources only

**Security Testing:**
- [ ] **🧪 Injection Attack Tests**: Write tests that attempt various injection attacks on all input parameters
- [ ] **🧪 Error Handling Tests**: Verify error messages are safe for client consumption
- [ ] **🧪 Input Boundary Tests**: Test edge cases (nulls, empty strings, oversized inputs, special characters)
- [ ] **🧪 Network Trust Tests**: Test that endpoints function correctly in trusted network environment

## Security Anti-Patterns and Best Practices

**❌ Security Anti-Patterns to Avoid:**

```rust
// 🚫 NEVER: Direct string manipulation for paths
fn vulnerable_path_handling(user_path: &str) -> String {
    format!("/data/{}", user_path) // Path traversal vulnerability
}

// 🚫 NEVER: Exposing internal system details in API responses
fn vulnerable_error_exposure(internal_error: anyhow::Error) -> Result<(), String> {
    Err(internal_error.to_string()) // May leak system information
}

// 🚫 NEVER: Logging sensitive request data
fn vulnerable_logging_tokens(auth_token: &str) {
    tracing::info!("Processing request with token: {}", auth_token);
}

// 🚫 NEVER: Exposing sensitive system internals
fn vulnerable_system_exposure() -> Json<SystemInfo> {
    // Exposing too much internal system information
    Json(SystemInfo {
        database_connection_string: get_db_conn(), // Never expose connection details
        internal_service_urls: get_internal_urls(), // Never expose internal URLs
        system_secrets: get_secrets(), // Never expose secrets
    })
}

// 🚫 NEVER: Using weak input validation
fn vulnerable_weak_validation(input: String) {
    if !input.is_empty() { // Insufficient validation
        process_input(input);
    }
}
```

**✅ Secure Coding Patterns:**

```rust
// ✅ SECURE: Proper input validation using Axum extractors
#[derive(Debug, serde::Deserialize)]
pub struct QueryParams {
    #[serde(default = "default_limit")]
    pub limit: usize,
}

#[tracing::instrument(skip_all, err)]
pub async fn secure_handler(
    State(ctx): State<Ctx>,
    Path(id): Path<ResourceId>,
    Query(params): Query<QueryParams>,
) -> Result<Json<ResourceResponse>, BoxRequestError> {
    // Input validation with business logic
    if params.limit > MAX_LIMIT {
        return Err(Error::LimitTooLarge { 
            limit: params.limit, 
            max: MAX_LIMIT 
        }.into());
    }
    
    // Safe resource access with proper error handling
    let resource = ctx.store
        .get_resource(id)
        .await
        .ok_or_else(|| Error::ResourceNotFound { id })?;
    
    Ok(Json(ResourceResponse::from(resource)))
}

// ✅ SECURE: Safe error handling without information disclosure
impl RequestError for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::ResourceNotFound { .. } => "RESOURCE_NOT_FOUND",
            Error::InvalidInput { .. } => "INVALID_INPUT",
            Error::InternalError => "INTERNAL_ERROR", // Never expose internal details
        }
    }
    
    fn status_code(&self) -> StatusCode {
        match self {
            Error::ResourceNotFound { .. } => StatusCode::NOT_FOUND,
            Error::InvalidInput { .. } => StatusCode::BAD_REQUEST,
            Error::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

// ✅ SECURE: Proper response data filtering
pub fn create_safe_system_info() -> SystemInfo {
    SystemInfo {
        version: env!("CARGO_PKG_VERSION").to_string(),
        status: "healthy".to_string(),
        uptime_seconds: get_uptime_seconds(),
        // Only expose safe, non-sensitive system information
        // Never include connection strings, internal URLs, or secrets
    }
}
```

## PII (Personally Identifiable Information) in `admin-api` Context

**🔐 CRITICAL: Understand what constitutes PII in the `admin-api` crate to ensure proper data protection in API responses.**

### What is NOT PII in `admin-api`

The `admin-api` crate primarily exposes blockchain infrastructure metadata via HTTP API, which is generally **NOT considered PII**:

- **📊 System Status**: Server status, health checks, system metrics
- **🗄️ Dataset Information**: Dataset names, schemas, block ranges, file counts
- **⚙️ Provider Configurations**: Provider types, networks, connection endpoints (when they don't contain credentials)
- **📁 Resource Metadata**: Resource IDs, creation timestamps, status indicators
- **🔗 Blockchain References**: Block numbers, transaction hashes, network identifiers

### What COULD be PII in `admin-api`

**⚠️ Be cautious with these data types that might contain PII in API responses:**

- **📝 User-Provided Descriptions**: Dataset descriptions, job names, or custom labels that might contain personal references
- **🏷️ Configuration Fields**: TOML/JSON configuration that might store user information
- **📋 System Identifiers**: Hostnames, IP addresses, or usernames in worker/provider configurations
- **🔗 File Paths**: Paths that might contain usernames or personal directory names
- **📧 External References**: URLs, endpoints, or identifiers that might contain personal information
- **🎯 API Keys/Tokens**: Authentication credentials or API keys (should never be exposed)

### PII Protection Requirements

**If PII is identified in `admin-api` responses:**

- [ ] **🔐 Response Filtering**: Filter out PII fields from API responses
- [ ] **🔒 Access Control**: Implement stricter access controls for endpoints that might expose PII
- [ ] **🗑️ Data Sanitization**: Sanitize API responses to remove sensitive information
- [ ] **🎭 Data Masking**: Mask or redact PII in API responses (e.g., "user****@example.com")
- [ ] **📊 Audit Logging**: Enhanced logging for API operations involving PII
- [ ] **🔍 Response Validation**: Additional validation to prevent accidental PII exposure

### PII Detection Guidelines

**During development, check for PII in:**

1. **API Response Structures** - Review all response types for potential PII fields
2. **Input Processing** - Identify if request data might contain personal information
3. **Logging and Tracing** - Ensure PII is not logged in request/response logs
4. **Error Messages** - Verify error responses don't expose PII
5. **Configuration Data** - Check if exposed configuration contains personal information
6. **File Path Exposure** - Ensure file paths don't reveal personal directory structures

### Non-PII Assumptions

**The following are generally safe assumptions for `admin-api`:**

- System-generated IDs (resource IDs, job IDs) are not PII
- Blockchain-specific data (block numbers, hashes) are not PII
- System metrics and health information are not PII
- Network identifiers and provider types are not PII
- Timestamps and counters are not PII

### API-Specific PII Protection

**Special considerations for HTTP APIs:**

- [ ] **🔐 URL Parameters**: Ensure URL paths and query parameters don't contain PII
- [ ] **🔐 Request Headers**: Avoid logging or exposing personal information in headers
- [ ] **🔐 Response Caching**: Be cautious with caching API responses that might contain PII
- [ ] **🔐 CORS Headers**: Ensure CORS configuration doesn't expose sensitive origins
- [ ] **🔐 Documentation**: API documentation should not include real PII in examples

**💡 Remember: Admin APIs often have elevated access to system data. When in doubt, treat data as potentially containing PII and apply appropriate protections. The cost of over-protection is much lower than the cost of a data breach.**

## Security Review Process

**🔍 Security Review Questions:**

Before merging any `admin-api` changes, ask yourself:

1. **Could this endpoint be vulnerable to injection attacks (path traversal, command injection)?**
2. **Does this endpoint properly validate and sanitize all input parameters?**
3. **Are error messages safe to expose to API clients?**
4. **Could this endpoint leak sensitive information in responses or logs?**
5. **Are HTTP security headers configured correctly?**
6. **Is all sensitive data properly filtered from API responses?**
7. **Does this endpoint assume it operates in a trusted network environment?**
8. **Are all response data structures free of sensitive internal details?**

## Security Incident Response

**🚨 Security Incident Response:**

If a security vulnerability is discovered in the admin API:

1. **Immediate**: Stop any deployment of affected code and consider taking the API offline if necessary
2. **Assessment**: Evaluate the scope and impact of the vulnerability (data exposure, unauthorized access, etc.)
3. **Mitigation**: Implement and deploy fixes as quickly as possible
4. **Communication**: Notify relevant stakeholders about the issue and resolution
5. **Log Analysis**: Review access logs to determine if the vulnerability was exploited
6. **Prevention**: Update security practices and add monitoring to prevent similar issues

## Security Standards Reference

This security framework follows established industry standards:

- **OWASP Application Security Verification Standard (ASVS)** - Web application security requirements
- **OWASP REST Security Guidelines** - REST API specific security best practices
- **HTTP Security Headers** - Browser security and attack prevention
- **Rust Security Guidelines** - Memory safety and secure coding patterns
- **NIST Cybersecurity Framework** - Security risk management

### DoS Protection (Operator Responsibility)

**Infrastructure-Level Protection:**
- [ ] **⏱️ External Rate Limiting**: Service operators MUST implement rate limiting at reverse proxy/load balancer level
- [ ] **⏱️ External Payload Limits**: Operators MUST configure request body size limits at infrastructure level
- [ ] **⏱️ External Timeout Configuration**: Operators MUST configure appropriate request timeouts
- [ ] **⏱️ External Connection Limits**: Operators MUST limit concurrent connections at infrastructure level

**Application-Level Protection:**
- [ ] **⏱️ Request Timeout Handling**: Implement reasonable request processing timeouts within handlers
- [ ] **⏱️ Resource Usage Monitoring**: Monitor and log resource usage for expensive operations

**🛡️ Remember: Security is not optional. These requirements protect administrative systems, maintain service availability, and prevent security vulnerabilities that could compromise the entire Nozzle infrastructure.**
