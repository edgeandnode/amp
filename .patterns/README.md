Development Patterns
====================

## 🎯 PURPOSE

This directory provides reusable solutions and best practices for Nozzle Core development, ensuring consistency and quality across the Nozzle project Rust codebase.

## 🔧 HOW TO USE

### For Developers
1. **Reference before implementing** - Check relevant patterns before starting new work
2. **Check crate-specific guidelines** - **MANDATORY**: Review crate-specific code patterns and security guidelines for the target crate
3. **Follow established conventions** - Use patterns as templates for consistency
4. **Contribute improvements** - Update patterns based on lessons learned

### For Code Reviews
1. **Validate pattern compliance** - Ensure implementations follow established patterns
2. **Verify crate-specific adherence** - **CRITICAL**: Confirm crate-specific code patterns and security guidelines were followed
3. **Identify pattern violations** - Look for anti-patterns and forbidden practices
4. **Suggest pattern usage** - Recommend appropriate patterns for specific use cases

### For Documentation
1. **Extract reusable patterns** - Identify common solutions for documentation
2. **Update crate-specific patterns** - Keep crate-specific guidelines current with implementation changes
3. **Update patterns** - Keep patterns current with library evolution
4. **Cross-reference examples** - Link patterns to real codebase examples

## 🚨 CRITICAL PRINCIPLES

### Mandatory Patterns (ALWAYS USE)
- **Immediate formatting**: `just fmt-file <rust_file>.rs` after editing ANY Rust file
- **Compilation validation**: `just check-crate <crate-name>` and `just check-all` MUST pass

### Forbidden Patterns (NEVER USE)
- **Unsafe code without documentation**: Any `unsafe` block without thorough safety comments
- **Unwrapping without context**: Using `.unwrap()` or `.expect()` without clear justification
- **Blocking in async contexts**: Using synchronous I/O in async functions

## 📈 PATTERN QUALITY METRICS

### Completeness
- [ ] Core concepts clearly explained
- [ ] Executable code examples provided
- [ ] Common use cases covered
- [ ] Integration patterns documented

### Accuracy
- [ ] All examples compile and run correctly
- [ ] Patterns follow current Rust and crate conventions
- [ ] No deprecated or anti-pattern usage
- [ ] Proper error handling demonstrated

### Clarity
- [ ] Clear explanations for "why" not just "how"
- [ ] Progressive complexity (simple to advanced examples)
- [ ] Common pitfalls identified and explained
- [ ] Best practices highlighted

## 🔄 MAINTENANCE

### Regular Updates
- **API Changes**: Update patterns when APIs evolve
- **New Patterns**: Add patterns for newly identified common use cases
- **Deprecation**: Mark outdated patterns and provide migration paths
- **Examples**: Keep examples current with latest crate versions

### Quality Assurance
- **Pattern Validation**: Regularly test all code examples
- **Consistency**: Ensure patterns align with current codebase standards
- **Documentation**: Keep pattern documentation synchronized with implementation

## 🎯 SUCCESS INDICATORS

### Developer Experience
- Reduced time to implement common patterns
- Consistent code quality across the codebase
- Fewer pattern-related code review comments
- Improved onboarding for new contributors

### Code Quality
- Consistent architecture patterns throughout codebase
- Proper error handling and resource management
- Type-safe implementations without workarounds
- Comprehensive test coverage with proper patterns

### Documentation Quality
- Practical, working examples for all patterns
- Clear guidance on when and how to use each pattern
- Integration examples showing pattern composition
- Anti-pattern identification and alternatives

## 📚 AVAILABLE PATTERNS

### ⚛️ Core Development Patterns
- **[cargo-workspace-patterns.md](./cargo-workspace-patterns.md)** - **🚨 MANDATORY for ALL workspace operations** - Comprehensive workspace management, crate organization rules, dependency management with `cargo add`/`cargo remove`, and workspace structure guidelines. MUST be consulted before creating crates or managing dependencies.
- **[testing-patterns.md](./testing-patterns.md)** - **🚨 MANDATORY for ALL testing** - Testing strategies, GIVEN-WHEN-THEN structure, naming conventions, and three-tier testing strategy. MUST be consulted before writing ANY tests.

### 🏗️ Crate-Specific Development Guidelines

**🚨 CRITICAL: The following patterns are MANDATORY when working on specific crates. AI agents and developers MUST review these before making any changes to the respective crates.**

#### 🔧 `admin-api` Crate
- **[admin-api/crate.md](./admin-api/crate.md)** - **REQUIRED reading** for all `admin-api` development. Contains mandatory patterns for HTTP handlers using Axum framework, error handling, response types, and documentation standards.
- **[admin-api/security.md](./admin-api/security.md)** - **🚨 MANDATORY SECURITY REVIEW** for ALL `admin-api` changes. Comprehensive security checklist for HTTP API security, input validation, injection prevention, and network isolation patterns.

#### 🗄️ `metadata-db` Crate  
- **[metadata-db/crate.md](./metadata-db/crate.md)** - **REQUIRED reading** for all `metadata-db` development. Contains mandatory database design patterns, testing strategies, and API conventions.
- **[metadata-db/security.md](./metadata-db/security.md)** - **🚨 MANDATORY SECURITY REVIEW** for ALL `metadata-db` changes. Comprehensive security checklist for database security, access control, OWASP compliance, and secure coding patterns.

**⚠️ AI Agent Reminder: These crate-specific patterns override general patterns and MUST be consulted before implementing any functionality in the respective crates.**

------

This `/.patterns` directory serves as the authoritative guide for Rust development in the Nozzle project, ensuring consistent, high-quality implementations across the entire codebase.
