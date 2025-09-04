# Nozzle Core Development Patterns

## 🎯 PURPOSE
This directory provides reusable solutions and best practices for Nozzle Core development, ensuring consistency and quality across the Nozzle project Rust codebase.

## 📚 AVAILABLE PATTERNS

### Testing Patterns
- **[testing-patterns.md](./testing-patterns.md)** - Testing strategies and best practices for writing tests.

## 🔧 HOW TO USE

### For Developers
1. **Reference before implementing** - Check relevant patterns before starting new work
2. **Follow established conventions** - Use patterns as templates for consistency
3. **Contribute improvements** - Update patterns based on lessons learned

### For Code Reviews
1. **Validate pattern compliance** - Ensure implementations follow established patterns
2. **Identify pattern violations** - Look for anti-patterns and forbidden practices
3. **Suggest pattern usage** - Recommend appropriate patterns for specific use cases

### For Documentation
1. **Extract reusable patterns** - Identify common solutions for documentation
2. **Update patterns** - Keep patterns current with library evolution
3. **Cross-reference examples** - Link patterns to real codebase examples

## 🚨 CRITICAL PRINCIPLES

### Forbidden Patterns (NEVER USE)
- **Unsafe code without documentation**: Any `unsafe` block without thorough safety comments
- **Unwrapping without context**: Using `.unwrap()` or `.expect()` without clear justification
- **Blocking in async contexts**: Using synchronous I/O in async functions

### Mandatory Patterns (ALWAYS USE)
- **Immediate formatting**: `just fmt` after editing Rust files
- **Compilation validation**: `just check` and `just test-local` must pass

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

This patterns directory serves as the authoritative guide for Rust development in the Nozzle project, ensuring consistent, high-quality implementations across the entire codebase.
