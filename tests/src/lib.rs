pub mod testlib;

#[cfg(test)]
mod steps;

// These test suite modules cannot be moved to the tests/ integration tests directory because
// they cause linking failures in CI where the `cc` linker gets killed (signal 9, OOM).
#[cfg(test)]
mod tests {
    mod it_dependencies;
    mod it_functions;
    mod it_sql;
    mod it_streaming;
}
