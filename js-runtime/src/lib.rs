use std::sync::Once;

mod convert;
pub mod exception;
pub mod isolate;
pub mod isolate_pool;

#[cfg(test)]
mod tests;

pub type BoxError = Box<dyn std::error::Error + Sync + Send + 'static>;

/// Initialize the per-process V8 platform.
pub(crate) fn init_platform() {
    static V8_INIT: Once = Once::new();

    V8_INIT.call_once(|| {
        v8::V8::set_flags_from_string("--max-opt=2 --random-seed=42 --hash-seed=42");
        let v8_platform = v8::new_default_platform(0, false).make_shared();
        v8::V8::initialize_platform(v8_platform.clone());
        v8::V8::initialize();
        v8::cppgc::initialize_process(v8_platform);
    });
}
