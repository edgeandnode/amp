use datafusion::common::HashMap;
use v8::{
    Handle as _,
    script_compiler::{CompileOptions, NoCacheReason},
};

use crate::{
    BoxError,
    convert::{FromV8, ToV8},
    exception::{ExceptionMessage, catch},
    init_platform,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("exception in script: {0}")]
    Exception(#[from] ExceptionMessage),

    #[error("string too long: {0}")]
    StringTooLong(usize),

    #[error("function {0} not found")]
    FunctionNotFound(String),

    #[error("runtime error: {0}")]
    Other(String),

    #[error("error converting return value: {0}")]
    ConvertReturnValue(BoxError),

    #[error("error converting param at idx {0}: {1}")]
    ConvertParam(usize, BoxError),

    #[error("JS invocation panicked: {0}")]
    Panic(String),

    #[error("isolate pool error: {0}")]
    PoolError(#[from] deadpool::unmanaged::PoolError),
}

#[derive(Debug)]
pub struct Isolate {
    isolate: v8::OwnedIsolate,

    // TODO: Use cache capable of evicting
    scripts: HashMap<blake3::Hash, v8::Global<v8::UnboundScript>>,
}

impl Isolate {
    pub fn new() -> Self {
        // Ensure the platform is initialized.
        init_platform();

        let isolate = v8::Isolate::new(v8::CreateParams::default());
        Self {
            isolate,
            scripts: HashMap::new(),
        }
    }

    /// Compile a script. This is cached.
    ///
    /// `filename` is only used for display in stack traces.
    fn compile_script(
        &mut self,
        filename: &str,
        script: &str,
    ) -> Result<v8::Global<v8::UnboundScript>, Error> {
        let key = blake3::hash(script.as_bytes());

        if let Some(script) = self.scripts.get(&key) {
            return Ok(script.clone());
        }

        let s = &mut v8::HandleScope::new(&mut self.isolate);
        let origin = {
            let filename = v8_string(s, filename)?;
            script_origin(s, filename)
        };
        let mut source = v8::script_compiler::Source::new(v8_string(s, script)?, Some(&origin));
        let context = v8::Context::new(s, v8::ContextOptions::default());
        let compiled_script = {
            let s = &mut v8::ContextScope::new(s, context);
            let s = &mut v8::TryCatch::new(s);
            match v8::script_compiler::compile_unbound_script(
                s,
                &mut source,
                CompileOptions::EagerCompile,
                NoCacheReason::NoReason,
            ) {
                Some(compiled_script) => v8::Global::new(s, compiled_script),
                None => return Err(catch(s).into()),
            }
        };
        self.scripts.insert(key, compiled_script.clone());
        Ok(compiled_script)
    }

    /// Invoke a function in a fresh context (aka JS realm).
    ///
    /// `filename` is only used for display in stack traces.
    pub fn invoke<'a, R: FromV8>(
        &mut self,
        filename: &str,
        script: &str,
        function: &str,
        params: impl Iterator<Item = &'a dyn ToV8>,
    ) -> Result<R, Error> {
        // Reduce to a batch invocation with a single batch.
        let params_batches = vec![params];
        Ok(self
            .invoke_batch::<R>(filename, script, function, params_batches)?
            .into_iter()
            .next()
            .unwrap())
    }

    // Call the same function multiple times with different params. This is faster as it will amortize
    // the cost of initializing the context and will benefit from warm V8 JIT caches.
    //
    // The outputs may not be the same as doing the same call using `invoke`, as global variables will
    // be retained across invocations.
    pub fn invoke_batch<'a, R: FromV8>(
        &mut self,
        filename: &str,
        script: &str,
        function: &str,
        params_batches: Vec<impl Iterator<Item = &'a dyn ToV8>>,
    ) -> Result<Vec<R>, Error> {
        let script = self.compile_script(filename, script)?;

        // Enter a fresh context.
        let s = &mut v8::HandleScope::new(&mut self.isolate);
        let context = v8::Context::new(s, v8::ContextOptions::default());
        let s = &mut v8::ContextScope::new(s, context);

        // Load script.
        let script = v8::Local::new(s, &script).bind_to_current_context(s);
        let s = &mut v8::TryCatch::new(s);
        match script.run(s) {
            Some(_) => {} // Ignore script return value
            None => return Err(catch(s).into()),
        }

        // Call function
        let func = get_function(s, function)?;
        params_batches
            .into_iter()
            .map(|params| call_function(s, func, params))
            .collect()
    }
}

fn call_function<'a, 't, 's, R: FromV8>(
    s: &mut v8::TryCatch<'t, v8::HandleScope<'s>>,
    func: v8::Local<'s, v8::Function>,
    params: impl Iterator<Item = &'a dyn ToV8>,
) -> Result<R, Error> {
    let receiver = v8::undefined(s);

    let params = params
        .enumerate()
        .map(|(i, p)| p.to_v8(s).map_err(|e| Error::ConvertParam(i, e)))
        .collect::<Result<Vec<_>, _>>()?;

    let ret_val = match func.open(s).call(s, receiver.into(), &params) {
        Some(ret_val) => ret_val,
        None => return Err(catch(s).into()),
    };

    R::from_v8(s, ret_val).map_err(Error::ConvertReturnValue)
}

/// Global context must define a `Module` that contains a `run` function
pub fn get_function<'s>(
    s: &mut v8::HandleScope<'s>,
    func_name: &str,
) -> Result<v8::Local<'s, v8::Function>, Error> {
    let context = s.get_current_context();

    let global = context
        .global(s)
        .to_object(s)
        .ok_or_else(|| Error::Other("Failed to get global object".to_string()))?;

    let v8_func_name = v8_string(s, func_name)?;
    let func = global
        .get(s, v8_func_name.into())
        .ok_or_else(|| Error::FunctionNotFound(func_name.to_string()))?;

    if !func.is_function() {
        return Err(Error::FunctionNotFound(func_name.to_string()));
    }

    // Unwrap: we checked that it is a function
    Ok(v8::Local::<v8::Function>::try_from(func).unwrap())
}

// Errors if the string is too long
fn v8_string<'s>(
    scope: &mut v8::HandleScope<'s, ()>,
    s: &str,
) -> Result<v8::Local<'s, v8::String>, Error> {
    v8::String::new_from_utf8(scope, s.as_bytes(), v8::NewStringType::Normal)
        .ok_or(Error::StringTooLong(s.len()))
}

fn script_origin<'s>(
    scope: &mut v8::HandleScope<'s, ()>,
    filename: v8::Local<'s, v8::String>,
) -> v8::ScriptOrigin<'s> {
    v8::ScriptOrigin::new(
        scope,
        filename.into(),
        0,
        0,
        false,
        -1,
        None,
        false,
        false,
        false,
        None,
    )
}
