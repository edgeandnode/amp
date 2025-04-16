use thiserror::Error;

// If this function is called but no exception has been caught by `s`, it will panic.
pub(crate) fn catch<'s, 't, S>(s: &mut v8::TryCatch<'s, S>) -> ExceptionMessage
where
    v8::TryCatch<'s, S>: AsMut<v8::HandleScope<'t, ()>>,
    v8::TryCatch<'s, S>: AsMut<v8::HandleScope<'t>>,
    't: 's,
{
    assert!(s.has_caught());

    // Unwrap: an exception has been caught
    let exception = s.exception().unwrap().to_rust_string_lossy(s.as_mut());
    let position = s.message().map(|m| exception_position(s.as_mut(), m));

    let stack = s.stack_trace().and_then(|val| {
        val.to_string(s.as_mut())
            .map(|val| val.to_rust_string_lossy(s.as_mut()))
    });

    ExceptionMessage {
        exception,
        position,
        stack,
    }
}

#[derive(Debug, Error)]
pub struct ExceptionMessage {
    // Stringified exception object
    pub exception: String,
    // Stringified stack trace
    pub stack: Option<String>,
    // Position of the exception in the source code
    pub position: Option<ExceptionPosition>,
}

impl fmt::Display for ExceptionMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Uncaught: {}", self.exception)?;

        if let Some(pos) = &self.position {
            writeln!(f, "{}", pos)?;
        }

        if let Some(stack) = &self.stack {
            writeln!(f, "\nStack trace:\n{}", stack)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct ExceptionPosition {
    pub filename: Option<String>,
    pub line_number: Option<usize>,
    pub start_column: usize,
    pub end_column: usize,
    pub source_line: Option<String>,
}

pub fn exception_position<'s>(
    scope: &mut v8::HandleScope<'s>,
    message: v8::Local<'s, v8::Message>,
) -> ExceptionPosition {
    let filename = message
        .get_script_resource_name(scope)
        .and_then(|val| val.to_string(scope).map(|s| s.to_rust_string_lossy(scope)));

    let line_number = message.get_line_number(scope);
    let start_column = message.get_start_column();
    let end_column = message.get_end_column();

    let source_line = message
        .get_source_line(scope)
        .map(|s| s.to_rust_string_lossy(scope));

    ExceptionPosition {
        filename,
        line_number,
        start_column,
        end_column,
        source_line,
    }
}

use std::fmt;

impl fmt::Display for ExceptionPosition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let filename = self.filename.as_deref().unwrap_or("<unknown>");
        let line = self.line_number.unwrap_or(0);
        let source = self.source_line.as_deref().unwrap_or("");

        writeln!(
            f,
            " --> {}:{}:{}-{}",
            filename, line, self.start_column, self.end_column
        )?;
        if !source.is_empty() {
            // Highlight the error region with a caret line
            let underline: String = {
                let mut buf = String::new();
                for i in 0..self.end_column {
                    if i >= self.start_column {
                        buf.push('^');
                    } else {
                        buf.push(' ');
                    }
                }
                buf
            };
            writeln!(f, "{}", source)?;
            writeln!(f, "{}", underline)?;
        }

        Ok(())
    }
}
