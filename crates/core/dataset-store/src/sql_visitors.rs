use std::ops::ControlFlow;

use common::BoxError;
use datafusion::sql::{
    parser::Statement,
    sqlparser::ast::{Expr, Function, ObjectNamePart, Visit, Visitor},
};
use itertools::Itertools;

/// Returns a list of all function names in the SQL statement.
///
/// Errors in case of some DML statements.
pub fn all_function_names(stmt: &Statement) -> Result<Vec<String>, BoxError> {
    let mut collector = FunctionCollector {
        functions: Vec::new(),
    };
    let stmt = match stmt {
        Statement::Statement(statement) => statement,
        Statement::CreateExternalTable(_) | Statement::CopyTo(_) => {
            return Err("DML not supported".into());
        }
        Statement::Explain(explain) => match explain.statement.as_ref() {
            Statement::Statement(statement) => statement,
            _ => return Err("unsupported statement in EXPLAIN".into()),
        },
    };

    let c = stmt.visit(&mut collector);
    assert!(c.is_continue());

    Ok(collector
        .functions
        .into_iter()
        .map(|f| {
            f.name
                .0
                .into_iter()
                .map(|s| match s {
                    ObjectNamePart::Identifier(ident) => ident.value,
                })
                .join(".")
        })
        .collect())
}

struct FunctionCollector {
    functions: Vec<Function>,
}

impl Visitor for FunctionCollector {
    type Break = ();

    fn pre_visit_expr(&mut self, function: &Expr) -> ControlFlow<()> {
        if let Expr::Function(f) = function {
            self.functions.push(f.clone());
        }
        ControlFlow::Continue(())
    }
}
