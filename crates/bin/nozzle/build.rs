use vergen_gitcl::{BuildBuilder, Emitter, GitclBuilder};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

fn main() -> Result<(), BoxError> {
    let build = BuildBuilder::all_build()?;
    let gitcl = GitclBuilder::default().describe(true, true, None).build()?;
    Emitter::new()
        .add_instructions(&build)?
        .add_instructions(&gitcl)?
        .emit()?;
    Ok(())
}
