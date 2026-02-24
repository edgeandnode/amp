pub trait BuildState {}

pub enum Set {}
pub enum Unset {}

impl BuildState for Set {}
impl BuildState for Unset {}
