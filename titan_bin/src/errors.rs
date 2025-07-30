use bedrock::lock_manager::LockError;

#[derive(Debug)]
pub enum ExecutionError {
    IoError(()),
    TableNotFound(String),
    ColumnNotFound(String),
    GenericError(String),
    Deadlock,
    SerializationFailure,
    PlanningError(String),
    ParsingError(String),
}

impl From<std::io::Error> for ExecutionError {
    fn from(_err: std::io::Error) -> Self {
        ExecutionError::IoError(())
    }
}

impl From<LockError> for ExecutionError {
    fn from(err: LockError) -> Self {
        match err {
            LockError::Deadlock => ExecutionError::Deadlock,
        }
    }
}
