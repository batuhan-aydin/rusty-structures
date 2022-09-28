use std::fmt::Display;

#[derive(Clone, Copy, Debug)]
pub struct Pair<T> where T : Clone + Sized + Display + PartialEq {
    pub priority: usize,
    pub element: T
}

impl<T: Clone + Display + PartialEq> Display for Pair<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "priority: {}, element: {}", self.priority, self.element)
    }
}