use std::fmt::Display;

#[derive(Clone, Copy, Debug)]
pub struct Pair<T> where T : Clone + Sized + Display + PartialEq {
    pub priority: usize,
    element: T
}

impl<T> Pair<T> where T : Clone + Sized + Display + PartialEq {
    pub fn new(element: T, priority: usize) -> Self {
        Self { priority, element }
    }

    pub(super) fn get_element(&self) -> &T {
        &self.element
    }

    pub(super) fn get_cloned_element(&self) -> T {
        self.element.clone()
    }
}

impl<T: Clone + Display + PartialEq> Display for Pair<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "priority: {}, element: {}", self.priority, self.element)
    }
}