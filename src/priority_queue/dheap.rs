use std::fmt::Display;

use crate::priority_queue::pair::Pair;

#[derive(Debug)]
pub struct DHeap<T: Clone + Display + PartialEq> {
    data: Vec<Pair<T>>,
    branching_factor: usize
}

impl<T: Clone + Display + PartialEq> DHeap<T> {
    pub fn new(initial_capacity: Option<usize>, branching_factor: Option<usize>) -> Self {
        match initial_capacity {
            Some(v) => DHeap { data: Vec::with_capacity(v), 
                branching_factor: branching_factor.unwrap_or(2)},
            None => DHeap { data: Vec::new(),
                branching_factor: branching_factor.unwrap_or(2)},
        }
    }

    pub fn heapify(&mut self)
    {
        let mut max_index = (self.data.len() - 1) / self.branching_factor;
        while max_index != 0 {
            self.push_down_optimized(Some(max_index));
            max_index -= 1;
        }
    }

    pub fn insert(&mut self, element: Pair<T>) {
        self.data.push(element);
        self.bubble_up(None);
    }

    pub fn peek(&self) -> Option<&Pair<T>> {
        if self.data.len() == 0 {
            None
        } else {
            Some(&self.data[0])
        }
    }

    pub fn update_priority(&mut self, old_value: T, new_priority: usize) {
        if let Some(index) = self.find_index(old_value) {
            let temp = self.data[index].clone();
            self.data.remove(index);
            let updated_pair = Pair {
                element: temp.element,
                priority: new_priority
            };
            self.insert(updated_pair);
        }
    }

    fn find_index(&self, old_value: T) -> Option<usize> {
        for (index, pair) in self.data.iter().enumerate() {
            if pair.element == old_value {
                return Some(index);
            }
        }
        None
    }

    pub fn top(&mut self) -> Option<Pair<T>> {
        let last_element = self.remove_last()?;
        if self.data.is_empty() {
            Some(last_element)
        } else {
            let root_element = self.data[0].clone();
            self.data[0] = last_element;
            self.push_down_optimized(None);
            Some(root_element)
        }
    }

    fn remove_last(&mut self) -> Option<Pair<T>> {
        if self.data.is_empty() {
            None
        } else {
            self.data.pop()
        }
    }

    // bubbles up the selected element
    fn bubble_up(&mut self, index: Option<usize>) {
        // as default the last element is selected
        let mut parent_index = index.unwrap_or(self.data.len() - 1);
        while parent_index > 0 {
            let current_index = parent_index;
            parent_index = self.get_parent_index(parent_index);
            if self.data[parent_index].priority < self.data[current_index].priority {
                self.swap(current_index, parent_index)
            } else {
                break;
            }
        }
    }

    fn bubble_up_optimized(&mut self, initial_index: Option<usize>) {
        let mut index = initial_index.unwrap_or(self.data.len() - 1);
        let current = self.data[index].clone();
        while index > 0 {
            let parent_index = self.get_parent_index(index);
            if self.data[parent_index].priority < self.data[index].priority {
                self.data[index] = self.data[parent_index].clone();
                index = parent_index;
            } else {
                break;
            }
        }
        self.data[index] = current;
    }

    fn push_down(&mut self, initial_index: Option<usize>) {
        let index = initial_index.unwrap_or(0);
        let mut current_index = index;
        while current_index < self.first_leaf_index() {
            let highest_priority_child_index = self.highest_priority_child_index(index);
            if self.data[current_index].priority < self.data[highest_priority_child_index].priority {
                self.swap(current_index,highest_priority_child_index);
                current_index = highest_priority_child_index;
            } else {
                break;
            }
        }    
    }

    fn push_down_optimized(&mut self, initial_index: Option<usize>) {
        let mut index = initial_index.unwrap_or(0);
        let current = self.data[index].clone();
        while index < self.first_leaf_index() {
            let highest_priority_child_index = self.highest_priority_child_index(index);
            if self.data[index].priority < self.data[highest_priority_child_index].priority {
                self.data[index] = self.data[highest_priority_child_index].clone();
                index = highest_priority_child_index;
            } else {
                break;
            }
        } 
        self.data[index] = current;
    }

    fn first_leaf_index(&self) -> usize {
        (self.data.len() - 2) / self.branching_factor + 1
    }

    fn get_parent_index(&self, index: usize) -> usize {
        (index - 1) / self.branching_factor
    }

    fn swap(&mut self, first_index: usize, second_index: usize) {
        self.data.swap(first_index, second_index);
    }

    pub fn highest_priority_child_index(&self, index: usize) -> usize {
        // if it has no child, returns itself
        let first_child_index = (self.branching_factor * index) + 1;
        if self.data.len() - 1 < first_child_index {
            return index;
        }

        let mut highest_priority_index = index;
        for i in 1..=self.branching_factor {
            let child_index = (self.branching_factor * index) + i;
            if self.data.len() - 1 < child_index {
                continue;
            }

            if self.data[child_index].priority > self.data[highest_priority_index].priority {
                highest_priority_index = child_index;
            }
        }
        highest_priority_index
    }
}

#[derive(Debug)]
enum DheapError {
    EmptyHeap
}

impl std::error::Error for DheapError {}
impl Display for DheapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyHeap => write!(f, "empty heap")
        }
    }
}

mod tests {
    use super::*;

    fn testing_dheap() -> DHeap<String> {
        let mut heap = DHeap::new(None, None);
        for i in 1..10 {
            let example_pair = Pair {priority: i, element: i.to_string()};
            heap.insert(example_pair);
        }
        heap
    }

    #[test]
    fn get_top_test() {
        let mut heap = testing_dheap();
        let received = heap.top().unwrap();
        let expected = Pair {priority: 9, element: 9.to_string()};

        assert_eq!(expected.priority, received.priority);
        assert_eq!(expected.element, received.element);
    }

    #[test]
    fn peek_test() {
        let heap = testing_dheap();
        let pair = heap.peek().unwrap();
        assert_eq!(9, pair.priority);
    }

    #[test]
    fn update_is_correct() {
        let mut heap = testing_dheap();
        heap.update_priority("9".to_string(), 10);
        let top_pair = heap.top().unwrap();
        assert_eq!(10, top_pair.priority);
    }
}