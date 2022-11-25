use std::{fmt::Display, collections::HashMap};
use std::hash::Hash;
use anyhow::Result;
use thiserror::Error;

use pair::Pair;

pub mod pair;

#[derive(Error, Debug)]
pub enum DHeapError {
    #[error("Element already exists in the heap")]
    ElementAlreadyExists,
    #[error("usize max value is not available for priority")]
    UnavailablePriority
}


#[derive(Debug)]
pub struct DHeap<T: Eq + Hash + Clone + Display + PartialEq> {
    data: Vec<Pair<T>>,
    branching_factor: usize,
    map: HashMap<T, bool>
}

impl<T: Eq + Hash + Clone + Display + PartialEq> DHeap<T> {
    /// Creates a new heap
    pub fn new(initial_capacity: Option<usize>, branching_factor: Option<usize>) -> Self {
        match initial_capacity {
            Some(v) => DHeap { data: Vec::with_capacity(v), 
                branching_factor: branching_factor.unwrap_or(4),
                map: HashMap::with_capacity(v)},
            None => DHeap { data: Vec::new(),
                branching_factor: branching_factor.unwrap_or(4),
                map: HashMap::new()},
        }
    }

    /// Accepts a slice of pairs and creates a heap
    pub fn with_pairs(data: &[Pair<T>], initial_capacity: Option<usize>, branching_factor: Option<usize>) -> Result<Self> {
        if data.iter().any(|x| x.priority == std::usize::MAX) { return Err(anyhow::Error::new(DHeapError::UnavailablePriority)); }

        let capacity = if let Some(capacity) = initial_capacity {
            if capacity > data.len() { capacity } else { data.len() * 2 }
            } else { data.len() * 2 };
        
        let mut heap = DHeap { data: Vec::with_capacity(capacity), 
                branching_factor: branching_factor.unwrap_or(4),
                map: HashMap::with_capacity(capacity)};
        heap.map = data.iter().map(|x| (x.get_cloned_element(), true)).collect::<HashMap<T, bool>>();
        heap.data = Vec::from(data);
        heap.heapify();
            
        Ok(heap)
    }

    /// Returns if the element exists in the heap
    pub fn contains(&self, element: &T) -> bool {
        if self.map.contains_key(element) { true }
        else { false }
    }

    /// Removes the element from the heap
    // Essentially we're just updating its priority to the max, then pop
    // Due to that, we shouldn't allow max usize priority while inserting
    pub fn remove(&mut self, element: T) -> Option<Pair<T>> {
        if !self.map.contains_key(&element) { return None; }
        self.map.remove(&element);

        self.update_priority(element, std::usize::MAX);
        self.top()
    }

    /// Inserts the value
    pub fn insert_value(&mut self, element: T, priority: usize) -> Result<(), anyhow::Error> {
        if self.map.contains_key(&element) { return Err(anyhow::Error::new(DHeapError::ElementAlreadyExists)); }
        if priority == std::usize::MAX { return Err(anyhow::Error::new(DHeapError::UnavailablePriority)); }

        self.map.insert(element.clone(), true);
        
        let pair = Pair::new(element, priority);
        self.data.push(pair);
        self.bubble_up(None);

        Ok(())
    }

    /// Inserts a pair
    pub fn insert_pair(&mut self, element: Pair<T>) -> Result<(), anyhow::Error> {
        if self.map.contains_key(&element.get_element()) { return Err(anyhow::Error::new(DHeapError::ElementAlreadyExists)); }
        if element.priority == std::usize::MAX { return Err(anyhow::Error::new(DHeapError::UnavailablePriority)); }

        self.map.insert(element.get_cloned_element(), true);

        self.data.push(element);
        self.bubble_up(None);

        Ok(())
    }

    /// Returns the highest priority value without taking it out of the queue
    /// If empty, returns None
    pub fn peek(&self) -> Option<&Pair<T>> {
        if self.data.len() == 0 {
            None
        } else {
            Some(&self.data[0])
        }
    }
    
    /// Returns the highest priority value. This operation take the value out of the queue
    /// If empty, returns None
    pub fn top(&mut self) -> Option<Pair<T>> {
        let last_element = self.remove_last()?;
        if self.data.is_empty() {
            self.map.remove(last_element.get_element());
            Some(last_element)
        } else {
            let root_element = self.data[0].clone();
            self.data[0] = last_element;
            self.push_down_optimized(None);
            self.map.remove(root_element.get_element());
            Some(root_element)
        }
    }

    /// Finds and update priority of the value
    pub fn update_priority(&mut self, old_value: T, new_priority: usize) {
        if let Some(index) = self.find_index(old_value) {
            let temp = self.data[index].clone();
            self.data.remove(index);
            let updated_pair = Pair::new(temp.get_cloned_element(), new_priority);
            self.insert_pair_for_update(updated_pair);
        }
    }

    fn insert_pair_for_update(&mut self, element: Pair<T>) {
        self.data.push(element);
        self.bubble_up(None);
    }

    fn heapify(&mut self)
    {
        let mut max_index = (self.data.len() - 1) / self.branching_factor;
        while max_index != 0 {
            self.push_down_optimized(Some(max_index));
            max_index -= 1;
        }
        self.push_down_optimized(None);
    }

    fn find_index(&self, old_value: T) -> Option<usize> {
        for (index, pair) in self.data.iter().enumerate() {
            if *pair.get_element() == old_value {
                return Some(index);
            }
        }
        None
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

    #[allow(dead_code)]
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

    #[allow(dead_code)]
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

    fn highest_priority_child_index(&self, index: usize) -> usize {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn testing_dheap() -> DHeap<String> {
        let mut heap = DHeap::new(None, None);
        for i in 1..10 {
            let example_pair = Pair::new(i.to_string(), i);
            _ = heap.insert_pair(example_pair);
        }
        heap
    }

    #[test]
    fn get_top_test() {
        let mut heap = testing_dheap();
        let received = heap.top().unwrap();
        let expected = Pair::new("9", 9);

        assert_eq!(expected.priority, received.priority);
        assert_eq!(*expected.get_element(), *received.get_element());
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
        _ = heap.update_priority("9".to_string(), 10);
        let top_pair = heap.top().unwrap();
        assert_eq!(10, top_pair.priority);
    }

    #[test] 
    fn heapify_top_correct() {
        let pairs = vec![Pair::new("9", 9), Pair::new("4", 4), Pair::new("11", 11),
        Pair::new("10", 10), Pair::new("6", 6), Pair::new("20", 20)];
        let mut heap = DHeap::with_pairs(&pairs, None, Some(4)).unwrap();
        assert_eq!(20, heap.top().unwrap().priority);
    }

    #[test] 
    fn remove_element() {
        let pairs = vec![Pair::new("9", 9), Pair::new("4", 4), Pair::new("11", 11),
        Pair::new("10", 10), Pair::new("6", 6), Pair::new("20", 20)];
        let mut heap = DHeap::with_pairs(&pairs, None, Some(4)).unwrap();
        assert_eq!("11", *heap.remove("11").unwrap().get_element());
    }

    #[test] 
    fn contains_correct() {
        let pairs = vec![Pair::new("9", 9), Pair::new("4", 4), Pair::new("11", 11),
        Pair::new("10", 10), Pair::new("6", 6), Pair::new("20", 20)];
        let heap = DHeap::with_pairs(&pairs, None, Some(4)).unwrap();
        assert!(heap.contains(&"11"));
    }
}