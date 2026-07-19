use crate::error::{Error, Result};
use std::collections::{HashMap, HashSet, VecDeque};

/// Dependency graph for managing service dependencies
#[derive(Debug, Clone)]
pub struct Graph {
    nodes: HashSet<String>,
    /// `edges[A] = [B, C]` means A depends on B and C
    edges: HashMap<String, Vec<String>>,
    /// `reverse[A] = [B, C]` means B and C depend on A
    reverse: HashMap<String, Vec<String>>,
}

impl Graph {
    pub fn new() -> Self {
        Self {
            nodes: HashSet::new(),
            edges: HashMap::new(),
            reverse: HashMap::new(),
        }
    }

    /// Add a node to the graph
    pub fn add_node(&mut self, name: String) {
        self.nodes.insert(name.clone());
        self.edges.entry(name.clone()).or_default();
        self.reverse.entry(name).or_default();
    }

    /// Add a dependency edge (from depends on to)
    pub fn add_edge(&mut self, from: String, to: String) {
        self.add_node(from.clone());
        self.add_node(to.clone());

        self.edges.entry(from.clone()).or_default().push(to.clone());
        self.reverse.entry(to).or_default().push(from);
    }

    /// Get all transitive dependencies of a node in topological order
    pub fn get_dependencies(&self, node: &str) -> Vec<String> {
        let mut visited = HashSet::new();
        let mut result = Vec::new();
        let start_node = node.to_string();

        self.dfs_dependencies(node, &start_node, &mut visited, &mut result);

        result
    }

    fn dfs_dependencies(
        &self,
        node: &str,
        start_node: &str,
        visited: &mut HashSet<String>,
        result: &mut Vec<String>,
    ) {
        if visited.contains(node) {
            return;
        }
        visited.insert(node.to_string());

        // Visit dependencies first (post-order for topological sort)
        if let Some(deps) = self.edges.get(node) {
            for dep in deps {
                self.dfs_dependencies(dep, start_node, visited, result);
            }
        }

        // Don't include the original node in its own dependency list
        if node != start_node {
            result.push(node.to_string());
        }
    }

    /// Get direct dependencies of a node
    pub fn get_direct_dependencies(&self, node: &str) -> Vec<String> {
        self.edges.get(node).cloned().unwrap_or_default()
    }

    /// Get nodes that depend on the given node
    pub fn get_dependents(&self, node: &str) -> Vec<String> {
        self.reverse.get(node).cloned().unwrap_or_default()
    }

    /// Topological sort - returns nodes in dependency order (dependencies first)
    pub fn topological_sort(&self) -> Result<Vec<String>> {
        let mut in_degree = HashMap::new();

        // Calculate in-degrees
        for node in &self.nodes {
            in_degree.insert(
                node.clone(),
                self.edges.get(node).map_or(0, |deps| deps.len()),
            );
        }

        // Find all nodes with no dependencies
        let mut queue: VecDeque<String> = in_degree
            .iter()
            .filter(|&(_, &degree)| degree == 0)
            .map(|(node, _)| node.clone())
            .collect();

        let mut result = Vec::new();

        while let Some(node) = queue.pop_front() {
            result.push(node.clone());

            // For each node that depends on this one
            if let Some(dependents) = self.reverse.get(&node) {
                for dependent in dependents {
                    if let Some(degree) = in_degree.get_mut(dependent) {
                        *degree -= 1;
                        if *degree == 0 {
                            queue.push_back(dependent.clone());
                        }
                    }
                }
            }
        }

        // Check if all nodes were processed (no cycles)
        if result.len() != self.nodes.len() {
            // Find the cycle for a better error message
            let cycle = self.find_cycle();
            return Err(Error::CircularDependency(cycle));
        }

        Ok(result)
    }

    /// Find a cycle in the graph and return it as a path
    fn find_cycle(&self) -> Vec<String> {
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();
        let mut path = Vec::new();

        for node in &self.nodes {
            if !visited.contains(node) {
                if let Some(cycle) =
                    self.find_cycle_dfs(node, &mut visited, &mut rec_stack, &mut path)
                {
                    return cycle;
                }
            }
        }

        // Fallback if we can't find the exact cycle
        self.nodes.iter().take(3).cloned().collect()
    }

    fn find_cycle_dfs(
        &self,
        node: &str,
        visited: &mut HashSet<String>,
        rec_stack: &mut HashSet<String>,
        path: &mut Vec<String>,
    ) -> Option<Vec<String>> {
        visited.insert(node.to_string());
        rec_stack.insert(node.to_string());
        path.push(node.to_string());

        if let Some(deps) = self.edges.get(node) {
            for dep in deps {
                if !visited.contains(dep) {
                    if let Some(cycle) = self.find_cycle_dfs(dep, visited, rec_stack, path) {
                        return Some(cycle);
                    }
                } else if rec_stack.contains(dep) {
                    // Found cycle - extract it from path
                    let cycle_start = path.iter().position(|n| n == dep).unwrap_or(0);
                    let mut cycle: Vec<String> = path[cycle_start..].to_vec();
                    cycle.push(dep.clone()); // Complete the cycle
                    return Some(cycle);
                }
            }
        }

        rec_stack.remove(node);
        path.pop();
        None
    }

    /// Get groups of nodes that can be started in parallel
    pub fn get_parallel_groups(&self) -> Result<Vec<Vec<String>>> {
        // First check for cycles
        self.topological_sort()?;

        let mut in_degree = HashMap::new();
        for node in &self.nodes {
            in_degree.insert(
                node.clone(),
                self.edges.get(node).map_or(0, |deps| deps.len()),
            );
        }

        let mut groups = Vec::new();
        let mut processed = HashSet::new();

        while processed.len() < self.nodes.len() {
            // Find all nodes that can be processed now
            let current_group: Vec<String> = self
                .nodes
                .iter()
                .filter(|node| {
                    !processed.contains(*node) && in_degree.get(*node).copied().unwrap_or(0) == 0
                })
                .cloned()
                .collect();

            if current_group.is_empty() {
                return Err(Error::Config(
                    "Unable to determine parallel groups".to_string(),
                ));
            }

            groups.push(current_group.clone());

            // Mark these nodes as processed and reduce in-degree of dependents
            for node in &current_group {
                processed.insert(node.clone());
                if let Some(dependents) = self.reverse.get(node) {
                    for dependent in dependents {
                        if let Some(degree) = in_degree.get_mut(dependent) {
                            *degree = degree.saturating_sub(1);
                        }
                    }
                }
            }
        }

        Ok(groups)
    }

    /// Check if the graph has any cycles
    pub fn has_cycle(&self) -> bool {
        self.topological_sort().is_err()
    }

    /// Get all nodes in the graph
    pub fn nodes(&self) -> &HashSet<String> {
        &self.nodes
    }
}

impl Default for Graph {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_dependency() {
        let mut graph = Graph::new();
        graph.add_edge("a".to_string(), "b".to_string());
        graph.add_edge("b".to_string(), "c".to_string());

        let deps = graph.get_dependencies("a");
        assert!(deps.contains(&"b".to_string()));
        assert!(deps.contains(&"c".to_string()));
    }

    #[test]
    fn test_topological_sort() {
        let mut graph = Graph::new();
        graph.add_edge("a".to_string(), "b".to_string());
        graph.add_edge("b".to_string(), "c".to_string());

        let sorted = graph.topological_sort().unwrap();

        // c should come before b, b before a
        let c_idx = sorted.iter().position(|s| s == "c").unwrap();
        let b_idx = sorted.iter().position(|s| s == "b").unwrap();
        let a_idx = sorted.iter().position(|s| s == "a").unwrap();

        assert!(c_idx < b_idx);
        assert!(b_idx < a_idx);
    }

    #[test]
    fn test_circular_dependency() {
        let mut graph = Graph::new();
        graph.add_edge("a".to_string(), "b".to_string());
        graph.add_edge("b".to_string(), "a".to_string());

        assert!(graph.has_cycle());
    }

    #[test]
    fn test_parallel_groups() {
        let mut graph = Graph::new();
        graph.add_node("a".to_string());
        graph.add_node("b".to_string());
        graph.add_edge("c".to_string(), "a".to_string());
        graph.add_edge("c".to_string(), "b".to_string());

        let groups = graph.get_parallel_groups().unwrap();

        // First group should contain a and b (no dependencies)
        assert_eq!(groups[0].len(), 2);
        assert!(groups[0].contains(&"a".to_string()));
        assert!(groups[0].contains(&"b".to_string()));

        // Second group should contain c
        assert_eq!(groups[1].len(), 1);
        assert!(groups[1].contains(&"c".to_string()));
    }
}
