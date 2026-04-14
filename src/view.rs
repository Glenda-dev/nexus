use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;
use glenda::cap::Endpoint;

#[derive(Debug, Clone)]
pub struct View {
    pub root: String,
    pub mounts: BTreeMap<String, Vec<Endpoint>>,
}

impl View {
    pub fn new(root: &str) -> Self {
        Self { root: Self::normalize_absolute_path(root), mounts: BTreeMap::new() }
    }

    pub fn clone_with_root(&self, root: &str) -> Self {
        Self { root: Self::normalize_absolute_path(root), mounts: self.mounts.clone() }
    }

    pub fn push_mount(&mut self, path: &str, target: Endpoint) {
        self.mounts.entry(Self::normalize_absolute_path(path)).or_default().push(target);
    }

    pub fn pop_mount(&mut self, path: &str) -> Option<Endpoint> {
        let normalized = Self::normalize_absolute_path(path);
        let stack = self.mounts.get_mut(&normalized)?;
        let popped = stack.pop();
        if stack.is_empty() {
            self.mounts.remove(&normalized);
        }
        popped
    }

    pub fn normalize_absolute_path(path: &str) -> String {
        let src = if path.is_empty() { "/" } else { path };
        let mut stack: Vec<&str> = Vec::new();
        for part in src.split('/') {
            if part.is_empty() || part == "." {
                continue;
            }
            if part == ".." {
                let _ = stack.pop();
                continue;
            }
            stack.push(part);
        }

        if stack.is_empty() {
            return String::from("/");
        }

        let mut out = String::new();
        for part in stack {
            out.push('/');
            out.push_str(part);
        }
        out
    }

    fn path_matches_mount(path: &str, mount_path: &str) -> bool {
        if mount_path == "/" {
            return path.starts_with('/');
        }

        path == mount_path
            || (path.starts_with(mount_path)
                && path.as_bytes().get(mount_path.len()).map(|b| *b == b'/').unwrap_or(false))
    }

    fn path_is_under(path: &str, parent: &str) -> bool {
        path == parent
            || (path.starts_with(parent)
                && path.as_bytes().get(parent.len()).map(|b| *b == b'/').unwrap_or(false))
    }

    pub fn find_mount_stack_with_root(&self, path: &str) -> Option<(&str, &[Endpoint], String)> {
        let mut best_match: Option<(&str, &[Endpoint])> = None;
        for (m_path, stack) in &self.mounts {
            if !Self::path_matches_mount(path, m_path) {
                continue;
            }
            let stack_slice = stack.as_slice();
            if stack_slice.is_empty() {
                continue;
            }
            if best_match.is_none() || m_path.len() > best_match.unwrap().0.len() {
                best_match = Some((m_path.as_str(), stack_slice));
            }
        }

        best_match.map(|(m_path, stack)| {
            let mut sub_path = &path[m_path.len()..];
            if sub_path.is_empty() {
                sub_path = "/";
            }
            (m_path, stack, String::from(sub_path))
        })
    }

    pub fn find_mount_with_root(&self, path: &str) -> Option<(&str, Endpoint, String)> {
        self.find_mount_stack_with_root(path).and_then(|(m_path, stack, sub_path)| {
            stack.last().copied().map(|target| (m_path, target, sub_path))
        })
    }

    pub fn map_path_into_view_root(&self, path: &str) -> String {
        let normalized = Self::normalize_absolute_path(path);
        if self.root == "/" {
            return normalized;
        }

        if normalized == "/" {
            return self.root.clone();
        }

        if Self::path_is_under(&normalized, &self.root) {
            return normalized;
        }

        let mut out = String::with_capacity(self.root.len() + normalized.len());
        out.push_str(&self.root);
        out.push_str(&normalized);
        Self::normalize_absolute_path(&out)
    }
}
