/// Reusable state for a searchable, keyboard-driven dropdown.
///
/// The component owns its items so callers can use it for temporary overlays
/// without coupling the dropdown to an application's backing catalog. Callers
/// provide a search-text projection because display data is domain-specific.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SearchableDropdown<T> {
    pub items: Vec<T>,
    pub query: String,
    pub selected: usize,
}

impl<T> SearchableDropdown<T> {
    #[must_use]
    pub fn new(items: Vec<T>) -> Self {
        Self {
            items,
            query: String::new(),
            selected: 0,
        }
    }

    #[must_use]
    pub fn with_selected(items: Vec<T>, selected: usize) -> Self {
        let mut dropdown = Self::new(items);
        dropdown.selected = selected.min(dropdown.items.len().saturating_sub(1));
        dropdown
    }

    pub fn insert(&mut self, character: char) {
        if !character.is_control() {
            self.query.push(character);
            self.selected = 0;
        }
    }

    pub fn insert_str(&mut self, text: &str) {
        self.query
            .extend(text.chars().filter(|character| !character.is_control()));
        self.selected = 0;
    }

    pub fn backspace(&mut self) {
        self.query.pop();
        self.selected = 0;
    }

    pub fn clear(&mut self) {
        self.query.clear();
        self.selected = 0;
    }

    #[must_use]
    pub fn filtered_items<F>(&self, search_text: F) -> Vec<&T>
    where
        F: Fn(&T) -> String,
    {
        let query = self.query.to_lowercase();
        self.items
            .iter()
            .filter(|item| query.is_empty() || search_text(item).to_lowercase().contains(&query))
            .collect()
    }

    pub fn move_selection<F>(&mut self, delta: isize, search_text: F)
    where
        F: Fn(&T) -> String,
    {
        let length = self.filtered_items(search_text).len();
        if length > 0 {
            self.selected = offset_index(self.selected, length, delta);
        }
    }

    #[must_use]
    pub fn selected_item<F>(&self, search_text: F) -> Option<&T>
    where
        F: Fn(&T) -> String,
    {
        self.filtered_items(search_text).get(self.selected).copied()
    }
}

fn offset_index(current: usize, length: usize, delta: isize) -> usize {
    debug_assert!(length > 0);
    let amount = delta.unsigned_abs() % length;
    if delta.is_negative() {
        (current + length - amount) % length
    } else {
        (current + amount) % length
    }
}

#[cfg(test)]
mod tests {
    use super::SearchableDropdown;

    fn searchable(item: &&str) -> String {
        (*item).to_owned()
    }

    #[test]
    fn filters_case_insensitively_and_selects_from_filtered_items() {
        let mut dropdown = SearchableDropdown::new(vec!["Alpha", "Beta", "Alpine"]);
        dropdown.insert_str("ALP");

        assert_eq!(
            dropdown.filtered_items(searchable),
            vec![&"Alpha", &"Alpine"]
        );
        dropdown.move_selection(1, searchable);
        assert_eq!(dropdown.selected_item(searchable), Some(&"Alpine"));
    }

    #[test]
    fn query_changes_reset_selection_and_navigation_wraps() {
        let mut dropdown = SearchableDropdown::with_selected(vec!["one", "two"], 1);
        dropdown.move_selection(1, searchable);
        assert_eq!(dropdown.selected, 0);

        dropdown.insert('t');
        assert_eq!(dropdown.selected, 0);
        assert_eq!(dropdown.selected_item(searchable), Some(&"two"));
        dropdown.backspace();
        assert!(dropdown.query.is_empty());
    }

    #[test]
    fn empty_results_have_no_selection() {
        let mut dropdown = SearchableDropdown::new(vec!["one"]);
        dropdown.insert_str("missing");

        assert_eq!(dropdown.selected_item(searchable), None);
        dropdown.move_selection(1, searchable);
        assert_eq!(dropdown.selected, 0);
    }
}
