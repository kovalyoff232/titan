use std::cell::RefCell;
use std::collections::HashSet;
use std::io;

thread_local! {
    static FAILPOINTS: RefCell<HashSet<String>> = RefCell::new(HashSet::new());
}

pub fn enable(name: &str) {
    FAILPOINTS.with(|set| {
        set.borrow_mut().insert(name.to_string());
    });
}

pub fn disable(name: &str) {
    FAILPOINTS.with(|set| {
        set.borrow_mut().remove(name);
    });
}

pub fn clear() {
    FAILPOINTS.with(|set| {
        set.borrow_mut().clear();
    });
}

pub fn is_enabled(name: &str) -> bool {
    let local_enabled = FAILPOINTS.with(|set| set.borrow().contains(name));
    if local_enabled {
        return true;
    }

    std::env::var("TITAN_FAILPOINTS")
        .ok()
        .map(|raw| raw.split(',').any(|v| v.trim() == name))
        .unwrap_or(false)
}

pub fn maybe_fail(name: &str) -> io::Result<()> {
    if is_enabled(name) {
        Err(io::Error::other(format!("failpoint triggered: {name}")))
    } else {
        Ok(())
    }
}
