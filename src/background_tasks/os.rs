//! Unix process observation and signalling for managed background tasks.
//!
//! A task is one process group whose leader this runtime spawned. Its identity is the group id
//! (equal to the leader pid) plus the leader's OS start time. The kernel never hands out an id
//! while a process group with that id still exists, so:
//!
//! - a live leader with the recorded start time is the task;
//! - a live process with the leader's id but another start time proves the original group is gone
//!   and the id was reused, so nothing in that group is ours;
//! - with the leader gone, any remaining member of the group is a survivor of the task.
//!
//! Descendants that left the group but are still parented by a member are included when stopping.
//! Processes that detach completely (double fork, reparenting to init, launchd/systemd hand-off)
//! cannot be attributed and are outside containment.

use std::collections::{HashMap, HashSet};

use tokio::process::Command;

/// Environment variable naming the task in every process of it, for operators and diagnostics.
pub(crate) const TASK_MARKER: &str = "NAKODE_BACKGROUND_TASK_ID";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProcessEntry {
    pub(crate) pid: i32,
    pub(crate) parent: i32,
    pub(crate) group: i32,
    pub(crate) zombie: bool,
    pub(crate) start: String,
}

/// One consistent listing of every process visible to this user.
#[derive(Clone, Debug, Default)]
pub(crate) struct ProcessTable {
    entries: HashMap<i32, ProcessEntry>,
}

impl ProcessTable {
    pub(crate) async fn capture() -> Result<Self, String> {
        let output = Command::new("ps")
            .args(["-A", "-o", "pid=,ppid=,pgid=,stat=,lstart="])
            .env("LC_ALL", "C")
            .output()
            .await
            .map_err(|error| format!("could not list processes: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "could not list processes: ps exited with {}",
                output.status
            ));
        }
        Ok(Self::parse(&String::from_utf8_lossy(&output.stdout)))
    }

    pub(crate) fn parse(listing: &str) -> Self {
        let entries = listing
            .lines()
            .filter_map(parse_line)
            .map(|entry| (entry.pid, entry))
            .collect();
        Self { entries }
    }

    pub(crate) fn start_of(&self, pid: i32) -> Option<&str> {
        self.entries.get(&pid).map(|entry| entry.start.as_str())
    }

    /// Live processes belonging to the task identified by `group` and its leader's start time.
    ///
    /// Group members come first, then descendants that changed group but are still parented by a
    /// member. An empty result means the task has no live process.
    pub(crate) fn task_processes(&self, group: i32, leader_start: &str) -> TaskProcesses {
        self.tracked_task_processes(group, leader_start, &mut HashMap::new())
    }

    /// Like [`Self::task_processes`], but remembers escaped descendants across listings.
    ///
    /// A descendant whose parent died is reparented and would no longer be found through parent
    /// links. `tracked` maps each descendant seen so far to its start time, so it stays part of
    /// the task while it is alive with that same identity.
    pub(crate) fn tracked_task_processes(
        &self,
        group: i32,
        leader_start: &str,
        tracked: &mut HashMap<i32, String>,
    ) -> TaskProcesses {
        if let Some(leader) = self.entries.get(&group)
            && leader.start != leader_start
        {
            return TaskProcesses::default();
        }
        let members = self
            .entries
            .values()
            .filter(|entry| entry.group == group && !entry.zombie)
            .map(|entry| entry.pid)
            .collect::<HashSet<_>>();
        let mut descendants = tracked
            .iter()
            .filter(|(pid, start)| {
                !members.contains(pid)
                    && self
                        .entries
                        .get(pid)
                        .is_some_and(|entry| !entry.zombie && &entry.start == *start)
            })
            .map(|(pid, _)| *pid)
            .collect::<Vec<_>>();
        let mut frontier = members
            .iter()
            .chain(&descendants)
            .copied()
            .collect::<Vec<_>>();
        let mut seen = frontier.iter().copied().collect::<HashSet<_>>();
        while let Some(parent) = frontier.pop() {
            for entry in self.entries.values() {
                if entry.parent == parent && !entry.zombie && seen.insert(entry.pid) {
                    descendants.push(entry.pid);
                    frontier.push(entry.pid);
                }
            }
        }
        for pid in &descendants {
            if let Some(entry) = self.entries.get(pid) {
                tracked.insert(*pid, entry.start.clone());
            }
        }
        let mut members = members.into_iter().collect::<Vec<_>>();
        members.sort_unstable();
        descendants.sort_unstable();
        TaskProcesses {
            members,
            descendants,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct TaskProcesses {
    /// Live members of the task's process group.
    pub(crate) members: Vec<i32>,
    /// Live descendants of members that moved to another process group.
    pub(crate) descendants: Vec<i32>,
}

impl TaskProcesses {
    pub(crate) fn is_empty(&self) -> bool {
        self.members.is_empty() && self.descendants.is_empty()
    }

    pub(crate) fn count(&self) -> usize {
        self.members.len() + self.descendants.len()
    }

    pub(crate) fn all(&self) -> Vec<i32> {
        self.members
            .iter()
            .chain(&self.descendants)
            .copied()
            .collect()
    }
}

fn parse_line(line: &str) -> Option<ProcessEntry> {
    let mut fields = line.split_whitespace();
    let pid = fields.next()?.parse().ok()?;
    let parent = fields.next()?.parse().ok()?;
    let group = fields.next()?.parse().ok()?;
    let stat = fields.next()?;
    let started = fields.collect::<Vec<_>>().join(" ");
    if started.is_empty() {
        return None;
    }
    Some(ProcessEntry {
        pid,
        parent,
        group,
        zombie: stat.starts_with('Z'),
        start: started,
    })
}

/// Sends `signal` to the task's process group and to its escaped descendants.
///
/// Callers pass processes observed in the same table that proved the group identity, so a
/// recycled id is never signalled. Returns the failures other than "already gone".
pub(crate) fn signal_task(
    group: i32,
    processes: &TaskProcesses,
    signal: nix::sys::signal::Signal,
) -> Vec<String> {
    use nix::{errno::Errno, sys::signal, unistd::Pid};

    let mut failures = Vec::new();
    if !processes.members.is_empty() {
        match signal::killpg(Pid::from_raw(group), signal) {
            Ok(()) | Err(Errno::ESRCH) => {}
            Err(error) => failures.push(format!("process group {group}: {error}")),
        }
    }
    for pid in &processes.descendants {
        match signal::kill(Pid::from_raw(*pid), signal) {
            Ok(()) | Err(Errno::ESRCH) => {}
            Err(error) => failures.push(format!("process {pid}: {error}")),
        }
    }
    failures
}

#[cfg(test)]
mod tests {
    use super::*;

    const LISTING: &str = "\
    1     0     1 Ss   Mon Oct 12 08:46:43 2026
  500     1   500 Ss   Thu Oct 23 09:00:00 2026
  501   500   500 S    Thu Oct 23 09:00:01 2026
  502   501   777 S    Thu Oct 23 09:00:02 2026
  503   500   500 Z    Thu Oct 23 09:00:03 2026
  900     1   900 S    Thu Oct 23 10:00:00 2026
";

    #[test]
    fn a_matching_leader_owns_its_group_and_escaped_descendants() {
        let table = ProcessTable::parse(LISTING);
        let processes = table.task_processes(500, "Thu Oct 23 09:00:00 2026");
        assert_eq!(processes.members, vec![500, 501]);
        assert_eq!(processes.descendants, vec![502]);
    }

    #[test]
    fn a_reused_leader_id_means_the_original_task_is_gone() {
        let table = ProcessTable::parse(LISTING);
        assert!(
            table
                .task_processes(900, "Thu Oct 23 09:59:59 2026")
                .is_empty()
        );
    }

    #[test]
    fn survivors_of_a_dead_leader_still_belong_to_the_task() {
        let listing = "\
  601     1   600 S    Thu Oct 23 09:00:01 2026
  602   601   600 S    Thu Oct 23 09:00:02 2026
";
        let table = ProcessTable::parse(listing);
        let processes = table.task_processes(600, "Thu Oct 23 09:00:00 2026");
        assert_eq!(processes.members, vec![601, 602]);
    }

    #[test]
    fn a_tracked_descendant_stays_in_the_task_after_reparenting() {
        let mut tracked = HashMap::new();
        let before = ProcessTable::parse(LISTING);
        before.tracked_task_processes(500, "Thu Oct 23 09:00:00 2026", &mut tracked);
        // 501 died, so its escaped child 502 was reparented to init.
        let after = ProcessTable::parse(
            "\
  500     1   500 Ss   Thu Oct 23 09:00:00 2026
  502     1   777 S    Thu Oct 23 09:00:02 2026
",
        );
        let processes = after.tracked_task_processes(500, "Thu Oct 23 09:00:00 2026", &mut tracked);
        assert_eq!(processes.descendants, vec![502]);
        assert!(
            after
                .task_processes(500, "Thu Oct 23 09:00:00 2026")
                .descendants
                .is_empty()
        );
    }

    #[test]
    fn a_tracked_descendant_with_a_reused_id_is_not_the_task() {
        let mut tracked = HashMap::from([(502, "Thu Oct 23 09:00:02 2026".to_owned())]);
        let table = ProcessTable::parse(
            "\
  500     1   500 Ss   Thu Oct 23 09:00:00 2026
  502     1   502 S    Thu Oct 23 11:00:00 2026
",
        );
        let processes = table.tracked_task_processes(500, "Thu Oct 23 09:00:00 2026", &mut tracked);
        assert!(processes.descendants.is_empty());
    }

    #[test]
    fn zombies_are_not_live_processes() {
        let listing = "  700     1   700 Z    Thu Oct 23 09:00:00 2026\n";
        let table = ProcessTable::parse(listing);
        assert!(
            table
                .task_processes(700, "Thu Oct 23 09:00:00 2026")
                .is_empty()
        );
    }
}
