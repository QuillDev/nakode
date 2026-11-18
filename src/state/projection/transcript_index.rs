//! The same mutation fence that protects prefix proofs also protects their viewport index.
//! A live-end edit rewinds just its own contribution; older edits re-prove the affected suffix.
use super::{DomainTranscript, EntryKind, entry_digest, hex_digest, visible_transcript_entry};
use crate::domain_transcript::{ProvenChain, ProvenStep};
use sha2::{Digest, Sha256};
use std::{collections::HashSet, sync::MutexGuard};

pub(super) fn indexed(transcript: &DomainTranscript) -> MutexGuard<'_, ProvenChain> {
    let entries = transcript.entries();
    let mut index = transcript.proven_chain();
    let from = index.unchanged.min(index.steps.len()).min(entries.len());
    if from == entries.len() && from == index.steps.len() && !index.start.is_empty() && from != 0 {
        return index;
    }
    let old_parents: HashSet<_> = index.steps[from..]
        .iter()
        .filter_map(|s| s.codemode_id.clone())
        .collect();
    let new_parents: HashSet<_> = entries[from..]
        .iter()
        .filter(|e| entry_digest(transcript, e).codemode_tool)
        .map(|e| e.id.clone())
        .collect();
    // Changing the parent set can change an earlier child's projection and proof.
    let from = old_parents
        .symmetric_difference(&new_parents)
        .filter_map(|parent| index.children_by_parent.get(parent)?.first())
        .copied()
        .fold(from, usize::min);
    rewind(&mut index, from);
    for entry in &entries[from..] {
        if entry_digest(transcript, entry).codemode_tool {
            index.codemode_tools.insert(entry.id.clone());
        }
    }
    let mut genesis = Sha256::new();
    genesis.update(b"nakode.transcript.prefix.v3");
    genesis.update([u8::from(transcript.has_earlier_entries())]);
    let genesis: [u8; 32] = genesis.finalize().into();
    index.start = hex_digest(&genesis);
    let mut chain = index.steps.last().map_or(genesis, |s| s.chain);
    for (offset, entry) in entries[from..].iter().enumerate() {
        let digest = entry_digest(transcript, entry);
        let visible = visible_transcript_entry(entry);
        let prefix = visible.then(|| {
            index.visible.push(from + offset);
            if entry.kind == EntryKind::User {
                index.owners.push(from + offset);
            }
            let mut step = Sha256::new();
            step.update(chain);
            step.update(digest.content);
            step.update((transcript.image_artifacts(entry).count() as u64).to_le_bytes());
            for (label, image) in transcript.image_artifacts(entry) {
                for bytes in [label.as_bytes(), image.mime_type.as_bytes(), &image.data] {
                    step.update((bytes.len() as u64).to_le_bytes());
                    step.update(bytes);
                }
            }
            if let Some(parent) = digest
                .parent_item_id
                .as_deref()
                .filter(|p| index.codemode_tools.contains(*p))
            {
                step.update([1]);
                step.update((parent.len() as u64).to_le_bytes());
                step.update(parent.as_bytes());
            } else {
                step.update([0]);
            }
            chain = step.finalize().into();
            hex_digest(&chain)
        });
        let tool_turn = entry
            .owner_turn_id
            .as_ref()
            .filter(|_| visible && matches!(entry.kind, EntryKind::Tool | EntryKind::Diff))
            .cloned();
        if let Some(turn) = &tool_turn {
            *index.tool_counts.entry(turn.clone()).or_default() += 1;
        }
        let position = from + offset;
        let first_user = index
            .steps
            .last()
            .and_then(|s| s.first_user)
            .or_else(|| (entry.kind == EntryKind::User).then_some(position));
        if let Some(parent) = &digest.parent_item_id {
            index
                .children_by_parent
                .entry(parent.clone())
                .or_default()
                .push(position);
        }
        index.by_id.insert(entry.id.clone(), position);
        index.steps.push(ProvenStep {
            id: entry.id.clone(),
            first_user,
            codemode_id: digest.codemode_tool.then(|| entry.id.clone()),
            digest,
            chain,
            prefix,
            tool_turn,
        });
    }
    index.unchanged = entries.len();
    transcript.prune_entry_digests();
    index
}

pub(super) fn first_user(transcript: &DomainTranscript) -> Option<&super::TranscriptEntry> {
    let i = indexed(transcript).steps.last()?.first_user?;
    transcript.entries().get(i)
}
pub(super) fn entry_by_id<'a>(
    transcript: &'a DomainTranscript,
    id: &str,
) -> Option<&'a super::TranscriptEntry> {
    let i = *indexed(transcript).by_id.get(id)?;
    transcript.entries().get(i)
}

fn rewind(index: &mut ProvenChain, from: usize) {
    while index.steps.len() > from {
        let step = index.steps.pop().expect("suffix step");
        index.by_id.remove(&step.id);
        if let Some(parent) = &step.digest.parent_item_id
            && let Some(children) = index.children_by_parent.get_mut(parent)
        {
            children.pop();
            if children.is_empty() {
                index.children_by_parent.remove(parent);
            }
        }
        if let Some(id) = step.codemode_id {
            index.codemode_tools.remove(&id);
        }
        if let Some(turn) = step.tool_turn
            && let Some(count) = index.tool_counts.get_mut(&turn)
        {
            *count -= 1;
            if *count == 0 {
                index.tool_counts.remove(&turn);
            }
        }
    }
    while index.visible.last().is_some_and(|i| *i >= from) {
        index.visible.pop();
    }
    while index.owners.last().is_some_and(|i| *i >= from) {
        index.owners.pop();
    }
}
