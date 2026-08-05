# Initial-session deletion invariant

`ServerCore::default_session` names a **live control-plane engine**, not a permanent historical exemption for the logical session that first occupied that engine.

Deletion follows these rules:

1. Any session with work in flight behind a live provider is refused.
2. The current default is additionally refused while it owns a live native provider session. This preserves the workspace lifecycle resource that still serves it.
3. Once that native resource is closed/disconnected, deleting the former initial session first creates a fresh, unpersisted control-plane successor and atomically points `default_session` at it. Only then is the former engine released and its persisted history deleted.
4. The successor is not listed as a conversation until provider work persists it. This avoids recreating the epoch-timestamp `New session` records that older discovery projected from an uncreated control-plane engine.
5. A retry or a stale persisted row for the former id is an ordinary persistence delete. It cannot regain initial-session protection because the live role already names the successor.

The delete effects are executed through the post-command default engine. Provider release remains ordered before persistence deletion, and creating/opening later sessions continues from the successor normally.
