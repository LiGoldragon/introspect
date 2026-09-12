use std::collections::HashMap;

use signal_introspect::{
    BootIdentifier, CoalescedSystemEvent, CoalescingClosure, ExactCoalescingStatus, SystemEvent,
    SystemEventAccepted,
};

use crate::contract::{CoalescedReading, ExactDuplicateIdentity, coalesced};

const DEFAULT_MAXIMUM_ACTIVE_KEYS: usize = 10_000;
const DEFAULT_INTERVAL_MICROSECONDS: i64 = 60_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExactCoalescingPolicy {
    maximum_active_keys: usize,
    interval_microseconds: i64,
}

impl ExactCoalescingPolicy {
    pub const fn new(maximum_active_keys: usize, interval_microseconds: i64) -> Self {
        Self {
            maximum_active_keys,
            interval_microseconds,
        }
    }
}

impl Default for ExactCoalescingPolicy {
    fn default() -> Self {
        Self::new(DEFAULT_MAXIMUM_ACTIVE_KEYS, DEFAULT_INTERVAL_MICROSECONDS)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CoalescingUpdate {
    current: CoalescedSystemEvent,
    closed: Vec<CoalescedSystemEvent>,
}

impl CoalescingUpdate {
    pub fn current(&self) -> &CoalescedSystemEvent {
        &self.current
    }

    pub fn closed(&self) -> &[CoalescedSystemEvent] {
        self.closed.as_slice()
    }

    pub fn receipt(&self) -> SystemEventAccepted {
        SystemEventAccepted {
            event_identifier: self.current.representative().event_identifier,
            first_integer: self.current.count(),
            second_integer: self.current.suppressed_count(),
        }
    }
}

#[derive(Debug)]
pub struct ExactDuplicateCoalescer {
    policy: ExactCoalescingPolicy,
    active: HashMap<ExactDuplicateIdentity, CoalescedSystemEvent>,
    evictions: i64,
}

impl ExactDuplicateCoalescer {
    pub fn new(policy: ExactCoalescingPolicy) -> Self {
        Self {
            policy,
            active: HashMap::new(),
            evictions: 0,
        }
    }

    pub fn ingest(&mut self, event: SystemEvent) -> CoalescingUpdate {
        let now = event.event_instant;
        let boot = event.boot_identifier.clone();
        let mut closed = self.close_expired(&boot, now);
        let identity = ExactDuplicateIdentity::of(&event);
        if let Some(summary) = self.active.get_mut(&identity) {
            summary.first_integer = summary.first_integer.saturating_add(1);
            summary.second_integer = summary.first_integer.saturating_sub(1);
            summary.second_event_instant = now;
            return CoalescingUpdate {
                current: summary.clone(),
                closed,
            };
        }

        if self.active.len() >= self.policy.maximum_active_keys
            && let Some(evicted) = self.evict_oldest()
        {
            closed.push(evicted);
        }

        let summary = coalesced(event, CoalescingClosure::Active);
        self.active.insert(identity, summary.clone());
        CoalescingUpdate {
            current: summary,
            closed,
        }
    }

    pub fn flush_boot(
        &mut self,
        boot: &BootIdentifier,
        closure: CoalescingClosure,
    ) -> Vec<CoalescedSystemEvent> {
        let identities = self
            .active
            .iter()
            .filter(|(_, summary)| &summary.representative().boot_identifier == boot)
            .map(|(identity, _)| identity.clone())
            .collect::<Vec<_>>();
        self.close_identities(identities, closure)
    }

    pub fn flush_all(&mut self, closure: CoalescingClosure) -> Vec<CoalescedSystemEvent> {
        let identities = self.active.keys().cloned().collect::<Vec<_>>();
        self.close_identities(identities, closure)
    }

    pub fn status(&self) -> ExactCoalescingStatus {
        ExactCoalescingStatus {
            first_integer: self.active.len() as i64,
            second_integer: self.evictions,
            third_integer: self.policy.maximum_active_keys as i64,
        }
    }

    fn close_expired(&mut self, boot: &BootIdentifier, now: i64) -> Vec<CoalescedSystemEvent> {
        let identities = self
            .active
            .iter()
            .filter(|(_, summary)| &summary.representative().boot_identifier == boot)
            .filter(|(_, summary)| {
                now.saturating_sub(summary.first_seen()) >= self.policy.interval_microseconds
            })
            .map(|(identity, _)| identity.clone())
            .collect::<Vec<_>>();
        self.close_identities(identities, CoalescingClosure::Interval)
    }

    fn close_identities(
        &mut self,
        identities: Vec<ExactDuplicateIdentity>,
        closure: CoalescingClosure,
    ) -> Vec<CoalescedSystemEvent> {
        identities
            .into_iter()
            .filter_map(|identity| self.active.remove(&identity))
            .map(|mut summary| {
                summary.coalescing_closure = closure.clone();
                summary
            })
            .collect()
    }

    fn evict_oldest(&mut self) -> Option<CoalescedSystemEvent> {
        let identity = self
            .active
            .iter()
            .min_by_key(|(_, summary)| {
                (
                    summary.last_seen(),
                    summary.representative().event_identifier,
                )
            })
            .map(|(identity, _)| identity.clone())?;
        let mut summary = self.active.remove(&identity)?;
        summary.coalescing_closure = CoalescingClosure::Eviction;
        self.evictions = self.evictions.saturating_add(1);
        Some(summary)
    }
}

impl Default for ExactDuplicateCoalescer {
    fn default() -> Self {
        Self::new(ExactCoalescingPolicy::default())
    }
}
