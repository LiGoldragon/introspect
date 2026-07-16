use std::collections::HashMap;

use signal_introspect::{
    BootIdentifier, CoalescedSystemEvent, CoalescingClosure, EventInstant, ExactCoalescingStatus,
    ExactDuplicateIdentity, SystemEvent, SystemEventAccepted,
};

const DEFAULT_MAXIMUM_ACTIVE_KEYS: usize = 10_000;
const DEFAULT_INTERVAL_MICROSECONDS: u64 = 60_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExactCoalescingPolicy {
    maximum_active_keys: usize,
    interval_microseconds: u64,
}

impl ExactCoalescingPolicy {
    pub const fn new(maximum_active_keys: usize, interval_microseconds: u64) -> Self {
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

#[derive(Debug, Clone, PartialEq, Eq)]
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
            representative_identifier: self.current.representative.identifier,
            count: self.current.count,
            suppressed_count: self.current.suppressed_count,
        }
    }
}

#[derive(Debug)]
pub struct ExactDuplicateCoalescer {
    policy: ExactCoalescingPolicy,
    active: HashMap<ExactDuplicateIdentity, CoalescedSystemEvent>,
    evictions: u64,
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
        let now = event.observed_at;
        let mut closed = self.close_expired(&event.boot, now);
        let identity = event.exact_duplicate_identity();
        if let Some(summary) = self.active.get_mut(&identity) {
            summary.count = summary.count.saturating_add(1);
            summary.suppressed_count = summary.count.saturating_sub(1);
            summary.last_seen = now;
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

        let summary = CoalescedSystemEvent::new(event, CoalescingClosure::Active);
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
            .filter(|(_, summary)| &summary.representative.boot == boot)
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
            active_keys: self.active.len() as u64,
            evictions: self.evictions,
            maximum_active_keys: self.policy.maximum_active_keys as u64,
        }
    }

    fn close_expired(
        &mut self,
        boot: &BootIdentifier,
        now: EventInstant,
    ) -> Vec<CoalescedSystemEvent> {
        let identities = self
            .active
            .iter()
            .filter(|(_, summary)| &summary.representative.boot == boot)
            .filter(|(_, summary)| {
                now.value().saturating_sub(summary.first_seen.value())
                    >= self.policy.interval_microseconds
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
                summary.closure = closure;
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
                    summary.last_seen.value(),
                    summary.representative.identifier.value(),
                )
            })
            .map(|(identity, _)| identity.clone())?;
        let mut summary = self.active.remove(&identity)?;
        summary.closure = CoalescingClosure::Eviction;
        self.evictions = self.evictions.saturating_add(1);
        Some(summary)
    }
}

impl Default for ExactDuplicateCoalescer {
    fn default() -> Self {
        Self::new(ExactCoalescingPolicy::default())
    }
}
