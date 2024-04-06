use std::{
    collections::BTreeSet,
    sync::Arc,
    thread::{self, JoinHandle},
    time::Duration,
};

use crossbeam::{
    channel::{bounded, tick, Sender},
    select,
};
use crossbeam_skiplist::SkipSet;

use crate::{error::LiteDbResult, ss_table::SSTable, utils::AtomicOperationExecutor};

pub(crate) trait CompactionPolicy: Sync + Send {
    /// Evaluate a set of ss_tables and returns merge-able groups of ss_tables.
    fn evaluate(&self, ss_tables: Vec<Arc<SSTable>>) -> Vec<Vec<Arc<SSTable>>>;
    /// Returns the duration left till next evaluation
    fn next_schedule(&self) -> Duration;
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug)]
pub enum CompactorPolicyConfig {
    SizeTiered,
}

// #[derive(Default)]
pub(crate) struct Compactor {
    kill_signal_sender: Sender<()>,
    task_handle: Option<JoinHandle<()>>,
}

impl Compactor {
    pub fn start(
        ss_tables: Arc<SkipSet<Arc<SSTable>>>,
        atomic_operation_executor: Arc<AtomicOperationExecutor>,
        compactor_policy: &CompactorPolicyConfig,
    ) -> LiteDbResult<Self> {
        let policy = Compactor::create_policy(compactor_policy)?;
        let (kill_signal_sender, kill_signal_receiver) = bounded(1);
        let ticker = tick(policy.next_schedule());
        let task_handle = thread::spawn(move || {
            loop {
                select! {
                    recv(ticker) -> _ => (),
                    recv(kill_signal_receiver) -> _ => break,
                };
                let candidate_ss_tables = ss_tables
                    .iter()
                    .map(|entry| entry.value().clone())
                    .collect::<Vec<_>>();

                let compaction_groups = policy.evaluate(candidate_ss_tables);
                if compaction_groups.is_empty() {
                    continue;
                }

                //TODO: perform merge operations and publish
                let (new_tables, old_tables) = do_compaction(compaction_groups);
                atomic_operation_executor.perform(|| {
                    for table in &old_tables {
                        ss_tables.remove(table);
                    }
                    for table in &new_tables {
                        ss_tables.insert(table.clone());
                    }
                });
            }
        });
        Ok(Self {
            kill_signal_sender,
            task_handle: Some(task_handle),
        })
    }

    pub fn stop(&mut self) {
        self.kill_signal_sender.send(()).unwrap();
        let task_handle = self.task_handle.take().unwrap();
        task_handle.join().unwrap();
    }

    /// Creates a policy from a CompactorPolicyConfig.
    fn create_policy(
        policy_config: &CompactorPolicyConfig,
    ) -> LiteDbResult<Arc<dyn CompactionPolicy>> {
        let policy = match policy_config {
            CompactorPolicyConfig::SizeTiered => SizeTieredCompactor,
        };
        Ok(Arc::new(policy))
    }
}

fn do_compaction(
    _compaction_groups: Vec<Vec<Arc<SSTable>>>,
) -> (BTreeSet<Arc<SSTable>>, BTreeSet<Arc<SSTable>>) {
    let new_tables: BTreeSet<Arc<SSTable>> = BTreeSet::new();
    let old_tables: BTreeSet<Arc<SSTable>> = BTreeSet::new();

    // TODO:

    (new_tables, old_tables)
}

struct SizeTieredCompactor;

impl CompactionPolicy for SizeTieredCompactor {
    fn evaluate(&self, _ss_tables: Vec<Arc<SSTable>>) -> Vec<Vec<Arc<SSTable>>> {
        vec![] // TODO:
    }

    fn next_schedule(&self) -> Duration {
        // check every 10min, could be in config.
        Duration::from_secs(60 * 10)
    }
}
