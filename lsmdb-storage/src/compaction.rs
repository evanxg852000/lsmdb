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

use crate::{
    error::{LiteDbError, LiteDbResult},
    ss_table::SSTable,
    utils::AtomicOperation,
};

pub(crate) trait CompactionPolicy: Sync + Send {
    /// Evaluate a set of ss_tables and returns merge-able groups of ss_tables.
    fn evaluate(&self, ss_tables: Vec<Arc<SSTable>>) -> Vec<Vec<Arc<SSTable>>>;
    /// Returns the duration left till next evaluation
    fn next_schedule(&self) -> Duration;
}

// #[derive(Default)]
pub(crate) struct Compactor {
    kill_signal_sender: Sender<()>,
    task_handle: Option<JoinHandle<()>>,
}

impl Compactor {
    pub fn start(ss_tables: Arc<SkipSet<Arc<SSTable>>>, policy_config: &str) -> LiteDbResult<Self> {
        let policy = Compactor::create_policy(policy_config)?;
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
                let atomic_op = AtomicOperation::new(|| {
                    for table in &old_tables {
                        ss_tables.remove(table);
                    }
                    for table in &new_tables {
                        ss_tables.insert(table.clone());
                    }
                });
                atomic_op.perform();
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

    /// Creates a policy from a config string, this config is of the format.
    /// `{name}: {arg1} {arg1} ...` where:
    /// - name: is the name of the policy
    /// - args: is a space separate list or argument that will be used to instantiate the policy.
    fn create_policy(config: &str) -> LiteDbResult<Arc<dyn CompactionPolicy>> {
        let (name, _arguments) = match config.trim().split_once(':') {
            Some((name, arguments)) => (name, arguments),
            None => (config, ""),
        };

        let policy = match name {
            "size_tiered" => SizeTieredCompactor,
            _ => {
                return Err(LiteDbError::PolicyError(format!(
                    "No CompactionPolicy matching this configuration `{config}`."
                )))
            }
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
