use std::{
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
    mem_table::MemTable,
    ss_table::SSTable,
    utils::AtomicOperation,
};

pub(crate) trait MemTableControllerPolicy: Sync + Send {
    /// Evaluate a set of mem_table for its maturity
    fn is_mature(&self, mem_table: &MemTable) -> bool;
    /// Returns the duration left till next evaluation
    fn next_schedule(&self) -> Duration;
}

// #[derive(Default)]
pub(crate) struct MemTableController {
    kill_signal_sender: Sender<()>,
    task_handle: Option<JoinHandle<()>>,
}

impl MemTableController {
    pub fn start(
        mem_tables: Arc<SkipSet<Arc<MemTable>>>,
        ss_tables: Arc<SkipSet<Arc<SSTable>>>,
        bloom_filter_size_bytes: usize,
        bloom_filter_item_count: usize,
        sparse_index_range_size: usize,
        policy_config: &str,
    ) -> LiteDbResult<Self> {
        let policy = MemTableController::create_policy(policy_config)?;
        let (kill_signal_sender, kill_signal_receiver) = bounded(1);
        let ticker = tick(policy.next_schedule());
        let task_handle = thread::spawn(move || {
            loop {
                select! {
                    recv(ticker) -> _ => (),
                    recv(kill_signal_receiver) -> _ => break,
                };
                //let mut mem_table_lock = current_mem_table.write();
                let current_mem_table = mem_tables.front().unwrap();
                if !policy.is_mature(&current_mem_table) {
                    continue;
                }

                // Swap to current_mem_table with new_mem_table
                let dir = current_mem_table.dir();
                let id = current_mem_table.id();
                let new_mem_table = MemTable::open(dir, id + 1).unwrap();
                mem_tables.insert(Arc::new(new_mem_table));

                // Persist current_mem_table & publish it.
                let ss_table = current_mem_table
                    .save(
                        bloom_filter_size_bytes,
                        bloom_filter_item_count,
                        sparse_index_range_size,
                    )
                    .unwrap();
                let atomic_op = AtomicOperation::new(|| {
                    mem_tables.clone().remove(current_mem_table.as_ref());
                    ss_tables.clone().insert(ss_table.clone());
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
    fn create_policy(config: &str) -> LiteDbResult<Arc<dyn MemTableControllerPolicy>> {
        let (name, arguments) = match config.trim().split_once(':') {
            Some((name, arguments)) => (name, arguments),
            None => (config, ""),
        };

        let policy = match name {
            "size_tiered" => {
                let argument_values = arguments
                    .trim()
                    .split(' ')
                    .map(|arg| arg.parse())
                    .collect::<Result<Vec<usize>, _>>()
                    .map_err(|err| {
                        LiteDbError::PolicyError(format!(
                            "Couldn't parse `SizeTieredMemTableController` arguments: {err:?}"
                        ))
                    })?;

                if argument_values.len() < 2 {
                    return Err(LiteDbError::PolicyError(format!("Not enough arguments passed to `SizeTieredMemTableController`. Needed 2 but {} given", argument_values.len())));
                }
                SizeTieredMemTableController::new(argument_values[0], argument_values[1])
            }
            _ => {
                return Err(LiteDbError::PolicyError(format!(
                    "There is not MemTableControllerPolicy matching this configuration `{config}`."
                )))
            }
        };
        Ok(Arc::new(policy))
    }
}

struct SizeTieredMemTableController {
    max_entries: usize,
    max_size_bytes: usize,
}

impl SizeTieredMemTableController {
    pub(crate) fn new(max_entries: usize, max_size_bytes: usize) -> Self {
        Self {
            max_entries,
            max_size_bytes,
        }
    }
}

impl MemTableControllerPolicy for SizeTieredMemTableController {
    fn is_mature(&self, mem_table: &MemTable) -> bool {
        mem_table.is_full(self.max_entries, self.max_size_bytes)
    }

    fn next_schedule(&self) -> Duration {
        Duration::from_secs(3)
    }
}
