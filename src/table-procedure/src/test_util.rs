// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::time::Duration;

use catalog::local::MemoryCatalogManager;
use catalog::CatalogManagerRef;
use common_procedure::local::{LocalManager, ManagerConfig};
use common_procedure::store::state_store::ObjectStateStore;
use common_procedure::ProcedureManagerRef;
use common_test_util::temp_dir::{create_temp_dir, TempDir};
use log_store::NoopLogStore;
use mito::config::EngineConfig;
use mito::engine::MitoEngine;
use object_store::services::Fs;
use object_store::ObjectStore;
use storage::compaction::noop::NoopCompactionScheduler;
use storage::config::EngineConfig as StorageEngineConfig;
use storage::EngineImpl;

pub struct TestEnv {
    pub dir: TempDir,
    pub table_engine: Arc<MitoEngine<EngineImpl<NoopLogStore>>>,
    pub procedure_manager: ProcedureManagerRef,
    pub catalog_manager: CatalogManagerRef,
}

impl TestEnv {
    pub fn new(prefix: &str) -> TestEnv {
        let dir = create_temp_dir(prefix);
        let store_dir = format!("{}/db", dir.path().to_string_lossy());
        let mut builder = Fs::default();
        builder.root(&store_dir);
        let object_store = ObjectStore::new(builder).unwrap().finish();

        let compaction_scheduler = Arc::new(NoopCompactionScheduler::default());
        let storage_engine = EngineImpl::new(
            StorageEngineConfig::default(),
            Arc::new(NoopLogStore::default()),
            object_store.clone(),
            compaction_scheduler,
        );
        let table_engine = Arc::new(MitoEngine::new(
            EngineConfig::default(),
            storage_engine,
            object_store,
        ));

        let procedure_dir = format!("{}/procedure", dir.path().to_string_lossy());
        let mut builder = Fs::default();
        builder.root(&procedure_dir);
        let object_store = ObjectStore::new(builder).unwrap().finish();

        let config = ManagerConfig {
            max_retry_times: 3,
            retry_delay: Duration::from_secs(500),
        };
        let state_store = Arc::new(ObjectStateStore::new(object_store));
        let procedure_manager = Arc::new(LocalManager::new(config, state_store));

        let catalog_manager = Arc::new(MemoryCatalogManager::default());

        TestEnv {
            dir,
            table_engine,
            procedure_manager,
            catalog_manager,
        }
    }
}
