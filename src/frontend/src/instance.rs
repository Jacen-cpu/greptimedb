use std::collections::HashMap;
use std::sync::Arc;

use api::helper::ColumnDataTypeWrapper;
use api::v1::{
    insert_expr, AdminExpr, AdminResult, ColumnDataType, ColumnDef as GrpcColumnDef, CreateExpr,
    InsertExpr, ObjectExpr, ObjectResult as GrpcObjectResult,
};
use async_trait::async_trait;
use client::admin::{admin_result_to_output, Admin};
use client::{Client, Database, Select};
use common_error::prelude::BoxedError;
use datatypes::schema::ColumnSchema;
use query::Output;
use servers::error as server_error;
use servers::query_handler::{GrpcAdminHandler, GrpcQueryHandler, SqlQueryHandler};
use snafu::prelude::*;
use sql::ast::{ColumnDef, TableConstraint};
use sql::statements::create_table::{CreateTable, TIME_INDEX};
use sql::statements::statement::Statement;
use sql::statements::{column_def_to_schema, table_idents_to_full_name};
use sql::{dialect::GenericDialect, parser::ParserContext};

use crate::error::{self, Result};
use crate::frontend::FrontendOptions;

pub(crate) type InstanceRef = Arc<Instance>;

pub struct Instance {
    db: Database,
    admin: Admin,
}

impl Instance {
    pub(crate) fn new() -> Self {
        let client = Client::default();
        let db = Database::new("greptime", client.clone());
        let admin = Admin::new("greptime", client);
        Self { db, admin }
    }

    pub(crate) async fn start(&mut self, opts: &FrontendOptions) -> Result<()> {
        let addr = opts.datanode_grpc_addr();
        self.db
            .start(addr.clone())
            .await
            .context(error::ConnectDatanodeSnafu { addr: addr.clone() })?;
        self.admin
            .start(addr.clone())
            .await
            .context(error::ConnectDatanodeSnafu { addr })?;
        Ok(())
    }
}

#[cfg(test)]
impl Instance {
    pub fn with_client(client: Client) -> Self {
        Self {
            db: Database::new("greptime", client.clone()),
            admin: Admin::new("greptime", client),
        }
    }
}

#[async_trait]
impl SqlQueryHandler for Instance {
    async fn do_query(&self, query: &str) -> server_error::Result<Output> {
        let mut stmt = ParserContext::create_with_dialect(query, &GenericDialect {})
            .map_err(BoxedError::new)
            .context(server_error::ExecuteQuerySnafu { query })?;
        if stmt.len() != 1 {
            // TODO(LFC): Support executing multiple SQLs,
            // which seems to be a major change to our whole server framework?
            return server_error::NotSupportedSnafu {
                feat: "Only one SQL is allowed to be executed at one time.",
            }
            .fail();
        }
        let stmt = stmt.remove(0);

        match stmt {
            Statement::Query(_) => self
                .db
                .select(Select::Sql(query.to_string()))
                .await
                .and_then(|object_result| object_result.try_into()),
            Statement::Insert(insert) => {
                let table_name = insert.table_name();
                let expr = InsertExpr {
                    table_name,
                    expr: Some(insert_expr::Expr::Sql(query.to_string())),
                };
                self.db
                    .insert(expr)
                    .await
                    .and_then(|object_result| object_result.try_into())
            }
            Statement::Create(create) => {
                let expr = create_to_expr(create)
                    .map_err(BoxedError::new)
                    .context(server_error::ExecuteQuerySnafu { query })?;
                self.admin
                    .create(expr)
                    .await
                    .and_then(admin_result_to_output)
            }
            // TODO(LFC): Support other SQL execution,
            // update, delete, alter, explain, etc.
            _ => return server_error::NotSupportedSnafu { feat: query }.fail(),
        }
        .map_err(BoxedError::new)
        .context(server_error::ExecuteQuerySnafu { query })
    }

    async fn insert_script(&self, _name: &str, _script: &str) -> server_error::Result<()> {
        server_error::NotSupportedSnafu {
            feat: "Script execution in Frontend",
        }
        .fail()
    }

    async fn execute_script(&self, _script: &str) -> server_error::Result<Output> {
        server_error::NotSupportedSnafu {
            feat: "Script execution in Frontend",
        }
        .fail()
    }
}

fn create_to_expr(create: CreateTable) -> Result<CreateExpr> {
    let (catalog_name, schema_name, table_name) =
        table_idents_to_full_name(&create.name).context(error::ParseSqlSnafu)?;

    let expr = CreateExpr {
        catalog_name,
        schema_name,
        table_name,
        column_defs: columns_to_expr(&create.columns)?,
        time_index: find_time_index(&create.constraints)?,
        primary_keys: find_primary_keys(&create.constraints)?,
        create_if_not_exists: create.if_not_exists,
        // TODO(LFC): Fill in other table options.
        table_options: HashMap::from([("engine".to_string(), create.engine)]),
        ..Default::default()
    };
    Ok(expr)
}

fn find_primary_keys(constraints: &[TableConstraint]) -> Result<Vec<String>> {
    let primary_keys = constraints
        .iter()
        .filter_map(|constraint| match constraint {
            TableConstraint::Unique {
                name: _,
                columns,
                is_primary: true,
            } => Some(columns.iter().map(|ident| ident.value.clone())),
            _ => None,
        })
        .flatten()
        .collect::<Vec<String>>();
    Ok(primary_keys)
}

fn find_time_index(constraints: &[TableConstraint]) -> Result<String> {
    let time_index = constraints
        .iter()
        .filter_map(|constraint| match constraint {
            TableConstraint::Unique {
                name: Some(name),
                columns,
                is_primary: false,
            } => {
                if name.value == TIME_INDEX {
                    Some(columns.iter().map(|ident| &ident.value))
                } else {
                    None
                }
            }
            _ => None,
        })
        .flatten()
        .collect::<Vec<&String>>();
    ensure!(
        time_index.len() == 1,
        error::InvalidSqlSnafu {
            err_msg: "must have one and only one TimeIndex columns",
        }
    );
    Ok(time_index.first().unwrap().to_string())
}

fn columns_to_expr(column_defs: &[ColumnDef]) -> Result<Vec<GrpcColumnDef>> {
    let column_schemas = column_defs
        .iter()
        .map(|c| column_def_to_schema(c).context(error::ParseSqlSnafu))
        .collect::<Result<Vec<ColumnSchema>>>()?;

    let column_datatypes = column_schemas
        .iter()
        .map(|c| {
            ColumnDataTypeWrapper::try_from(c.data_type.clone())
                .map(|w| w.datatype())
                .context(error::ColumnDataTypeSnafu)
        })
        .collect::<Result<Vec<ColumnDataType>>>()?;

    Ok(column_schemas
        .iter()
        .zip(column_datatypes.into_iter())
        .map(|(schema, datatype)| GrpcColumnDef {
            name: schema.name.clone(),
            data_type: datatype as i32,
            is_nullable: schema.is_nullable,
        })
        .collect::<Vec<GrpcColumnDef>>())
}

#[async_trait]
impl GrpcQueryHandler for Instance {
    async fn do_query(&self, query: ObjectExpr) -> server_error::Result<GrpcObjectResult> {
        self.db
            .object(query.clone())
            .await
            .map_err(BoxedError::new)
            .with_context(|_| server_error::ExecuteQuerySnafu {
                query: format!("{:?}", query),
            })
    }
}

#[async_trait]
impl GrpcAdminHandler for Instance {
    async fn exec_admin_request(&self, expr: AdminExpr) -> server_error::Result<AdminResult> {
        self.admin
            .do_request(expr.clone())
            .await
            .map_err(BoxedError::new)
            .with_context(|_| server_error::ExecuteQuerySnafu {
                query: format!("{:?}", expr),
            })
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use api::v1::codec::{InsertBatch, SelectResult};
    use api::v1::greptime_client::GreptimeClient;
    use api::v1::{
        admin_expr, admin_result, column, object_expr, object_result, select_expr, Column,
        ExprHeader, MutateResult, SelectExpr,
    };
    use datafusion::arrow_print;
    use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
    use datanode::datanode::{DatanodeOptions, ObjectStoreConfig};
    use datanode::instance::Instance as DatanodeInstance;
    use servers::grpc::GrpcServer;
    use tempdir::TempDir;
    use tonic::transport::{Endpoint, Server};
    use tower::service_fn;

    use super::*;

    #[tokio::test]
    async fn test_execute_sql() {
        common_telemetry::init_default_ut_logging();

        let datanode_instance = create_datanode_instance().await;
        let frontend_instance = create_frontend_instance(datanode_instance).await;

        let sql = r#"CREATE TABLE demo(
                            host STRING,
                            ts TIMESTAMP,
                            cpu DOUBLE NULL,
                            memory DOUBLE NULL,
                            TIME INDEX (ts),
                            PRIMARY KEY(ts, host)
                        ) engine=mito with(regions=1);"#;
        let output = SqlQueryHandler::do_query(&*frontend_instance, sql)
            .await
            .unwrap();
        match output {
            Output::AffectedRows(rows) => assert_eq!(rows, 1),
            _ => unreachable!(),
        }

        let sql = r#"insert into demo(host, cpu, memory, ts) values 
                                ('frontend.host1', 1.1, 100, 1000),
                                ('frontend.host2', null, null, 2000),
                                ('frontend.host3', 3.3, 300, 3000)
                                "#;
        let output = SqlQueryHandler::do_query(&*frontend_instance, sql)
            .await
            .unwrap();
        match output {
            Output::AffectedRows(rows) => assert_eq!(rows, 3),
            _ => unreachable!(),
        }

        let sql = "select * from demo";
        let output = SqlQueryHandler::do_query(&*frontend_instance, sql)
            .await
            .unwrap();
        match output {
            Output::RecordBatches(recordbatches) => {
                let recordbatches = recordbatches
                    .to_vec()
                    .into_iter()
                    .map(|r| r.df_recordbatch)
                    .collect::<Vec<DfRecordBatch>>();
                let pretty_print = arrow_print::write(&recordbatches);
                let pretty_print = pretty_print.lines().collect::<Vec<&str>>();
                let expected = vec![
                    "+----------------+---------------------+-----+--------+",
                    "| host           | ts                  | cpu | memory |",
                    "+----------------+---------------------+-----+--------+",
                    "| frontend.host1 | 1970-01-01 00:00:01 | 1.1 | 100    |",
                    "| frontend.host2 | 1970-01-01 00:00:02 |     |        |",
                    "| frontend.host3 | 1970-01-01 00:00:03 | 3.3 | 300    |",
                    "+----------------+---------------------+-----+--------+",
                ];
                assert_eq!(pretty_print, expected);
            }
            _ => unreachable!(),
        };
    }

    #[tokio::test]
    async fn test_execute_grpc() {
        common_telemetry::init_default_ut_logging();

        let datanode_instance = create_datanode_instance().await;
        let frontend_instance = create_frontend_instance(datanode_instance).await;

        // testing data:
        let expected_host_col = Column {
            column_name: "host".to_string(),
            values: Some(column::Values {
                string_values: vec!["fe.host.a", "fe.host.b", "fe.host.c", "fe.host.d"]
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect(),
                ..Default::default()
            }),
            datatype: 12, // string
            ..Default::default()
        };
        let expected_cpu_col = Column {
            column_name: "cpu".to_string(),
            values: Some(column::Values {
                f64_values: vec![1.0, 3.0, 4.0],
                ..Default::default()
            }),
            null_mask: vec![2],
            datatype: 10, // float64
            ..Default::default()
        };
        let expected_mem_col = Column {
            column_name: "memory".to_string(),
            values: Some(column::Values {
                f64_values: vec![100.0, 200.0, 400.0],
                ..Default::default()
            }),
            null_mask: vec![4],
            datatype: 10, // float64
            ..Default::default()
        };
        let expected_ts_col = Column {
            column_name: "ts".to_string(),
            values: Some(column::Values {
                ts_millis_values: vec![1000, 2000, 3000, 4000],
                ..Default::default()
            }),
            datatype: 15, // timestamp
            ..Default::default()
        };

        // create
        let create_expr = create_expr();
        let admin_expr = AdminExpr {
            header: Some(ExprHeader::default()),
            expr: Some(admin_expr::Expr::Create(create_expr)),
        };
        let result = GrpcAdminHandler::exec_admin_request(&*frontend_instance, admin_expr)
            .await
            .unwrap();
        assert_matches!(
            result.result,
            Some(admin_result::Result::Mutate(MutateResult {
                success: 1,
                failure: 0
            }))
        );

        // insert
        let values = vec![InsertBatch {
            columns: vec![
                expected_host_col.clone(),
                expected_cpu_col.clone(),
                expected_mem_col.clone(),
                expected_ts_col.clone(),
            ],
            row_count: 4,
        }
        .into()];
        let insert_expr = InsertExpr {
            table_name: "demo".to_string(),
            expr: Some(insert_expr::Expr::Values(insert_expr::Values { values })),
        };
        let object_expr = ObjectExpr {
            header: Some(ExprHeader::default()),
            expr: Some(object_expr::Expr::Insert(insert_expr)),
        };
        let result = GrpcQueryHandler::do_query(&*frontend_instance, object_expr)
            .await
            .unwrap();
        assert_matches!(
            result.result,
            Some(object_result::Result::Mutate(MutateResult {
                success: 4,
                failure: 0
            }))
        );

        // select
        let object_expr = ObjectExpr {
            header: Some(ExprHeader::default()),
            expr: Some(object_expr::Expr::Select(SelectExpr {
                expr: Some(select_expr::Expr::Sql("select * from demo".to_string())),
            })),
        };
        let result = GrpcQueryHandler::do_query(&*frontend_instance, object_expr)
            .await
            .unwrap();
        match result.result {
            Some(object_result::Result::Select(select_result)) => {
                let select_result: SelectResult = (*select_result.raw_data).try_into().unwrap();

                assert_eq!(4, select_result.row_count);
                let actual_columns = select_result.columns;
                assert_eq!(4, actual_columns.len());

                // Respect the order in create table schema
                let expected_columns = vec![
                    expected_host_col,
                    expected_cpu_col,
                    expected_mem_col,
                    expected_ts_col,
                ];
                expected_columns
                    .iter()
                    .zip(actual_columns.iter())
                    .for_each(|(x, y)| assert_eq!(x, y));
            }
            _ => unreachable!(),
        }
    }

    async fn create_datanode_instance() -> Arc<DatanodeInstance> {
        let wal_tmp_dir = TempDir::new("/tmp/greptimedb_test_wal").unwrap();
        let data_tmp_dir = TempDir::new("/tmp/greptimedb_test_data").unwrap();
        let opts = DatanodeOptions {
            wal_dir: wal_tmp_dir.path().to_str().unwrap().to_string(),
            storage: ObjectStoreConfig::File {
                data_dir: data_tmp_dir.path().to_str().unwrap().to_string(),
            },
            ..Default::default()
        };

        let instance = Arc::new(DatanodeInstance::new(&opts).await.unwrap());
        instance.start().await.unwrap();
        instance
    }

    async fn create_frontend_instance(datanode_instance: Arc<DatanodeInstance>) -> Arc<Instance> {
        let (client, server) = tokio::io::duplex(1024);

        // create a mock datanode grpc service, see example here:
        // https://github.com/hyperium/tonic/blob/master/examples/src/mock/mock.rs
        let datanode_service =
            GrpcServer::new(datanode_instance.clone(), datanode_instance).create_service();
        tokio::spawn(async move {
            Server::builder()
                .add_service(datanode_service)
                .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(server)]))
                .await
        });

        // Move client to an option so we can _move_ the inner value
        // on the first attempt to connect. All other attempts will fail.
        let mut client = Some(client);
        // "http://[::]:50051" is just a placeholder, does not actually connect to it,
        // see https://github.com/hyperium/tonic/issues/727#issuecomment-881532934
        let channel = Endpoint::try_from("http://[::]:50051")
            .unwrap()
            .connect_with_connector(service_fn(move |_| {
                let client = client.take();

                async move {
                    if let Some(client) = client {
                        Ok(client)
                    } else {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "Client already taken",
                        ))
                    }
                }
            }))
            .await
            .unwrap();
        let client = Client::with_client(GreptimeClient::new(channel));
        Arc::new(Instance::with_client(client))
    }

    fn create_expr() -> CreateExpr {
        let column_defs = vec![
            GrpcColumnDef {
                name: "host".to_string(),
                data_type: 12, // string
                is_nullable: false,
            },
            GrpcColumnDef {
                name: "cpu".to_string(),
                data_type: 10, // float64
                is_nullable: true,
            },
            GrpcColumnDef {
                name: "memory".to_string(),
                data_type: 10, // float64
                is_nullable: true,
            },
            GrpcColumnDef {
                name: "ts".to_string(),
                data_type: 15, // timestamp
                is_nullable: true,
            },
        ];
        CreateExpr {
            table_name: "demo".to_string(),
            column_defs,
            time_index: "ts".to_string(),
            primary_keys: vec!["ts".to_string(), "host".to_string()],
            ..Default::default()
        }
    }
}