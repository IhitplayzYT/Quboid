pub mod dbs{
use chrono::{DateTime, NaiveDateTime, Utc};
use mysql::{
    Params, Pool, PooledConn, TxOpts, params, prelude::*, Value,
};

use crate::model::misc::misc::{Token, build_extraction_regex, extract_row, generate_create_table, parse_format};

    pub struct DataBase{
        pub pool: Pool,
    }

    impl DataBase{
        fn new(url: &str) -> mysql::Result<Self>{
            Ok(Self { pool:Pool::new(url)? })
        }

        fn conn(&self) -> mysql::Result<PooledConn>{
            self.pool.get_conn()
        }

        pub fn import_with_format(&self,table_name: &str,format: &str,raw_data: &str) -> Result<(), Box<dyn std::error::Error>> {
            let mut conn = self.conn()?;
            let tokens = parse_format(format)?;
            let create_sql = generate_create_table(table_name, &tokens)?;
            conn.query_drop(&create_sql)?;
            let extraction_regex = build_extraction_regex(&tokens)?;
            let columns: Vec<String> = tokens.iter().filter_map(|token| {
                    if let Token::Column { name, .. } = token {
                        Some(name.clone())
                    } else {
                        None
                    }
                }).collect();

            let placeholders = vec!["?"; columns.len()].join(", ");

            let insert_sql = format!(
                "INSERT INTO \"{}\" ({}) VALUES ({})",
                table_name,
                columns.iter().map(|c| format!("\"{}\"", c)).collect::<Vec<_>>().join(", "),placeholders
            );

            let mut tx = conn.start_transaction(TxOpts::default())?;
            for line in raw_data.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let values = extract_row(&extraction_regex,&tokens,line)?;
                let mysql_values: Vec<Value> = values.into_iter().map(|v| Value::from(v)).collect();
                tx.exec::<mysql::Row, _, _>(&insert_sql, mysql_values)?;
            }

            tx.commit()?;

            Ok(())
        }

    }



}