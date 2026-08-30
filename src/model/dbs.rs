pub mod dbs{
use chrono::{DateTime, NaiveDateTime, Utc};
use mysql::{
    params,
    prelude::*,
    Pool,
    PooledConn,
    TxOpts,
};

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

    }



}