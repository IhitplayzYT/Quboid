use crate::{helper::Helper::CLI, model::cache::cache::CacheConnector};

mod helper;
mod model;




use std::collections::VecDeque;

use uuid::Uuid;

use crate::model::{dbs::dbs::DataBase, misc::misc::{MetaData, TaskCruncher}, ob_store::ob_store::DataLake};


    pub struct Qube{
        pub metadata: Option<MetaData>,
        task_q: VecDeque<Uuid>,
        task_cr: Box<dyn TaskCruncher>,
        lake: DataLake,
        cacher: Box<dyn CacheConnector>,
        db: DataBase,
    }








fn _main() {
    let mut clargs = CLI::new();
    clargs.Parse_Args();
    if clargs.dbg{
        println!("{clargs:?}");
    }

    println!("Hello, world!");
}
