pub mod misc{
    use std::{collections::HashMap, error::Error};

use uuid::Uuid;



    #[derive(Debug,Clone,Copy,PartialEq, Eq, PartialOrd, Ord)]
    pub enum Priority{
        Low,
        Default,
        Medium,
        High
    }


    pub struct Maintainer{
        name: String,
        email: String,
        branch: String,
        other: Option<HashMap<String,String>>
    }


    #[derive()]
    pub struct MetaData{
        id: Uuid,
        gid: Uuid,
        ldbid: Uuid,
        maintainer: Maintainer,
        checksum: u64,
        name: String,
        desc: String,
        version: String,
        is_valid: bool,
        is_maintained: bool,
    }

    
pub enum e_Storage{
    Lake,
    Cache,
    Db
}

type Data = HashMap<String,String>;

type TaskResult = Result<(e_Storage,Data),Box<dyn Error>>;

pub trait TaskCruncher{
    fn ingest(&self,tid: Uuid, rule_map: HashMap<String,String>) -> TaskResult;
    fn ingest_all(&self,tids: Vec<Uuid>, rule_map: HashMap<String,String>) -> Vec<TaskResult>;
    fn ingest_batch(&self,tids: Vec<Uuid>, rule_map: HashMap<String,String>) -> Vec<TaskResult>;
    fn any_pending(&self) -> bool;
    fn is_done(&self) -> bool;
}


pub trait CacheConnector{
    fn insert(&self,k:String,v:String);
    fn delete(&self,k:String);
    fn contains(&self,k:String) -> bool;
    fn get(&self,k:String) -> Option<String>;
    fn update(&self,k:String,v:String);
}





}