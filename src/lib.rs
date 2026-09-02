use crate::{helper::Helper::CLI, model::{cache::cache::{CacheConnector, Rustis_Connector}, misc::misc::{CacheLayer, PrioScheduler, Priority, TaskResult, e_Storage}}};

mod helper;
mod model;




use std::{collections::{BinaryHeap, HashMap, VecDeque}, path::PathBuf, sync::{LazyLock, RwLock}};

use axum::middleware::MapRequestLayer;
use uuid::Uuid;

use crate::model::{dbs::dbs::DataBase, misc::misc::{MetaData, TaskCruncher}, ob_store::ob_store::DataLake};

    static TABLE_NAME: LazyLock<RwLock<usize>> = LazyLock::new(|| {RwLock::new(0)});


    pub struct Qube{
        pub metadata:MetaData,
        task_q: BinaryHeap<(Priority,Uuid)>,
        data_map: HashMap<Uuid,(String,HashMap<String,String>)>,
        exec_q: VecDeque<TaskResult>,
        task_cr: Option<Box<dyn TaskCruncher>>,
        lake: DataLake,
        cacher: Box<dyn CacheConnector>,
        db: DataBase,
        pub batch_size: usize,
    }


    impl Qube{
        pub fn new(metadata: Option<MetaData>,batch_size:usize,db_url: &str,blob_size: usize,obj_storage_path: Option<PathBuf>) -> Self{
            let mut ret = Self { metadata:metadata.unwrap_or_default(), task_q: BinaryHeap::new() , exec_q: VecDeque::new(), task_cr: None, lake: DataLake::new(blob_size, obj_storage_path), cacher: Box::new(Rustis_Connector::new("0.0.0.0",8080)),db:DataBase::new(db_url).unwrap(),data_map:HashMap::new(),batch_size};
            ret.task_cr = Some(Box::new(PrioScheduler::new(batch_size, ret.db.conn().unwrap(),blob_size)));
            ret
        }

        pub fn submit(&mut self,data:String,prio: Option<Priority>,rule: Option<HashMap<String,String>>,fmt:Option<String>){
            let id = Uuid::new_v4();
            self.data_map.insert(id, (data.clone(),rule.unwrap_or_default()));
            self.task_q.push((prio.unwrap_or(Priority::Default),id));
            if let Some(task_cr) = &mut self.task_cr{
                task_cr.insert(id, data, Some(CacheLayer::L1), prio.unwrap_or(Priority::Default), fmt);
            }
        }

        pub fn submit_all(&mut self,vect:Vec<(String,Option<Priority>,Option<HashMap<String,String>>,Option<String>)>){
            for (data,prio,rule,fmt) in vect{
                let id = Uuid::new_v4();
                self.data_map.insert(id, (data.clone(),rule.unwrap_or_default()));
                self.task_q.push((prio.unwrap_or(Priority::Default),id));
                if let Some(task_cr) = &mut self.task_cr{
                    task_cr.insert(id, data, Some(CacheLayer::L1), prio.unwrap_or(Priority::Default), fmt);
                }
            }
        }

        pub async fn batch_finalise(&mut self){
            let mut i = 0;
            while let Some((p,v)) = self.task_q.pop() && i < self.batch_size{
                i += 1;
                if let Some((data,rule)) = self.data_map.get(&v){
                    if let Some(task_cr) = self.task_cr.as_mut(){
                        self.exec_q.push_back(task_cr.ingest(v,rule.clone())); // We donto care about ingest batch since the binmheap does the prio ordering itself
                    }
                }
            }
            
            while let Some(z) = self.exec_q.pop_front(){
                if let Ok(p) = z{
                    match p.0{
                        e_Storage::Cache => {
                            for (k,v) in p.1{
                                let mut data = "".to_string();
                                let mut fmt = "".to_string();
                                let layer;
                                let mut cmnd = Vec::new();
                                match &k[..]{
                                    "CACHE_LAYER" => {
                                        layer = CacheLayer::from(v);
                                    },
                                    "DATA" => {
                                        data = v;
                                    },
                                    "FORMAT" => {
                                        fmt = v;
                                    },
                                    _ => {
                                        cmnd.push(v);
                                    }
                                }

                                for cmnd_str in cmnd{
                                    match &cmnd_str[..]{
                                        "insert" => {
                                            let st = data.find("KEY:").unwrap() + 4;
                                            let ed = data[st..].find("\n").unwrap();
                                            let key = data[st..ed].to_string();

                                            let st = data.find("VALUE:").unwrap() + 6;
                                            let ed = data[st..].find("\n").unwrap();
                                            let value = data[st..ed].to_string();

                                            let mut ttl = None;
                                            if let Some(st) = data.find("TTL:"){
                                                let st = st + 4;
                                                let ed = data[st..].find("\n").unwrap();
                                                ttl = Some(data[st..ed].parse::<usize>().unwrap());
                                            }

                                            self.cacher.insert(key, value, ttl).await;
                                        },
                                        "delete" => {
                                            let st = data.find("KEY:").unwrap() + 4;
                                            let ed = data[st..].find("\n").unwrap();
                                            let key = data[st..ed].to_string();
                                            self.cacher.delete(key).await;
                                        },
                                        "contains" => {
                                            let st = data.find("KEY:").unwrap() + 4;
                                            let ed = data[st..].find("\n").unwrap();
                                            let key = data[st..ed].to_string();
                                            self.cacher.contains(key).await;
                                        },
                                        "get" => {
                                            let st = data.find("KEY:").unwrap() + 4;
                                            let ed = data[st..].find("\n").unwrap();
                                            let key = data[st..ed].to_string();
                                            self.cacher.get(key).await;
                                        },
                                        "update" => {
                                            let st = data.find("KEY:").unwrap() + 4;
                                            let ed = data[st..].find("\n").unwrap();
                                            let key = data[st..ed].to_string();

                                            let mut value = None;
                                            if let Some(st) = data.find("VALUE:"){
                                                let st = st + 6;
                                                let ed = data[st..].find("\n").unwrap();
                                                value = Some(data[st..ed].to_string());
                                            }

                                            let mut ttl = None;
                                            if let Some(st) = data.find("TTL:"){
                                                let st = st + 4;
                                                let ed = data[st..].find("\n").unwrap();
                                                ttl = Some(data[st..ed].parse::<usize>().unwrap());
                                            }

                                            self.cacher.update(key, value, ttl).await;
                                        },
                                        _ => {
                                            if fmt.is_empty(){
                                                self.db.exec(&data).unwrap();
                                            }else{
                                                self.db.import_with_format(&format!("T{}",*TABLE_NAME.read().unwrap()), &fmt, &data).unwrap();
                                                *TABLE_NAME.write().unwrap() += 1;
                                            }                                               
                                        }
                                    }
                                }                                   
                            }
                        },
                        e_Storage::Db => {
                            for (k,v) in p.1{
                                let mut data = "".to_string();
                                let mut fmt = "".to_string();
                                match &k[..]{
                                    "DATA" => {
                                        data = v;
                                    },
                                    "FORMAT" => {
                                        fmt = v;
                                    }
                                    _ => {}
                                }
                                if fmt.is_empty(){
                                    self.db.exec(&data).unwrap();
                                }else{
                                    self.db.import_with_format(&format!("T{}",*TABLE_NAME.read().unwrap()), &fmt, &data).unwrap();
                                    *TABLE_NAME.write().unwrap() += 1;
                                }   
                            }
                        },
                        e_Storage::Lake => {
                            for (k,v) in p.1{
                                let mut data = "".to_string();
                                let mut cmnd = Vec::new();
                                match &k[..]{
                                    "DATA" => {
                                        data = v;
                                    },
                                    _ => {
                                        cmnd.push(v);
                                    }
                                }
                                for i in cmnd{
                                    match &(i.to_uppercase())[..]{
                                        "ADD" | "INSERT" => {self.lake.add(&data);},
                                        "DIRTY?" | "IS_DIRTY" => {self.lake.is_dirty(data.parse().unwrap());},
                                        "IS_ID?" | "IS_ID_STORRED" => {self.lake.is_id_storred(data.parse().unwrap());},
                                        "IS_DATA?" | "IS_DATA_STORRED" => {self.lake.is_data_storred(data.clone());},
                                        "DATA_TO_ID" => {self.lake.data_to_id(data.clone());},
                                        "ID_TO_DATA" => {self.lake.id_to_data(data.parse().unwrap());},
                                        "DATA_LEN" | "GET_DATA_LEN" => {self.lake.get_data_len(data.parse().unwrap());},
                                        "BLOB_COUNT" | "N_BLOBS" | "GET_BLOB_COUNT" => {self.lake.get_blob_count(data.parse().unwrap());},
                                        "RETRIEVE" | "GET" => {self.lake.retrive(data.parse().unwrap());},
                                        "DELETE" | "REMOVE" => {self.lake.delete(data.parse().unwrap());}
                                        _ => {},
                                    }
                                }                                
                            }
                        }
                    }                    
                }
            }
 


        }

        pub async fn finalise(&mut self){
            while let Some((p,v)) = self.task_q.pop(){
                if let Some((data,rule)) = self.data_map.get(&v){
                    if let Some(task_cr) = self.task_cr.as_mut(){
                        self.exec_q.push_back(task_cr.ingest(v,rule.clone()));
                    }
                }
            }

            while let Some(z) = self.exec_q.pop_front(){
                if let Ok(p) = z{
                    match p.0{
                        e_Storage::Cache => {
                            for (k,v) in p.1{
                                let mut data = "".to_string();
                                let mut fmt = "".to_string();
                                let layer;
                                let mut cmnd = Vec::new();
                                match &k[..]{
                                    "CACHE_LAYER" => {
                                        layer = CacheLayer::from(v);
                                    },
                                    "DATA" => {
                                        data = v;
                                    },
                                    "FORMAT" => {
                                        fmt = v;
                                    },
                                    _ => {
                                        cmnd.push(v);
                                    }
                                }

                                for cmnd_str in cmnd{
                                    match &cmnd_str[..]{
                                        "insert" => {
                                            let st = data.find("KEY:").unwrap() + 4;
                                            let ed = data[st..].find("\n").unwrap();
                                            let key = data[st..ed].to_string();

                                            let st = data.find("VALUE:").unwrap() + 6;
                                            let ed = data[st..].find("\n").unwrap();
                                            let value = data[st..ed].to_string();

                                            let mut ttl = None;
                                            if let Some(st) = data.find("TTL:"){
                                                let st = st + 4;
                                                let ed = data[st..].find("\n").unwrap();
                                                ttl = Some(data[st..ed].parse::<usize>().unwrap());
                                            }

                                            self.cacher.insert(key, value, ttl).await;
                                        },
                                        "delete" => {
                                            let st = data.find("KEY:").unwrap() + 4;
                                            let ed = data[st..].find("\n").unwrap();
                                            let key = data[st..ed].to_string();
                                            self.cacher.delete(key).await;
                                        },
                                        "contains" => {
                                            let st = data.find("KEY:").unwrap() + 4;
                                            let ed = data[st..].find("\n").unwrap();
                                            let key = data[st..ed].to_string();
                                            self.cacher.contains(key).await;
                                        },
                                        "get" => {
                                            let st = data.find("KEY:").unwrap() + 4;
                                            let ed = data[st..].find("\n").unwrap();
                                            let key = data[st..ed].to_string();
                                            self.cacher.get(key).await;
                                        },
                                        "update" => {
                                            let st = data.find("KEY:").unwrap() + 4;
                                            let ed = data[st..].find("\n").unwrap();
                                            let key = data[st..ed].to_string();

                                            let mut value = None;
                                            if let Some(st) = data.find("VALUE:"){
                                                let st = st + 6;
                                                let ed = data[st..].find("\n").unwrap();
                                                value = Some(data[st..ed].to_string());
                                            }

                                            let mut ttl = None;
                                            if let Some(st) = data.find("TTL:"){
                                                let st = st + 4;
                                                let ed = data[st..].find("\n").unwrap();
                                                ttl = Some(data[st..ed].parse::<usize>().unwrap());
                                            }

                                            self.cacher.update(key, value, ttl).await;
                                        },
                                        _ => {
                                            if fmt.is_empty(){
                                                self.db.exec(&data).unwrap();
                                            }else{
                                                self.db.import_with_format(&format!("T{}",*TABLE_NAME.read().unwrap()), &fmt, &data).unwrap();
                                                *TABLE_NAME.write().unwrap() += 1;
                                            }                                               
                                        }
                                    }
                                }                                   
                            }
                        },
                        e_Storage::Db => {
                            for (k,v) in p.1{
                                let mut data = "".to_string();
                                let mut fmt = "".to_string();
                                match &k[..]{
                                    "DATA" => {
                                        data = v;
                                    },
                                    "FORMAT" => {
                                        fmt = v;
                                    }
                                    _ => {}
                                }
                                if fmt.is_empty(){
                                    self.db.exec(&data).unwrap();
                                }else{
                                    self.db.import_with_format(&format!("T{}",*TABLE_NAME.read().unwrap()), &fmt, &data).unwrap();
                                    *TABLE_NAME.write().unwrap() += 1;
                                }   
                            }
                        },
                        e_Storage::Lake => {
                            for (k,v) in p.1{
                                let mut data = "".to_string();
                                let mut cmnd = Vec::new();
                                match &k[..]{
                                    "DATA" => {
                                        data = v;
                                    },
                                    _ => {
                                        cmnd.push(v);
                                    }
                                }
                                for i in cmnd{
                                    match &(i.to_uppercase())[..]{
                                        "ADD" | "INSERT" => {self.lake.add(&data);},
                                        "DIRTY?" | "IS_DIRTY" => {self.lake.is_dirty(data.parse().unwrap());},
                                        "IS_ID?" | "IS_ID_STORRED" => {self.lake.is_id_storred(data.parse().unwrap());},
                                        "IS_DATA?" | "IS_DATA_STORRED" => {self.lake.is_data_storred(data.clone());},
                                        "DATA_TO_ID" => {self.lake.data_to_id(data.clone());},
                                        "ID_TO_DATA" => {self.lake.id_to_data(data.parse().unwrap());},
                                        "DATA_LEN" | "GET_DATA_LEN" => {self.lake.get_data_len(data.parse().unwrap());},
                                        "BLOB_COUNT" | "N_BLOBS" | "GET_BLOB_COUNT" => {self.lake.get_blob_count(data.parse().unwrap());},
                                        "RETRIEVE" | "GET" => {self.lake.retrive(data.parse().unwrap());},
                                        "DELETE" | "REMOVE" => {self.lake.delete(data.parse().unwrap());}
                                        _ => {},
                                    }
                                }                                
                            }
                        }
                    }                    
                }
            }
        }


    }








fn _main() {
    let mut clargs = CLI::new();
    clargs.Parse_Args();
    if clargs.dbg{
        println!("{clargs:?}");
    }

    println!("Hello, world!");
}
