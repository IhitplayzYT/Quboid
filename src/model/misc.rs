pub mod misc{
    use std::{collections::HashMap, error::Error, fmt::Display, hash::{DefaultHasher, Hash, Hasher}, ptr::hash};

use mysql::{Pool, PooledConn};
use serde::Serialize;
use uuid::Uuid;

use regex::Regex;

    #[derive(Debug,Clone,Copy,PartialEq, Eq, PartialOrd, Ord)]
    pub enum Priority{
        Low,
        Default,
        Medium,
        High
    }

    #[derive(Serialize)]
    pub struct Maintainer{
       pub name: String,
       pub email: String,
       pub branch: String,
       pub other: Option<HashMap<String,String>>
    }

    impl Default for Maintainer{

        fn default() -> Self {
            let mut other = HashMap::new();
            other.insert("version".to_string(), "1.0.0".to_string());
            Self { name: "UNKNOWN".to_string(), email: "".to_string(), branch: "main".to_string(), other:Some(other)}
        }

    }


    #[derive(Serialize)]
    pub struct MetaData{
       pub id: Uuid,
       pub gid: Uuid,
       pub ldbid: Uuid,
       pub maintainer: Maintainer,
       pub checksum: u64,
       pub name: String,
       pub desc: String,
       pub version: String,
       pub is_valid: bool,
       pub is_maintained: bool,
    }

    impl Default for MetaData{

        fn default() -> Self {
            let mut ret = Self { id: Uuid::new_v4(), gid: Uuid::new_v4(), ldbid: Uuid::new_v4(), checksum: 0, name: "QuBe".to_string(), desc: "A DataBricks type Object Storage".to_string(), version: "1.0.0".to_string(), is_valid: true, is_maintained: true ,maintainer: Maintainer::default()};
            let str = serde_json::to_string(&ret).unwrap();
            let mut hasher = DefaultHasher::new();
            str.hash(&mut hasher);
            let hsh = hasher.finish();
            ret.checksum = hsh;
            ret
        }

    }

    
pub enum e_Storage{
    Lake,
    Cache,
    Db
}

type Data = HashMap<String,String>;

pub type TaskResult = Result<(e_Storage,Data),Box<dyn Error>>;

pub trait TaskCruncher{
    fn ingest(&self,tid: Uuid, rule_map: HashMap<String,String>) -> TaskResult;
    fn ingest_all(&self,tids: Vec<Uuid>, rule_maps: HashMap<Uuid,HashMap<String,String>>) -> Vec<TaskResult>;
    fn ingest_batch(&self,tids: &mut Vec<Uuid>, rule_maps: HashMap<Uuid,HashMap<String,String>>) -> Vec<TaskResult>;
    fn insert(&mut self,id:Uuid,data: String,layer: Option<CacheLayer>,prio: Priority,fmt: Option<String>);
    fn any_pending(&self) -> bool;
    fn is_done(&self) -> bool;
}

pub enum CacheLayer{
    L1,
    L2,
    L3,
    Below 
}

pub struct Comm{
    data: String,
    cache: Option<CacheLayer>,
    prio:Priority,
    fmt: Option<String>
}

#[derive(Debug)]
struct TaskError{
    info: String
}

impl Display for TaskError{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f,"{}",self.info)
    }
}

impl Error for TaskError{
    fn description(&self) -> &str {
        &self.info
    }

    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }

    fn cause(&self) -> Option<&dyn Error> {
        None    
    }

}


pub struct PrioScheduler{
   pub batch_size: usize,
   pub conn: PooledConn,
   pub tasks: HashMap<Uuid,Comm>, 
   pub obj_storage_min_size: usize
}

impl PrioScheduler{
    pub fn new(batch_size:usize,conn:PooledConn,obj_storage_min_size:usize) -> Self{
        Self { batch_size, conn, tasks: HashMap::new(), obj_storage_min_size}
    }

    

}


impl From<String> for CacheLayer{

    fn from(value: String) -> Self {
        match &value[..]{
            "L1" => {CacheLayer::L1},
            "L2" => {CacheLayer::L2},
            "L3" => {CacheLayer::L3},
            "B4" => {CacheLayer::Below},
            _ => {CacheLayer::L1}
        }
    }

}

impl TaskCruncher for PrioScheduler{
    fn insert(&mut self,id:Uuid,data: String,layer: Option<CacheLayer>,prio: Priority,fmt: Option<String>){
        self.tasks.insert(id, Comm { data, cache:layer , prio, fmt });
    }

    fn ingest(&self,tid: Uuid, rule_map: HashMap<String,String>) -> TaskResult {
        let mut data = HashMap::new();
        if let Some(command) = self.tasks.get(&tid){
            if let Some(cl) = &command.cache{
                use crate::model::misc::misc::CacheLayer::*;
                data.insert("CACHE_LAYER".to_string(),match cl{
                    L1 => {"L1"},
                    L2 => {"L2"},
                    L3 => {"L3"},
                    Below => {"B4"},
                }.to_string());
            }
            let data_len = command.data.len();
            data.insert("DATA".to_string(), command.data.clone());
            
            if let Some(fmt) = &command.fmt {
                data.insert("FORMAT".to_string(), fmt.clone());
            }

            let storage = match command.cache {
                Some(_) => e_Storage::Cache,
                None => {
                    if let Some(_) = data.get("FORMAT"){
                        e_Storage::Db
                    }else{
                        if data_len >= self.obj_storage_min_size{
                            e_Storage::Lake
                        }else{
                            e_Storage::Db
                        }
                    }
                },
            };
            data.extend(&mut rule_map.into_iter());
            Ok((storage, data))
        }else{
            Err(Box::new(TaskError{info: "No Task Found".to_string()}))
        }
    }

    fn ingest_all(&self,tids: Vec<Uuid>, rule_maps: HashMap<Uuid,HashMap<String,String>>) -> Vec<TaskResult> {
        let mut task_pairs: Vec<_> = tids.iter().filter_map(|tid| self.tasks.get(tid).map(|comm| (*tid, comm.prio))).collect();
        task_pairs.sort_by(|a, b| b.1.cmp(&a.1));   
        task_pairs.into_iter().map(|(tid, _)| self.ingest(tid, rule_maps.get(&tid).unwrap_or(&HashMap::new()).clone())).collect()
    }

    fn ingest_batch(&self,tids: &mut Vec<Uuid>, rule_maps: HashMap<Uuid,HashMap<String,String>>) -> Vec<TaskResult> {
        let l = tids.len();
        let my_tids = tids[..self.batch_size.min(l)].to_vec(); // Takes first batch
        if l > self.batch_size{
            *tids = tids[self.batch_size..].to_vec(); // Removes the first batch
        }else{
            tids.clear(); // clears if less then or equal to batch size elems
        }

        let mut task_pairs: Vec<_> = tids.iter().filter_map(|tid| self.tasks.get(tid).map(|comm| (*tid, comm.prio))).collect();
        task_pairs.sort_by(|a, b| b.1.cmp(&a.1));
        task_pairs.into_iter().map(|(tid, _)| self.ingest(tid, rule_maps.get(&tid).unwrap_or(&HashMap::new()).clone())).collect()
    }

    fn any_pending(&self) -> bool {
        !self.tasks.is_empty()
    }

    fn is_done(&self) -> bool {
        self.tasks.is_empty()
    }

}



#[derive(Debug, Clone)]
pub enum Token {
    Column {
        name: String,
        sql_type: String,
    },
    Separator(String),
}

pub fn generate_create_table(table_name: &str,tokens: &[Token]) -> Result<String, String> {
    let mut columns = Vec::new();
    for token in tokens {
        if let Token::Column { name, sql_type } = token {
            columns.push(format!("\"{}\" {}",name,sql_type));
        }
    }
    if columns.is_empty() {
        return Err("No columns defined".into());
    }
    Ok(format!("CREATE TABLE IF NOT EXISTS \"{}\" ({})",table_name,columns.join(", ")))
}


pub fn parse_format(fmt: &str) -> Result<Vec<Token>, String> {
    let token_re = Regex::new(r"\{(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)(?::(?P<type>[^}]+))?\}|\[(?P<sep>[^\]]*)\]").map_err(|e| e.to_string())?;
    let mut tokens = Vec::new();
    let mut last_end = 0;
    for caps in token_re.captures_iter(fmt) {
        let whole = caps.get(0).unwrap();
        // Reject unknown/unparsed characters between tokens
        if whole.start() != last_end {
            return Err(format!("Invalid format syntax near: {}",&fmt[last_end..whole.start()]));
        }

        if let Some(name) = caps.name("name") {
            let sql_type = caps.name("type").map(|m| m.as_str().to_string()).unwrap_or_else(|| "TEXT".to_string());
            tokens.push(Token::Column {name: name.as_str().to_string(),sql_type});
        } else if let Some(sep) = caps.name("sep") {
            tokens.push(Token::Separator(sep.as_str().to_string()));
        }
        last_end = whole.end();
    }

    if last_end != fmt.len() {
        return Err(format!("Invalid format syntax near: {}",&fmt[last_end..]));
    }

    Ok(tokens)
}


pub fn build_extraction_regex(tokens: &[Token]) -> Result<Regex, String> {
    let mut pattern = String::from("^");
    for token in tokens {
        match token {
            Token::Column { .. } => { pattern.push_str("(.*?)");}
            Token::Separator(regex) => {pattern.push_str(regex);}
        }
    }
    pattern.push('$');
    Regex::new(&pattern).map_err(|e| format!("Invalid generated regex: {e}"))
}

pub fn extract_row(regex: &Regex,tokens: &[Token],line: &str) -> Result<Vec<String>, String> {
    let captures = regex.captures(line).ok_or_else(|| {format!("Line does not match format: {line}")})?;
    let mut values = Vec::new();
    let mut capture_index = 1;
    for token in tokens {
        if matches!(token, Token::Column { .. }) {
            let value = captures.get(capture_index).map(|m| m.as_str().to_string()).unwrap_or_default();
            values.push(value);
            capture_index += 1;
        }
    }
    Ok(values)
}

}