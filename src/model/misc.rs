pub mod misc{
    use std::{collections::HashMap, error::Error, fmt::Display};

use mysql::PooledConn;
use uuid::Uuid;

use regex::Regex;



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
    fmt: Option<String> //  "{row_name:}[sep.*]"
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
    batch_size: usize,
    conn: PooledConn,
    tasks: HashMap<Uuid,Comm> 
}

impl TaskCruncher for PrioScheduler{

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
            
            data.insert("DATA".to_string(), command.data.clone());
            
            if let Some(fmt) = &command.fmt {
                data.insert("FORMAT".to_string(), fmt.clone());
            }
            
            let storage = match command.cache {
                Some(_) => e_Storage::Cache,
                None => e_Storage::Db,
            };
            
            Ok((storage, data))
        }else{
            Err(Box::new(TaskError{info: "No Task Found".to_string()}))
        }
    }

    fn ingest_all(&self,tids: Vec<Uuid>, rule_map: HashMap<String,String>) -> Vec<TaskResult> {
        let mut task_pairs: Vec<_> = tids.iter()
            .filter_map(|tid| self.tasks.get(tid).map(|comm| (*tid, comm.prio)))
            .collect();
        
        task_pairs.sort_by(|a, b| b.1.cmp(&a.1));
        
        task_pairs.into_iter()
            .map(|(tid, _)| self.ingest(tid, rule_map.clone()))
            .collect()
    }

    fn ingest_batch(&self,tids: Vec<Uuid>, rule_map: HashMap<String,String>) -> Vec<TaskResult> {
        let mut task_pairs: Vec<_> = tids.iter()
            .filter_map(|tid| self.tasks.get(tid).map(|comm| (*tid, comm.prio)))
            .collect();
        
        task_pairs.sort_by(|a, b| b.1.cmp(&a.1));
        
        task_pairs.into_iter()
            .map(|(tid, _)| self.ingest(tid, rule_map.clone()))
            .collect()
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

pub fn generate_create_table(
    table_name: &str,
    tokens: &[Token],
) -> Result<String, String> {

    let mut columns = Vec::new();

    for token in tokens {
        if let Token::Column { name, sql_type } = token {

            // Production code should whitelist types.
            columns.push(format!(
                "\"{}\" {}",
                name,
                sql_type
            ));
        }
    }

    if columns.is_empty() {
        return Err("No columns defined".into());
    }

    Ok(format!(
        "CREATE TABLE IF NOT EXISTS \"{}\" ({})",
        table_name,
        columns.join(", ")
    ))
}


pub fn parse_format(fmt: &str) -> Result<Vec<Token>, String> {
    let token_re = Regex::new(
        r"\{(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)(?::(?P<type>[^}]+))?\}|\[(?P<sep>[^\]]*)\]"
    )
    .map_err(|e| e.to_string())?;

    let mut tokens = Vec::new();
    let mut last_end = 0;

    for caps in token_re.captures_iter(fmt) {
        let whole = caps.get(0).unwrap();

        // Reject unknown/unparsed characters between tokens
        if whole.start() != last_end {
            return Err(format!(
                "Invalid format syntax near: {}",
                &fmt[last_end..whole.start()]
            ));
        }

        if let Some(name) = caps.name("name") {
            let sql_type = caps
                .name("type")
                .map(|m| m.as_str().to_string())
                .unwrap_or_else(|| "TEXT".to_string());

            tokens.push(Token::Column {
                name: name.as_str().to_string(),
                sql_type,
            });
        } else if let Some(sep) = caps.name("sep") {
            tokens.push(Token::Separator(
                sep.as_str().to_string()
            ));
        }

        last_end = whole.end();
    }

    if last_end != fmt.len() {
        return Err(format!(
            "Invalid format syntax near: {}",
            &fmt[last_end..]
        ));
    }

    Ok(tokens)
}


pub fn build_extraction_regex(tokens: &[Token]) -> Result<Regex, String> {
    let mut pattern = String::from("^");

    for token in tokens {
        match token {
            Token::Column { .. } => {
                pattern.push_str("(.*?)");
            }

            Token::Separator(regex) => {
                pattern.push_str(regex);
            }
        }
    }

    pattern.push('$');

    Regex::new(&pattern)
        .map_err(|e| format!("Invalid generated regex: {e}"))
}

pub fn extract_row(
    regex: &Regex,
    tokens: &[Token],
    line: &str,
) -> Result<Vec<String>, String> {

    let captures = regex
        .captures(line)
        .ok_or_else(|| {
            format!("Line does not match format: {line}")
        })?;

    let mut values = Vec::new();
    let mut capture_index = 1;

    for token in tokens {
        if matches!(token, Token::Column { .. }) {
            let value = captures
                .get(capture_index)
                .map(|m| m.as_str().to_string())
                .unwrap_or_default();

            values.push(value);

            capture_index += 1;
        }
    }

    Ok(values)
}

}