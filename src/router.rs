pub mod router{
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

use axum::{Json, extract::{Query, State}, response::IntoResponse, http::StatusCode};
use serde::{Deserialize, Serialize};

use crate::{Qube, model::misc::misc::Priority};

    #[derive(Debug,Serialize,Deserialize)]
    pub struct SubmitTask{
        data: String,
        prio: Option<Priority>,
        rule: Option<HashMap<String,String>>,
    }

    pub const FINALISE: &str = "/finalise";
    pub const BATCH_FINALISE: &str = "/finalise/batch";
    pub const SUBMIT : &str = "/submit";
    pub const SUBMIT_ALL : &str = "/submit/all";


    pub async fn handle_finalise(State(qube): State<Arc<RwLock<Qube>>>,Query(is_batch): Query<bool>) -> impl IntoResponse{
        let mut qube = qube.write().await;
        if is_batch{
            qube.batch_finalise().await;
        }else{
            qube.finalise().await;
        }
        StatusCode::OK
    }

    pub async fn handle_batch_finalise(State(qube): State<Arc<RwLock<Qube>>>) -> impl IntoResponse{
        let mut qube = qube.write().await;
        qube.batch_finalise().await;
        StatusCode::OK
    }

    pub async fn handle_submit(State(qube): State<Arc<RwLock<Qube>>>,Json(task): Json<SubmitTask>) -> impl IntoResponse {
        let fmt = if let Some(mp) = &task.rule{
            if let Some(k) = mp.get("FMT") {Some(k.to_string())}
            else{None}
        }else{None};
        let mut qube = qube.write().await;
        qube.submit(task.data,task.prio,task.rule,fmt);
        StatusCode::OK
    }

    pub async fn handle_submit_all(State(qube): State<Arc<RwLock<Qube>>>,Json(tasks): Json<Vec<SubmitTask>>) -> impl IntoResponse {
        let mut ip = vec![];
        for task in tasks{
            let fmt = if let Some(mp) = &task.rule{
                if let Some(k) = mp.get("FMT") {Some(k.to_string())}
                else{None}
            }else{None};
            ip.push((task.data,task.prio,task.rule,fmt));
        }
        let mut qube = qube.write().await;
        qube.submit_all(ip);
        StatusCode::OK
    }

}