pub mod ob_store{
    use std::{collections::HashMap, path::{Path, PathBuf}};

use uuid::Uuid;


    pub struct DataLake{
        lake: HashMap<Uuid,(bool,PathBuf)>, // Dirty bool // Pathbuf to chunk/blob tree


    }


}