pub mod ob_store{
    use std::{collections::HashMap, hash::{DefaultHasher, Hash, Hasher}, path::{Path, PathBuf}};
    use uuid::Uuid;
    use rand::{Rng, rngs::ThreadRng};


    pub struct DataLake{
        lake: HashMap<u64,(u64,PathBuf)>, // Dirty bool // Pathbuf to chunk/blob tree
        hasher: Box<dyn Hasher>,
        root: PathBuf,
        rng: Box<dyn Rng>
    }

    impl DataLake{
        pub fn new() -> Self{
            Self { lake: HashMap::new(), hasher:Box::new(DefaultHasher::new()),root: PathBuf::from("OB_STORE"),rng: Box::new(ThreadRng::default())}
        }

        pub fn add(&mut self,data: String) -> u64{
            let salt = self.rng.next_u64(); 
            let data = data + &format!("\n{salt}"); // Salt
            data.hash(&mut self.hasher);
            let hash = self.hasher.finish();
            let mut pth = format!("{hash:x}");
            let fname =  pth.split_off(3);
            let dir = pth; 
            std::fs::create_dir(dir.clone()).unwrap();
            let fpath = self.root.join(dir.clone()).join(fname);
            self.lake.insert(hash, (salt,fpath.clone()));
            std::fs::write(self.root.join(dir).join("salt"),salt.to_string()).unwrap();
            std::fs::write(fpath,data[..data.rfind("\n").unwrap()].to_string()).unwrap();
            hash
        }


        pub fn is_dirty(&mut self,id: u64) -> bool{
            if let Some((slt,pth)) = self.lake.get(&id){
                let data = std::fs::read_to_string(pth).unwrap() + &format!("\n{slt}");
                data.hash(&mut self.hasher);
                return !(id == self.hasher.finish());
            }else{
                return true;
            }
        }

        pub fn delete(&mut self,id: u64){
            if let Some((slt,pth)) = self.lake.get(&id){
                std::fs::remove_dir_all(pth.parent().unwrap()).unwrap();
            }
            self.lake.remove_entry(&id);
        }

    }



}