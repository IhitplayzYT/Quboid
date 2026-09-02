pub mod ob_store{
    use std::{collections::HashMap, fs, hash::{DefaultHasher, Hash, Hasher}, path::{Path, PathBuf}};
    use uuid::Uuid;
    use rand::{Rng, rngs::ThreadRng};


    pub struct DataLake{
        lake: HashMap<u64,(u64,PathBuf)>, // Salt // Pathbuf to chunk/blob tree
        hasher: Box<dyn Hasher>,
        root: PathBuf,
        rng: Box<dyn Rng>,
        blob_sz: usize
    }

    impl DataLake{
        pub fn new(blob_sz:usize,path: Option<PathBuf>) -> Self{
            Self { lake: HashMap::new(), hasher:Box::new(DefaultHasher::new()),root: path.unwrap_or(PathBuf::from("OB_STORE")),rng: Box::new(ThreadRng::default()),blob_sz}
        }

        pub fn add(&mut self,data: String) -> u64{
            let l = data.len();
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

            if l <= self.blob_sz{
                std::fs::write(fpath,data[..data.rfind("\n").unwrap()].to_string()).unwrap();
            }else{
                let n = l as f32 / self.blob_sz as f32;
                let mut m = n as usize;
                if n - m as f32 > 0.0 {
                    m += 1;
                }
                let root = fpath.parent().unwrap();
                std::fs::write(&fpath,format!("HEADER CONFIG{l}\n")).unwrap();
                let mut buff = "".to_string();
                for (i,d_blob) in data.bytes().collect::<Vec<u8>>().chunks(self.blob_sz).enumerate(){
                    let blob_l = d_blob.len();
                    let ds = String::from_utf8(d_blob.to_vec()).unwrap();
                    let hasher_str = format!("{i}{ds}{i}");
                    hasher_str.hash(&mut self.hasher);
                    let hash = self.hasher.finish();
                    let hsh_str = format!("{hash:x}");
                    buff += &format!("{i}:{hsh_str}\n");
                    std::fs::write(root.join(hsh_str),format!("{blob_l}\n{}",String::from_utf8(d_blob.to_vec()).unwrap())).unwrap()
                }
                std::fs::write(&fpath, std::fs::read_to_string(&fpath).unwrap() + &buff).unwrap();
            }
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

        pub fn is_id_storred(&self,id:u64) -> bool{
            if let Some(_) = self.lake.get(&id){
                true
            }else{
                false
            }
        }

        pub fn is_data_storred(&mut self,data: String) -> bool{
            for (k,v) in self.lake.iter(){
                let hsh_str = format!("{data}\n{}",v.0);
                hsh_str.hash(&mut self.hasher);
                let hsh = format!("{:x}",self.hasher.finish());
                let pth = &v.1;
                let full_hash = "".to_string() + v.1.parent().unwrap().file_name().unwrap().to_str().unwrap() + v.1.file_name().unwrap().to_str().unwrap();
                if full_hash == hsh{
                    return true;
                }
            }
            false
        }

        pub fn data_to_id(&mut self,data: String) -> Option<u64>{
            for (k,v) in self.lake.iter(){
                let hsh_str = format!("{data}\n{}",v.0);
                hsh_str.hash(&mut self.hasher);
                let hsh = format!("{:x}",self.hasher.finish());
                let pth = &v.1;
                let full_hash = "".to_string() + v.1.parent().unwrap().file_name().unwrap().to_str().unwrap() + v.1.file_name().unwrap().to_str().unwrap();
                if full_hash == hsh{
                    return Some(*k);
                }
            }
            None
        }

        pub fn id_to_data(&mut self,id:u64) -> String{
            self.retrive(id)
        }

        pub fn get_data_len(&self,id:u64) -> usize{
            if let Some(elem) = self.lake.get(&id){
                let s = std::fs::read_to_string(&elem.1).unwrap();
                if s.starts_with("HEADER CONFIG"){
                    return s[13..s.find("\n").unwrap()].parse::<usize>().expect("Length of full data");
                }else{
                    return s.len();
                }
            }
            0
        }

        pub fn get_blob_count(&self,id:u64) -> usize{
            if let Some(elem) = self.lake.get(&id){
                let s = std::fs::read_to_string(&elem.1).unwrap();
                if s.starts_with("HEADER CONFIG"){
                    return s.chars().filter(|x| x == &'\n').count() - 1;
                }else{
                    return 1;
                }
            }
            0
        }

        pub fn retrive(&mut self,id: u64) -> String{
            if let Some(elem) = self.lake.get(&id){
                let str = fs::read_to_string(&elem.1).unwrap();
                let root = elem.1.parent().unwrap();
                if str.starts_with("HEADER CONFIG"){
                    let l = str[13..str.find("\n").unwrap()].parse::<usize>().expect("Length of full data");
                    let mut fmap = HashMap::new();
                    let mut buff = String::new();
                    for line in str.trim().split("\n"){
                        let m = line.split(":").collect::<Vec<&str>>();
                        fmap.insert(m[0].parse::<usize>().unwrap(),m[1].trim());
                    }

                    for (i,(k,v)) in fmap.into_iter().enumerate(){
                        if i != k{
                            panic!("Corrupt Config for {id}");
                        }
                        let file =  &std::fs::read_to_string(root.join(v)).unwrap();
                        let idx = file.find("\n").unwrap();
                        let content = &file[idx+1..];
                        let f_l = file[..idx].parse::<usize>().unwrap();
                        if content.len() != f_l{
                            panic!("Corrupt blob: {k}")
                        }
                        buff += content;  
                    }
                    if buff.len() != l{
                        panic!("Corrupt data built")
                    }
                    buff
                }else{
                    str
                }
            }else{
                "".to_string()
            }
        }

        pub fn delete(&mut self,id: u64){
            if let Some((slt,pth)) = self.lake.get(&id){
                std::fs::remove_dir_all(pth.parent().unwrap()).unwrap();
                let name = pth.parent().unwrap().file_name().unwrap();
                std::fs::remove_dir(name).unwrap();
            }
            self.lake.remove_entry(&id);
        }
    }



}