pub mod cache{
    
pub trait CacheConnector{
    fn insert(&self,k:String,v:String);
    fn delete(&self,k:String);
    fn contains(&self,k:String) -> bool;
    fn get(&self,k:String) -> Option<String>;
    fn update(&self,k:String,v:String);
}





}