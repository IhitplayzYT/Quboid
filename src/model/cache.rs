pub mod cache{
    use async_trait::async_trait;
use reqwest::Client;
use serde::Serialize;


   #[async_trait]
pub trait CacheConnector: Send + Sync{
   async fn insert(&self,k:String,v:String,ttl:Option<usize>);
   async fn delete(&self,k:String);
   async fn contains(&self,k:String) -> bool;
   async fn get(&self,k:String) -> Option<String>;
   async fn update(&self,k:String,v:Option<String>,ttl:Option<usize>);
}


#[derive(Debug)]
pub struct Rustis_Connector{
    base_url: String,
    client: Client
}


impl Rustis_Connector {
    pub fn new(ip: impl AsRef<str>, port: u16) -> Self {
        let ip = ip.as_ref();
        let base_url = if ip.starts_with("http://") || ip.starts_with("https://") {
            format!("{ip}:{port}")
        } else {
            format!("http://{ip}:{port}")
        };

        Self {base_url,client: Client::new()}
    }

    fn url(&self, route: &str) -> String {
        format!("{}{}", self.base_url, route)
    }
}

#[async_trait]

impl CacheConnector for Rustis_Connector {
    async fn insert(&self,k: String,v: String,ttl: Option<usize>) {
        let route = format!("/item/{}/{}", k, v);
        let url = self.url(&route);
        let request = self.client.post(url);
        let request = match ttl {
            Some(ttl) => request.query(&[("ttl", ttl)]),
            None => request,
        };
        let _ = request.send().await;
    }

    async fn delete(&self, k: String) {
        let route = format!("/item/{}", k);
        let url = self.url(&route);
        let _ = self.client.delete(url).send().await;
    }

    async fn contains(&self, k: String) -> bool {
        let route = format!("/key/{}", k);
        let url = self.url(&route);
        match self.client.get(url).send().await {
            Ok(response) => {
                match response.json::<bool>().await {
                    Ok(result) => result,
                    Err(_) => false,
                }
            },
            Err(_) => false,
        }
    }

    async fn get(&self,k: String) -> Option<String> {
        let route = format!("/item/{}", k);
        let url = self.url(&route);
        match self.client.get(url).send().await {
            Ok(response) => {response.json::<Option<String>>().await.ok().flatten()},
            Err(_) => None,
        }
    }

    async fn update(&self,k: String,v: Option<String>,ttl: Option<usize>) {
        let route = format!("/item/{}", k);
        let url = self.url(&route);
        let query = UpdateQuery {value: v,ttl};

        let _ = self.client.put(url).query(&query).send().await;
    }
}



    #[derive(Serialize)]
    pub struct UpdateQuery {
        pub value: Option<String>,
        pub ttl: Option<usize>,
    }



}