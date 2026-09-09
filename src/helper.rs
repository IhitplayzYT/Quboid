pub mod Helper{
    use std::process::exit;



    const DBG_STR: &str = "Quboid - Multi-tier Data Management System

USAGE:
    quboid [OPTIONS]

OPTIONS:
    -d, --debug          Enable debug mode
    -h, --help           Display this help message

DESCRIPTION:
    Quboid is a high-performance data management system that provides intelligent
    storage routing across cache, database, and object storage layers.

EXAMPLES:
    quboid -d             Run with debug mode enabled
    quboid -h             Show this help message

For more information, visit the project repository.";
    const OK:i32 = 0;
    const ERR:i32 = -1;


    #[derive(Debug,Clone)]
    pub struct CLI{
        pub dbg: bool
    }


    pub fn Help(){
        println!("{DBG_STR}");
        exit(OK);
    }


    impl CLI{
        pub fn new() -> Self{
            Self {dbg: false  }
        }

        pub fn Parse_Args(&mut self){
            let args: Vec<String> = std::env::args().skip(1).collect();
           for i in &args{
                if i == "-d" || i == "--debug" || i == " --DEBUG" || i == "-D"{
                    self.dbg = true;
                } else if i == "-h" || i == "--help" || i == " --HELP" || i == "-H"{
                    Help();
                } else{
                    Help();
                }
           } 


        }



    }


    





}