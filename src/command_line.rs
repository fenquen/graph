use clap::Parser;

#[derive(Parser, Debug)]
#[command(author = "fenquen", version = "0.1.0", about = "graph", long_about = None)]
pub struct CommandLine {
    /// json file
    #[arg(short, long)]
    pub configFilePath: Option<String>,
}