use {
    serde::Deserialize,
    std::{fs, path::Path},
};

#[derive(Debug, Deserialize)]
pub struct Config {
    //
}

impl Config {
    fn load_from_str(config: &str) -> anyhow::Result<Self> {
        serde_json::from_str(config).map_err(Into::into)
    }

    pub fn load_from_file<P: AsRef<Path>>(file: P) -> anyhow::Result<Self> {
        let config = fs::read_to_string(file)?;
        Self::load_from_str(&config)
    }
}
