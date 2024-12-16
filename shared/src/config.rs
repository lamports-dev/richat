use serde::{
    de::{self, Deserializer},
    Deserialize,
};

#[derive(Deserialize)]
#[serde(untagged)]
enum ValueIntStr<'a> {
    Int(usize),
    Str(&'a str),
}

pub fn deserialize_usize_str<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    match ValueIntStr::deserialize(deserializer)? {
        ValueIntStr::Int(value) => Ok(value),
        ValueIntStr::Str(value) => value
            .replace('_', "")
            .parse::<usize>()
            .map_err(de::Error::custom),
    }
}
