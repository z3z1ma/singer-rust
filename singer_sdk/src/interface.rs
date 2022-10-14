//! Contains Singer CLI implementation
use clap::{self, Parser};
use log;
use serde_json::{from_reader as json_load, Value};

use std::{
    error::Error,
    fmt::{Display, Formatter},
    fs::File,
    io::{stdin, BufRead, BufReader},
    path::PathBuf,
    str::FromStr,
};

pub trait ParsesSingerArgs {
    fn parse_args() -> Self;
}

#[derive(Debug, Clone)]
pub enum AboutFormat {
    Json,
    Markdown,
}

impl Display for AboutFormat {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AboutFormat::Json => write!(formatter, "json"),
            AboutFormat::Markdown => write!(formatter, "markdown"),
        }
    }
}

#[derive(Debug)]
pub struct AboutFormatError;

impl Display for AboutFormatError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?}", self)
    }
}

impl Error for AboutFormatError {
    fn description(&self) -> &str {
        "Invalid format option. Must be one of `json` or `markdown`."
    }
}

impl FromStr for AboutFormat {
    type Err = AboutFormatError;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        match string {
            "json" => Ok(Self::Json),
            "markdown" => Ok(Self::Markdown),
            _ => Err(AboutFormatError),
        }
    }
}

/* Target CLI Spec
Options:
  --input FILENAME          A path to read messages from instead of from
                          standard in.
  --config TEXT             Configuration file location or 'ENV' to use
                          environment variables.
  --format [json|markdown]  Specify output style for --about
  --about                   Display package metadata and settings.
  --version                 Display the package version.
  --help                    Show this message and exit.
*/

/// This struct defines the Singer Target CLI interface
#[derive(Parser, Debug)]
#[clap(author="z3z1ma", version, about="Singer target built with the Singer Rust SDK", long_about = None)]
pub struct SingerTarget {
    /// A path to read messages from instead of from standard in.
    #[clap(long, value_parser)]
    pub input: Option<PathBuf>,
    /// Configuration file location or 'ENV' to use environment variables.
    #[clap(long, value_parser)]
    pub config: PathBuf,
    /// Specify output style for --about
    #[clap(long, value_parser)]
    pub format: Option<AboutFormat>,
    /// Display package metadata and settings.
    #[clap(long, value_parser, default_value_t = false)]
    pub about: bool,
}

impl ParsesSingerArgs for SingerTarget {
    /// Parse CLI arguments passed to the program
    fn parse_args() -> Self {
        SingerTarget::parse()
    }
}

/* Tap CLI Spec
Options:
  --state PATH              Use a bookmarks file for incremental replication.
  --catalog PATH            Use a Singer catalog file with the tap.
  --test TEXT               Use --test to sync a single record for each
                            stream. Use --test=schema to test schema output
                            without syncing records.
  --discover                Run the tap in discovery mode.
  --config TEXT             Configuration file location or 'ENV' to use
                            environment variables.
  --format [json|markdown]  Specify output style for --about
  --about                   Display package metadata and settings.
  --version                 Display the package version.
  --help                    Show this message and exit.
*/

/// This struct defines the Singer Target CLI interface
#[derive(Parser, Debug)]
#[clap(author="z3z1ma", version, about="Singer tap built with the Singer Rust SDK", long_about = None)]
pub struct SingerTap {
    /// Use a bookmarks file for incremental replication.
    #[clap(long, value_parser)]
    pub state: Option<PathBuf>,
    /// Use a Singer catalog file with the tap.
    #[clap(long, value_parser)]
    pub catalog: Option<PathBuf>,
    /// Use --test to sync a single record for each stream. Use --test=schema
    /// to test schema output without syncing records.
    #[clap(long, value_parser)]
    pub test: Option<String>,
    /// Run the tap in discovery mode.
    #[clap(long, value_parser, default_value_t = false, conflicts_with = "about")]
    pub discover: bool,
    /// Configuration file location or 'ENV' to use environment variables.
    #[clap(long, value_parser)]
    pub config: PathBuf,
    /// Display package metadata and settings.
    #[clap(long, value_parser, default_value_t = false)]
    pub about: bool,
}

impl ParsesSingerArgs for SingerTap {
    /// Parse CLI arguments passed to the program
    fn parse_args() -> Self {
        SingerTap::parse()
    }
}

#[derive(Debug)]
pub enum JsonFiles {
    Config,
    Catalog,
    State,
}

pub fn parse_cli_args<T: ParsesSingerArgs>() -> T
where
    T: std::fmt::Debug,
{
    log::debug!("Parsing CLI arguments");
    let args = T::parse_args();
    log::debug!("Parsed, got {:?}", &args);
    args
}

pub fn parse_json_file(path: PathBuf, ftype: JsonFiles) -> Result<Value, std::io::Error> {
    log::debug!("Checking if {ftype:?} file exists");
    if !path.exists() || !path.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Invalid {ftype:?} file passed into --{ftype:?}"),
        ));
    }
    let fh = File::open(path)?;
    let config: Value = json_load(fh)?;
    log::debug!("{}", config);
    Ok(config)
}

pub fn parse_singer_input(input: Option<File>) -> Box<dyn BufRead> {
    match input {
        None => Box::new(stdin().lock()),
        Some(fobj) => Box::new(BufReader::new(fobj)),
    }
}
