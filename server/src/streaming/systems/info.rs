use crate::streaming::systems::system::System;
use iggy::error::Error;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use tracing::info;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct SystemInfo {
    pub version: Version,
    pub migrations: Vec<Migration>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Version {
    pub version: String,
    pub hash: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Migration {
    pub id: u32,
    pub name: String,
    pub hash: String,
    pub applied_at: u64,
}

#[derive(Debug)]
pub struct SemanticVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl System {
    pub(crate) async fn load_version(&mut self) -> Result<(), Error> {
        info!("Loading system info...");
        let mut system_info = SystemInfo::default();
        if let Err(err) = self.storage.info.load(&mut system_info).await {
            match err {
                Error::ResourceNotFound(_) => {
                    info!("System info not found, creating...");
                    self.update_system_info(&mut system_info).await?;
                }
                _ => return Err(err),
            }
        }

        info!("Loaded {system_info}");
        let current_version = SemanticVersion::from_str(VERSION)?;
        let loaded_version = SemanticVersion::from_str(&system_info.version.version)?;
        if current_version.is_equal_to(&loaded_version) {
            info!("System version {current_version} is up to date.");
        } else if current_version.is_greater_than(&loaded_version) {
            info!("System version {current_version} is greater than {loaded_version}, checking the available migrations...");
            self.update_system_info(&mut system_info).await?;
        } else {
            info!("System version {current_version} is lower than {loaded_version}, possible downgrade.");
            self.update_system_info(&mut system_info).await?;
        }

        Ok(())
    }

    async fn update_system_info(&self, system_info: &mut SystemInfo) -> Result<(), Error> {
        system_info.update_version(VERSION);
        self.storage.info.save(system_info).await?;
        Ok(())
    }
}

impl SystemInfo {
    pub fn update_version(&mut self, version: &str) {
        self.version.version = version.to_string();
        let mut hasher = DefaultHasher::new();
        self.version.hash.hash(&mut hasher);
        self.version.hash = hasher.finish().to_string();
    }
}

impl Hash for SystemInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.version.version.hash(state);
        for migration in &self.migrations {
            migration.hash(state);
        }
    }
}

impl Hash for Migration {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "version: {}", self.version)
    }
}

impl Display for SystemInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "system info, {}", self.version)
    }
}

impl FromStr for SemanticVersion {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut version = s.split('.');
        let major = version.next().unwrap().parse::<u32>()?;
        let minor = version.next().unwrap().parse::<u32>()?;
        let patch = version.next().unwrap().parse::<u32>()?;
        Ok(SemanticVersion {
            major,
            minor,
            patch,
        })
    }
}

impl SemanticVersion {
    pub fn is_equal_to(&self, other: &SemanticVersion) -> bool {
        self.major == other.major && self.minor == other.minor && self.patch == other.patch
    }

    pub fn is_greater_than(&self, other: &SemanticVersion) -> bool {
        if self.major > other.major {
            return true;
        }
        if self.major < other.major {
            return false;
        }

        if self.minor > other.minor {
            return true;
        }
        if self.minor < other.minor {
            return false;
        }

        if self.patch > other.patch {
            return true;
        }
        if self.patch < other.patch {
            return false;
        }

        false
    }
}

impl Display for SemanticVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{major}.{minor}.{patch}",
            major = self.major,
            minor = self.minor,
            patch = self.patch
        )
    }
}

mod tests {
    #[test]
    fn should_load_the_expected_version_from_package_definition() {
        use super::VERSION;

        const CARGO_TOML_VERSION: &str = env!("CARGO_PKG_VERSION");
        assert_eq!(VERSION, CARGO_TOML_VERSION);
    }
}
