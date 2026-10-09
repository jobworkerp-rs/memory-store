//! Building blocks of the self-contained local SQLite migration
//! (`memories-db-migrate local apply` / `local restore`) and of release bundle
//! identity. Everything that touches SQLite files exists only in the SQLite
//! build; bundle identity is shared by both builds.

#[cfg(not(feature = "postgres"))]
pub mod attempt;
#[cfg(not(feature = "postgres"))]
pub mod backup;
pub mod bundle;
pub mod files;
pub mod output;
#[cfg(not(feature = "postgres"))]
pub mod preflight;
#[cfg(not(feature = "postgres"))]
pub mod resources;
#[cfg(not(feature = "postgres"))]
pub mod restore;
#[cfg(not(feature = "postgres"))]
pub mod target;
#[cfg(not(feature = "postgres"))]
pub mod writer;
