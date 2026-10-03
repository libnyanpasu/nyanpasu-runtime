mod dist;
mod error_kind;
mod feature;
mod kind;

pub use dist::{CoreDistribution, VariantTag};
pub use error_kind::*;
pub use feature::{
    clash::{Feature, FeatureSupport},
    *,
};
pub use kind::*;
