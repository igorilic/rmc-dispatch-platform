//! The seeded world: plants, trucks, sites and mixes from `deploy/seed/rhine-neckar.json`.

use domain::{MixDesign, Plant, Site, Truck};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct World {
    pub plants: Vec<Plant>,
    pub trucks: Vec<Truck>,
    pub sites: Vec<SeedSite>,
    pub mixes: Vec<MixDesign>,
}

/// A site plus the customer's own reference for it. Customers only ever send their reference.
#[derive(Debug, Clone, Deserialize)]
pub struct SeedSite {
    #[serde(flatten)]
    pub site: Site,
    pub customer_ref: String,
}

impl World {
    /// The seed is compiled in, so the binary needs no data files at runtime.
    pub fn seed() -> serde_json::Result<World> {
        serde_json::from_str(include_str!("../../../deploy/seed/rhine-neckar.json"))
    }

    #[cfg(test)]
    pub fn plant(&self, id: &domain::PlantId) -> Option<&Plant> {
        self.plants.iter().find(|p| &p.id == id)
    }

    pub fn sites_of(&self, customer: &str) -> Vec<&SeedSite> {
        self.sites
            .iter()
            .filter(|s| s.site.customer.0 == customer)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    const CUSTOMERS: [&str; 3] = ["bauwerk-ag", "rheinbau-gmbh", "neckar-infra"];

    fn all_unique<'a>(ids: impl Iterator<Item = &'a str>) -> bool {
        let ids: Vec<_> = ids.collect();
        ids.len() == ids.iter().collect::<HashSet<_>>().len()
    }

    #[test]
    fn seed_is_consistent() {
        let world = World::seed().unwrap();
        let counts = (
            world.plants.len(),
            world.trucks.len(),
            world.sites.len(),
            world.mixes.len(),
        );
        assert_eq!(counts, (3, 25, 40, 4));

        assert!(all_unique(world.plants.iter().map(|p| p.id.0.as_str())));
        assert!(all_unique(world.trucks.iter().map(|t| t.id.0.as_str())));
        assert!(all_unique(world.sites.iter().map(|s| s.site.id.0.as_str())));
        assert!(all_unique(
            world.sites.iter().map(|s| s.customer_ref.as_str())
        ));

        for truck in &world.trucks {
            assert!(world.plant(&truck.home_plant).is_some(), "{truck:?}");
        }
        for customer in CUSTOMERS {
            assert!(
                !world.sites_of(customer).is_empty(),
                "{customer} has no sites"
            );
        }
        let site_customers: usize = CUSTOMERS.iter().map(|c| world.sites_of(c).len()).sum();
        assert_eq!(
            site_customers,
            world.sites.len(),
            "a site belongs to an unknown customer"
        );
    }
}
