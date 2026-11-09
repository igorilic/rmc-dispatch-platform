//! The seeded world from `deploy/seed/rhine-neckar.json`. It holds our master data (customers and
//! their ship-tos, with a postal-code table for address-only sites) plus plants, trucks and mixes.

use domain::{Customer, MixDesign, Plant, PostalCodeGeocoder, Site, Truck};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct World {
    pub customers: Vec<Customer>,
    #[serde(rename = "postal_codes")]
    pub geocoder: PostalCodeGeocoder,
    pub plants: Vec<Plant>,
    pub trucks: Vec<Truck>,
    pub sites: Vec<Site>,
    pub mixes: Vec<MixDesign>,
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

    /// The ship-tos of the customer with this customer number.
    pub fn sites_of(&self, customer: &str) -> Vec<&Site> {
        self.sites
            .iter()
            .filter(|s| s.customer.0 == customer)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn all_unique<'a>(ids: impl Iterator<Item = &'a str>) -> bool {
        let ids: Vec<_> = ids.collect();
        ids.len() == ids.iter().collect::<HashSet<_>>().len()
    }

    #[test]
    fn seed_is_consistent() {
        let world = World::seed().unwrap();
        let counts = (
            world.customers.len(),
            world.plants.len(),
            world.trucks.len(),
            world.sites.len(),
            world.mixes.len(),
        );
        assert_eq!(counts, (3, 3, 25, 40, 4));

        assert!(all_unique(world.customers.iter().map(|c| c.id.0.as_str())));
        assert!(all_unique(world.plants.iter().map(|p| p.id.0.as_str())));
        assert!(all_unique(world.trucks.iter().map(|t| t.id.0.as_str())));
        assert!(all_unique(world.sites.iter().map(|s| s.id.0.as_str())));

        for truck in &world.trucks {
            assert!(world.plant(&truck.home_plant).is_some(), "{truck:?}");
        }
        for customer in &world.customers {
            assert!(
                !world.sites_of(&customer.id.0).is_empty(),
                "{} has no ship-tos",
                customer.name
            );
        }
        for site in &world.sites {
            assert!(
                world.customers.iter().any(|c| c.id == site.customer),
                "{site:?}"
            );
            assert!(
                world.geocoder.locate(site).is_some(),
                "cannot place {site:?}"
            );
        }
        let address_only = world.sites.iter().filter(|s| s.location.is_none()).count();
        assert!(
            address_only > 0,
            "the seed should exercise the postal-code geocoder"
        );
    }
}
