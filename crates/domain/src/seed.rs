//! The seeded world from `deploy/seed/rhine-neckar.json`. It holds our master data (customers and
//! their ship-tos, with a postal-code table for address-only sites) plus plants, trucks and mixes.
//! The simulators, ingest and dispatch all read the same file, so they agree on every id.

use serde::Deserialize;

use crate::{Customer, MixDesign, Plant, PostalCodeGeocoder, Site, Truck};

#[derive(Debug, Deserialize)]
pub struct Seed {
    pub customers: Vec<Customer>,
    #[serde(rename = "postal_codes")]
    pub geocoder: PostalCodeGeocoder,
    pub plants: Vec<Plant>,
    pub trucks: Vec<Truck>,
    pub sites: Vec<Site>,
    pub mixes: Vec<MixDesign>,
}

impl Seed {
    /// The seed is compiled in, so no binary needs data files at runtime.
    pub fn rhine_neckar() -> serde_json::Result<Seed> {
        serde_json::from_str(include_str!("../../../deploy/seed/rhine-neckar.json"))
    }

    pub fn customer(&self, id: &str) -> Option<&Customer> {
        self.customers.iter().find(|c| c.id.0 == id)
    }

    pub fn site(&self, id: &str) -> Option<&Site> {
        self.sites.iter().find(|s| s.id.0 == id)
    }

    /// The ship-tos of the customer with this customer number.
    pub fn sites_of(&self, customer: &str) -> Vec<&Site> {
        self.sites
            .iter()
            .filter(|s| s.customer.0 == customer)
            .collect()
    }

    pub fn plant(&self, id: &str) -> Option<&Plant> {
        self.plants.iter().find(|p| p.id.0 == id)
    }

    pub fn truck(&self, id: &str) -> Option<&Truck> {
        self.trucks.iter().find(|t| t.id.0 == id)
    }

    pub fn mix(&self, code: &str) -> Option<&MixDesign> {
        self.mixes.iter().find(|m| m.code.0 == code)
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
        let seed = Seed::rhine_neckar().unwrap();
        let counts = (
            seed.customers.len(),
            seed.plants.len(),
            seed.trucks.len(),
            seed.sites.len(),
            seed.mixes.len(),
        );
        assert_eq!(counts, (3, 3, 25, 40, 4));

        assert!(all_unique(seed.customers.iter().map(|c| c.id.0.as_str())));
        assert!(all_unique(seed.plants.iter().map(|p| p.id.0.as_str())));
        assert!(all_unique(seed.trucks.iter().map(|t| t.id.0.as_str())));
        assert!(all_unique(seed.sites.iter().map(|s| s.id.0.as_str())));

        for truck in &seed.trucks {
            assert!(seed.plant(&truck.home_plant.0).is_some(), "{truck:?}");
            assert_eq!(seed.truck(&truck.id.0), Some(truck));
        }
        for customer in &seed.customers {
            assert!(
                !seed.sites_of(&customer.id.0).is_empty(),
                "{} has no ship-tos",
                customer.name
            );
            assert_eq!(seed.customer(&customer.id.0), Some(customer));
        }
        for site in &seed.sites {
            assert!(seed.customer(&site.customer.0).is_some(), "{site:?}");
            assert!(
                seed.geocoder.locate(site).is_some(),
                "cannot place {site:?}"
            );
            assert_eq!(seed.site(&site.id.0), Some(site));
        }
        for mix in &seed.mixes {
            assert_eq!(seed.mix(&mix.code.0), Some(mix));
        }
        let address_only = seed.sites.iter().filter(|s| s.location.is_none()).count();
        assert!(
            address_only > 0,
            "the seed should exercise the postal-code geocoder"
        );
        assert_eq!(seed.site("99999999"), None);
    }
}
