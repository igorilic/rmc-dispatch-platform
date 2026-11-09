//! Checks a mapped order against master data: the integration's own customer, an existing ship-to
//! that belongs to that customer, and a known mix. Customers and ship-tos are our master data;
//! an order can only reference them, never introduce them.

use std::collections::BTreeMap;

use domain::Seed;

use crate::mapping::{Field, FieldError, FieldSpec, Mapping, Values};

pub(crate) fn check(
    mapping: &Mapping,
    values: &Values,
    fields: &BTreeMap<Field, FieldSpec>,
    seed: &Seed,
) -> Vec<FieldError> {
    let mut errors = Vec::new();
    let mut error = |field: Field, message: String| {
        let name = fields
            .get(&field)
            .map(FieldSpec::name)
            .unwrap_or_default()
            .to_owned();
        errors.push(FieldError {
            field: Some(field),
            name,
            message,
        });
    };
    if let Some(customer) = &values.customer {
        if *customer != mapping.customer {
            let message = format!(
                "orders for customer {customer} are not accepted from {} (expects {})",
                mapping.integration, mapping.customer
            );
            error(Field::Customer, message);
        } else if seed.customer(customer).is_none() {
            error(Field::Customer, format!("unknown customer {customer}"));
        }
    }
    if let Some(ship_to) = &values.ship_to {
        match seed.site(ship_to) {
            None => error(Field::ShipTo, format!("unknown ship-to {ship_to}")),
            Some(site) => {
                if let Some(customer) = values.customer.as_ref().filter(|c| site.customer.0 != **c)
                {
                    let owner = &site.customer.0;
                    error(
                        Field::ShipTo,
                        format!("ship-to {ship_to} belongs to customer {owner}, not {customer}"),
                    );
                }
            }
        }
    }
    if let Some(mix) = &values.mix
        && seed.mix(mix).is_none()
    {
        error(Field::Mix, format!("unknown mix {mix}"));
    }
    errors
}
