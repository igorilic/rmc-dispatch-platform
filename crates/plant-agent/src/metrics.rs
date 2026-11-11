//! The agent's Prometheus metrics (SPEC §6, §8). Every name starts with `pourline_edge_`.

pub const CONNECTIVITY: &str = "pourline_edge_connectivity";
pub const TICKETS: &str = "pourline_edge_tickets_total";

/// 1 while `link` (mqtt or dispatch) is up, 0 while it is down.
pub fn connectivity(link: &'static str, up: bool) {
    metrics::gauge!(CONNECTIVITY, "link" => link).set(if up { 1.0 } else { 0.0 });
}

/// The broker acknowledged a ticket.
pub fn ticket_sent() {
    metrics::counter!(TICKETS, "outcome" => "sent").increment(1);
}
