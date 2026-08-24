// ============================================================================
// Shared projection: Tixr Studio API order -> events_orders / events_order_items
// / events_tickets / events_users.
//
// Used by BOTH webhook-server.js (one order at a time) and backfill-orders.js
// (batched) so the two paths can never drift apart in shape.
//
// Money comes from the Studio API order, NEVER from webhook payloads
// (webhooks carry no prices). All money values are dollars.
// ============================================================================

function capitalize(str) {
    if (typeof str !== 'string' || !str) return str;
    return str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();
}

/** Tixr millis timestamp -> ISO string (null-safe). */
function msToIso(ms) {
    if (!ms) return null;
    const d = new Date(ms);
    return isNaN(d.getTime()) ? null : d.toISOString();
}

/**
 * Order-level row. webhookBody is optional: when absent (backfill path) the
 * webhook bookkeeping keys are OMITTED so an upsert never clobbers fresher
 * webhook data with nulls.
 */
function transformOrderForDB(fullOrder, webhookBody) {
    const geo = fullOrder.geo_info || {};
    const row = {
        order_id:              fullOrder.order_id,
        event_id:              fullOrder.event_id,
        user_id:               fullOrder.user_id != null ? String(fullOrder.user_id) : null,

        status:                fullOrder.status || null,
        order_type:            fullOrder.type || webhookBody?.order_type || null,
        order_source:          webhookBody?.order_source || fullOrder.order_source || null,
        fulfillment_path:      fullOrder.fulfillment_path || webhookBody?.order_fulfillment_path || null,
        purchase_date:         msToIso(fullOrder.purchase_date),
        refund_date:           msToIso(fullOrder.refund_date),
        cancellation_date:     msToIso(fullOrder.cancellation_date),

        currency:              fullOrder.currency || null,
        exchange_rate:         fullOrder.exchange_rate ?? null,
        gross_sales:           fullOrder.gross_sales ?? null,
        net:                   fullOrder.net ?? null,
        total:                 fullOrder.total ?? null,
        taxes:                 fullOrder.taxes ?? null,
        fees:                  fullOrder.fees ?? null,
        credit_card_fees:      fullOrder.credit_card_fees ?? null,
        delivery_fees:         fullOrder.delivery_fees ?? null,
        discount:              fullOrder.discount ?? null,

        first_name:            capitalize(fullOrder.first_name) || null,
        last_name:             capitalize(fullOrder.lastname) || null,
        email:                 fullOrder.email || null,
        opt_in:                fullOrder.opt_in ?? null,
        opt_in_date:           msToIso(fullOrder.opt_in_date),
        geo_city:              geo.city || null,
        geo_state:             geo.state || null,
        geo_country:           geo.country_code || null,
        geo_postal:            geo.postal_code || null,
        geo_lat:               geo.latitude ?? null,
        geo_lng:               geo.longitude ?? null,

        ref_id:                fullOrder.ref_id || null,
        ref_type:              fullOrder.ref_type || null,
        referrer:              fullOrder.referrer || null,
        seller_id:             fullOrder.seller_id || null,
        user_agent_type:       fullOrder.user_agent_type || null,
        card_type:             fullOrder.card_type || null,

        api_synced_at:         new Date().toISOString(),
        updated_at:            new Date().toISOString(),
    };

    if (webhookBody) {
        row.last_transaction_type = webhookBody.transaction_type || null;
        row.webhook_updated_at    = new Date().toISOString();
    }

    return row;
}

function transformOrderItems(fullOrder) {
    return (fullOrder.sale_items || []).map(item => ({
        order_id:     fullOrder.order_id,
        sale_id:      item.sale_id,
        event_id:     fullOrder.event_id,
        tier_id:      item.tier_id ?? null,
        name:         item.name || null,
        category:     item.category || null,
        quantity:     item.quantity || 0,
        net:          item.net ?? null,
        total:        item.total ?? null,
        tax:          item.tax ?? null,
        group_fee:    item.group_fee ?? null,
        delivery_fee: item.delivery_fee ?? null,
        hold_id:      item.hold_id ?? null,
        hold_label:   item.hold_label || null,
    }));
}

function transformTickets(fullOrder) {
    const rows = [];
    for (const item of (fullOrder.sale_items || [])) {
        for (const ticket of (item.tickets || [])) {
            if (!ticket.serial_number) continue;
            rows.push({
                serial_number:     String(ticket.serial_number),
                order_id:          fullOrder.order_id,
                sale_id:           item.sale_id ?? null,
                event_id:          fullOrder.event_id,
                status:            ticket.status || null,
                holder_first_name: capitalize(ticket.first_name) || null,
                holder_last_name:  capitalize(ticket.lastname) || null,
                updated_at:        new Date().toISOString(),
            });
        }
    }
    return rows;
}

/** events_users row from an order (event_ids merging is the caller's job). */
function transformUserFromOrder(fullOrder, mergedEventIds) {
    const geo = fullOrder.geo_info || {};
    return {
        user_id:            String(fullOrder.user_id),
        user_first_name:    capitalize(fullOrder.first_name),
        user_last_name:     capitalize(fullOrder.lastname),
        user_mail:          fullOrder.email,
        user_opt_in:        fullOrder.opt_in,
        user_city:          geo.city,
        user_state:         geo.state,
        user_country:       geo.country_code,
        user_postal:        geo.postal_code,
        event_ids:          mergedEventIds,
        user_last_purchase: msToIso(fullOrder.purchase_date),
    };
}

/**
 * Per-order projection (webhook path): upserts the order, its items, its
 * tickets and the buyer profile. Returns { items, tickets } counts.
 */
async function projectOrder(supabase, fullOrder, webhookBody) {
    // 1. Order row (money lives here, once)
    const orderRow = transformOrderForDB(fullOrder, webhookBody);
    const { error: orderErr } = await supabase.from('events_orders').upsert(orderRow, { onConflict: 'order_id' });
    if (orderErr) throw new Error(`events_orders upsert failed: ${orderErr.message}`);

    // 2. Items — clean slate per order so removed/cancelled items don't linger
    const items = transformOrderItems(fullOrder);
    await supabase.from('events_order_items').delete().eq('order_id', fullOrder.order_id);
    if (items.length > 0) {
        const { error: itemErr } = await supabase.from('events_order_items').insert(items);
        if (itemErr) throw new Error(`events_order_items insert failed: ${itemErr.message}`);
    }

    // 3. Tickets (per serial — live door updates land here later)
    const tickets = transformTickets(fullOrder);
    if (tickets.length > 0) {
        const { error: ticketErr } = await supabase.from('events_tickets').upsert(tickets, { onConflict: 'serial_number' });
        if (ticketErr) throw new Error(`events_tickets upsert failed: ${ticketErr.message}`);
    }

    // 4. Keep the enriched user profile fresh (events_users is kept long-term)
    if (fullOrder.user_id) {
        const userIdStr = String(fullOrder.user_id);
        const { data: existingUser } = await supabase.from('events_users').select('event_ids').eq('user_id', userIdStr).maybeSingle();
        const existingEvents = existingUser?.event_ids || [];
        const updatedEvents = Array.from(new Set([...existingEvents, fullOrder.event_id]));

        const { error: userErr } = await supabase.from('events_users')
            .upsert(transformUserFromOrder(fullOrder, updatedEvents), { onConflict: 'user_id' });
        if (userErr) console.error(`  ⚠️ events_users upsert failed: ${userErr.message}`);
    }

    return { items: items.length, tickets: tickets.length };
}

module.exports = {
    capitalize,
    msToIso,
    transformOrderForDB,
    transformOrderItems,
    transformTickets,
    transformUserFromOrder,
    projectOrder,
};
