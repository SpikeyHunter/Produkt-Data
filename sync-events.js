const { createClient } = require("@supabase/supabase-js");
const axios = require("axios");
const crypto = require("crypto");

console.log("🚀 Starting Tixr Events Sync (ENHANCED)...");

// Load environment variables
if (process.env.NODE_ENV !== "production") {
  try {
    require("dotenv").config();
  } catch (error) {
    console.log("dotenv not available, using system environment variables");
  }
}

// Configuration
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_KEY;
const GROUP_ID = process.env.TIXR_GROUP_ID || "980";
const CPK = process.env.TIXR_CPK;
const SECRET_KEY = process.env.TIXR_SECRET_KEY;

// Validation
if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY || !CPK || !SECRET_KEY) {
  console.error("❌ Missing required environment variables");
  console.error(
    "Required: SUPABASE_URL, SUPABASE_KEY, TIXR_CPK, TIXR_SECRET_KEY"
  );
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// ==================== ARTIST EXTRACTION ====================
const EXCLUDE_LIST = [
  "moet city",
  "moët city",
  "le grand prix",
  "prix",
  "mutek",
  "édition",
  "évènement spécial",
  "room202",
  "produktworld",
  "admission",
  "taraka",
  "bazart",
  "city gas",
  "showcase",
  "special guest",
  "guests",
  "invité",
  "guest",
  "festival",
  "event",
  "experience",
  "produtk",
  "produkt",
  "soirée",
  "party",
  "post-race",
  "officiel",
  "after party",
  "ncg360",
  "visionnement",
  "montréal",
  "grand match",
  "off-piknic",
  "piknic",
  "ticket",
  "table",
  "official",
  "pass",
  "réveillon",
];

const INCLUDE_LIST = ["mimouna night", "dome of faith"];

function toTitleCase(name) {
  if (!name) return null;
  const preserveWords = [
    "DJ",
    "MC",
    "NYC",
    "LA",
    "UK",
    "USA",
    "II",
    "III",
    "IV",
  ];

  return name
    .split(/\s+/)
    .map((word) => {
      const upperWord = word.toUpperCase();
      if (preserveWords.includes(upperWord)) return upperWord;

      if (word.includes("-")) {
        return word
          .split("-")
          .map(
            (part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase()
          )
          .join("-");
      }

      return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
    })
    .join(" ");
}

function extractMainArtist(eventName) {
  if (!eventName || typeof eventName !== "string") return null;
  let name = eventName.trim();

  if (INCLUDE_LIST.some((w) => name.toLowerCase().includes(w))) {
    return toTitleCase(
      name
        .replace(/Takeover$/i, "")
        .replace(/Night$/i, "")
        .trim()
    );
  }

  name = name.replace(/^gp\d+[:\-\s]*/i, "");
  name = name.replace(/^(.+?)\s+(présente|présentent|presents?)\s+.+$/i, "$1");

  const delimiters = [", ", " + ", " b2b ", " & ", " x ", " / ", " vs ", " v "];
  for (const delimiter of delimiters) {
    if (name.includes(delimiter)) {
      name = name.split(delimiter)[0];
      break;
    }
  }

  name = name.replace(
    /\s+(et invités|and guests?|avec|feat\.?|featuring|ft\.?|w\/)\s.*$/i,
    ""
  );
  name = name.replace(/ *[\(\[].*?[\)\]] */g, " ");
  name = name.split("|")[0].split("-")[0].split(":")[0].split("@")[0].trim();

  if (/(\w+)'s\b/i.test(name)) {
    const match = name.match(/^(.+?)'s\b/i);
    if (match) name = match[1];
  }

  name = name.replace(
    /\b(tour(née)?|edition|montr[eé]al|takeover|night|experience|showcase|official|after\s?party|post-race)\b.*$/i,
    ""
  );
  name = name.replace(/\b\d{4}\b/g, "").replace(/\d{5,}/g, "");
  name = name.replace(/[-—–|•:]+$/g, "");
  name = name.replace(/^[^a-z0-9]+|[^a-z0-9]+$/gi, "");

  let main = toTitleCase(name.replace(/\s{2,}/g, " "));

  if (!main || !main.length) return null;

  const lowerMain = main.toLowerCase();
  const excludeRegex = new RegExp(`\\b(${EXCLUDE_LIST.join("|")})\\b`, "i");
  if (
    excludeRegex.test(lowerMain) &&
    !INCLUDE_LIST.some((w) => lowerMain.includes(w))
  ) {
    return null;
  }

  main = main
    .replace(/\b(\d{2,4}|live|tour|edition|set|experience)\b$/gi, "")
    .trim();

  return main || null;
}

function extractArtistFromEvent(tixrEvent) {
  if (tixrEvent.lineups?.length > 0) {
    for (const lineup of tixrEvent.lineups) {
      if (lineup.acts?.length > 0) {
        const sortedActs = lineup.acts.sort(
          (a, b) => (a.rank || 999) - (b.rank || 999)
        );
        if (sortedActs[0]?.artist?.name) {
          return toTitleCase(sortedActs[0].artist.name.trim());
        }
      }
    }
  }
  return extractMainArtist(tixrEvent.name);
}

// ==================== STATUS LOGIC ====================
function computeEventStatus(eventDate) {
  const now = new Date();
  const eventStart = new Date(eventDate + "T00:00:00");
  const eventEnd = new Date(eventStart);
  eventEnd.setDate(eventEnd.getDate() + 1);
  eventEnd.setHours(4, 0, 0, 0);

  return now > eventEnd ? "PAST" : "LIVE";
}

// ==================== DATE CONVERSION ====================
function convertToMontrealDate(utcDateString) {
  if (!utcDateString) return null;
  const utcDate = new Date(utcDateString);

  return utcDate.toLocaleDateString("en-CA", {
    timeZone: "America/Montreal",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
}

// ==================== TIXR API FUNCTIONS ====================
function buildHash(basePath, paramsObj) {
  const paramsSorted = Object.keys(paramsObj)
    .sort()
    .map((k) => `${k}=${encodeURIComponent(paramsObj[k])}`)
    .join("&");
  const hashString = `${basePath}?${paramsSorted}`;
  const hash = crypto
    .createHmac("sha256", SECRET_KEY)
    .update(hashString)
    .digest("hex");
  return { paramsSorted, hash };
}

async function fetchAllTixrEvents() {
  console.log(`📥 Fetching all events from Tixr group ${GROUP_ID}...`);
  const allEvents = [];
  let pageNumber = 1;
  let hasMorePages = true;

  while (hasMorePages) {
    const basePath = `/v1/groups/${GROUP_ID}/events`;
    const t = Date.now();
    const params = { cpk: CPK, t, page_number: pageNumber, page_size: 100 };
    const { paramsSorted, hash } = buildHash(basePath, params);
    const url = `https://studio.tixr.com${basePath}?${paramsSorted}&hash=${hash}`;

    try {
      console.log(`  📄 Fetching page ${pageNumber}...`);
      const { data } = await axios.get(url, { timeout: 15000 });

      if (!Array.isArray(data) || data.length === 0) {
        hasMorePages = false;
        break;
      }

      allEvents.push(...data);
      console.log(`  ✓ Page ${pageNumber}: ${data.length} events`);

      if (data.length < 100) {
        hasMorePages = false;
      } else {
        pageNumber++;
        await new Promise((resolve) => setTimeout(resolve, 250));
      }
    } catch (error) {
      console.error(`  ❌ Error fetching page ${pageNumber}:`, error.message);
      console.log("  🔄 Retrying in 5 seconds...");
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }

  console.log(`✅ Total events fetched: ${allEvents.length}`);
  return allEvents;
}

async function fetchTixrEventById(eventId) {
  const basePath = `/v1/groups/${GROUP_ID}/events/${eventId}`;
  const t = Date.now();
  const params = { cpk: CPK, t };
  const { paramsSorted, hash } = buildHash(basePath, params);
  const url = `https://studio.tixr.com${basePath}?${paramsSorted}&hash=${hash}`;

  try {
    const { data } = await axios.get(url, { timeout: 10000 });
    return Array.isArray(data) ? data[0] : data;
  } catch (error) {
    console.error(`Error fetching event ${eventId}:`, error.message);
    throw error;
  }
}

// ==================== TRANSFORM EVENT FOR DATABASE ====================
function transformEventForDB(tixrEvent) {
  const eventDate = convertToMontrealDate(tixrEvent.start_date);

  return {
    event_id: parseInt(tixrEvent.id),
    event_name: tixrEvent.name,
    event_date: eventDate,
    event_artist: extractArtistFromEvent(tixrEvent),
    event_status: computeEventStatus(eventDate),
    event_genre: null,
    event_flyer: tixrEvent.flyer_url || tixrEvent.mobile_image_url || null,
    event_tags: null,
    event_updated: new Date().toISOString(),
    event_order_updated: null,
    event_attendance_updated: null,
    timetable: null,
    timetable_active: true,
  };
}

// ==================== CHANGE DETECTION FUNCTION ====================
async function checkForEventChanges() {
  console.log("\n🔍 Checking for event changes...");
  const startTime = Date.now();

  try {
    // Get current events from database
    const { data: dbEvents, error: dbError } = await supabase
      .from("events")
      .select("event_id, event_name, event_date, event_flyer, event_status")
      .order("event_id");

    if (dbError) throw dbError;

    // Create a map for quick lookup
    const dbEventsMap = new Map();
    dbEvents.forEach((event) => {
      dbEventsMap.set(event.event_id, event);
    });

    // Fetch fresh data from Tixr
    const tixrEvents = await fetchAllTixrEvents();

    const changes = {
      new: [],
      updated: [],
      removed: [],
      statusChanged: [],
    };

    // Check for new and updated events
    for (const tixrEvent of tixrEvents) {
      const eventId = parseInt(tixrEvent.id);
      const dbEvent = dbEventsMap.get(eventId);
      const freshEvent = transformEventForDB(tixrEvent);

      if (!dbEvent) {
        // New event found
        changes.new.push(freshEvent);
        console.log(
          `  🆕 New event: ${freshEvent.event_name} (ID: ${eventId})`
        );
      } else {
        // Check for changes
        const hasChanges =
          dbEvent.event_name !== freshEvent.event_name ||
          dbEvent.event_date !== freshEvent.event_date ||
          dbEvent.event_flyer !== freshEvent.event_flyer;

        const statusChanged = dbEvent.event_status !== freshEvent.event_status;

        if (hasChanges || statusChanged) {
          changes.updated.push(freshEvent);

          if (statusChanged) {
            changes.statusChanged.push({
              id: eventId,
              name: freshEvent.event_name,
              oldStatus: dbEvent.event_status,
              newStatus: freshEvent.event_status,
            });
            console.log(
              `  🔄 Status change: ${freshEvent.event_name} (${dbEvent.event_status} → ${freshEvent.event_status})`
            );
          }

          if (hasChanges) {
            console.log(
              `  📝 Updated: ${freshEvent.event_name} (ID: ${eventId})`
            );
          }
        }

        // Remove from map to track removed events
        dbEventsMap.delete(eventId);
      }
    }

    // Check for removed events (left in the map)
    for (const [eventId, dbEvent] of dbEventsMap) {
      // 🛡️ PROTECT CUSTOM EVENTS: If ID is less than 10,000, do not delete it.
      if (eventId < 10000) {
        // console.log(`  🛡️ Ignoring custom event: ${dbEvent.event_name} (ID: ${eventId})`);
        continue;
      }

      changes.removed.push(eventId);
      console.log(
        `  🗑️ Removed from Tixr: ${dbEvent.event_name} (ID: ${eventId})`
      );
    }

    // Apply changes to database
    if (changes.new.length > 0 || changes.updated.length > 0) {
      const eventsToUpsert = [...changes.new, ...changes.updated];

      console.log(
        `\n💾 Applying ${eventsToUpsert.length} changes to database...`
      );

      // Upsert in batches
      const batchSize = 100;
      for (let i = 0; i < eventsToUpsert.length; i += batchSize) {
        const batch = eventsToUpsert.slice(i, i + batchSize);
        const { error } = await supabase
          .from("events")
          .upsert(batch, { onConflict: "event_id" });

        if (error) {
          console.error(`  ❌ Error saving batch:`, error.message);
        } else {
          console.log(
            `  ✓ Saved batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(
              eventsToUpsert.length / batchSize
            )}`
          );
        }
      }
    }

    // Remove deleted events
    if (changes.removed.length > 0) {
      console.log(`\n🗑️ Removing ${changes.removed.length} deleted events...`);
      const { error } = await supabase
        .from("events")
        .delete()
        .in("event_id", changes.removed);

      if (error) {
        console.error("  ❌ Error removing events:", error.message);
      } else {
        console.log("  ✓ Events removed");
      }
    }

    // Summary
    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log("\n📊 Change Detection Summary:");
    console.log(`  New events: ${changes.new.length}`);
    console.log(`  Updated events: ${changes.updated.length}`);
    console.log(`  Status changes: ${changes.statusChanged.length}`);
    console.log(`  Removed events: ${changes.removed.length}`);
    console.log(`  Time taken: ${duration}s`);

    if (
      changes.new.length === 0 &&
      changes.updated.length === 0 &&
      changes.removed.length === 0
    ) {
      console.log("\n✅ All events are up to date!");
    } else {
      console.log("\n✅ Changes applied successfully!");
    }
  } catch (error) {
    console.error("❌ Error checking for changes:", error);
    throw error;
  }
}

// ==================== MAIN SYNC FUNCTION ====================
async function syncAllEvents() {
  const startTime = Date.now();

  try {
    console.log("🔌 Testing Supabase connection...");
    const { error: testError } = await supabase
      .from("events")
      .select("count")
      .limit(1);
    if (testError) {
      console.error("❌ Supabase connection failed:", testError);
      process.exit(1);
    }
    console.log("✅ Supabase connected\n");

    const tixrEvents = await fetchAllTixrEvents();
    if (tixrEvents.length === 0) {
      console.log("No events to process. Exiting.");
      return;
    }

    console.log(
      `\n🔄 Transforming ${tixrEvents.length} events for database...`
    );
    const eventsToUpsert = tixrEvents.map(transformEventForDB).filter(Boolean);

    const statusCounts = eventsToUpsert.reduce((acc, event) => {
      acc[event.event_status] = (acc[event.event_status] || 0) + 1;
      return acc;
    }, {});

    console.log("📊 Status breakdown:");
    console.log(`   LIVE: ${statusCounts.LIVE || 0}`);
    console.log(`   PAST: ${statusCounts.PAST || 0}`);

    console.log(`\n💾 Saving ${eventsToUpsert.length} events to Supabase...`);
    const batchSize = 100;

    for (let i = 0; i < eventsToUpsert.length; i += batchSize) {
      const batch = eventsToUpsert.slice(i, i + batchSize);
      const { error } = await supabase
        .from("events")
        .upsert(batch, { onConflict: "event_id" });

      if (error) {
        console.error(`  ❌ Error saving batch:`, error.message);
      } else {
        console.log(
          `  ✓ Saved batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(
            eventsToUpsert.length / batchSize
          )}`
        );
      }
    }

    const totalTime = (Date.now() - startTime) / 1000;
    console.log(`\n✨ Sync complete in ${totalTime.toFixed(1)}s!`);
  } catch (error) {
    console.error("\n❌ Fatal error during sync:", error);
    process.exit(1);
  }
}

// ==================== STATUS UPDATE FUNCTION ====================
async function updateStatuses() {
  console.log("🔄 Updating event statuses...");

  try {
    const { data: liveEvents, error } = await supabase
      .from("events")
      .select("event_id, event_date")
      .eq("event_status", "LIVE");

    if (error) throw error;

    if (!liveEvents || liveEvents.length === 0) {
      console.log("  No LIVE events to check");
      return;
    }

    const eventsToUpdate = [];

    for (const event of liveEvents) {
      const status = computeEventStatus(event.event_date);

      if (status === "PAST") {
        eventsToUpdate.push(event.event_id);
      }
    }

    if (eventsToUpdate.length > 0) {
      const { error: updateError } = await supabase
        .from("events")
        .update({
          event_status: "PAST",
          event_updated: new Date().toISOString(),
        })
        .in("event_id", eventsToUpdate);

      if (updateError) {
        console.error("  ❌ Error updating statuses:", updateError);
      } else {
        console.log(`  ✅ Updated ${eventsToUpdate.length} events to PAST`);
      }
    } else {
      console.log("  ✓ All statuses are correct");
    }
  } catch (error) {
    console.error("  ❌ Error in status update:", error);
  }
}

// ==================== MAIN EXECUTION ====================
async function main() {
  const command = process.argv[2] || "sync";

  console.log("╔══════════════════════════════════════╗");
  console.log("║   TIXR EVENTS SYNC - ENHANCED       ║");
  console.log("╚══════════════════════════════════════╝\n");

  switch (command) {
    case "sync":
      // Full sync
      await syncAllEvents();
      break;

    case "status":
      // Only update statuses (for hourly cron)
      await updateStatuses();
      break;

    case "check-changes":
      // Check for any event changes (new, updated, removed)
      await checkForEventChanges();
      break;

    case "status-and-changes":
      // Combined: update statuses then check for changes
      await updateStatuses();
      console.log("\n" + "═".repeat(40) + "\n");
      await checkForEventChanges();
      break;

    default:
      console.log("Usage:");
      console.log(
        "  node sync-events.js sync              - Full sync of all events"
      );
      console.log(
        "  node sync-events.js status            - Update statuses only"
      );
      console.log(
        "  node sync-events.js check-changes     - Check for event changes"
      );
      console.log(
        "  node sync-events.js status-and-changes - Both status update and change check"
      );
      process.exit(1);
  }

  console.log("\n✅ Done!");
  process.exit(0);
}

// Run
main();
