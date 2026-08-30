//
// name-case.js — one house style for artist names.
//
// Flyers shout ("SARA LANDRY"), listing pages whisper, and the extractor
// copies whatever it saw. The same artist then lands in the database three
// ways and never groups. So names get normalised on the way in.
//
// The one rule that matters: an artist who already uses lower case or mixed
// case chose that (jigitz, deadmau5, A$AP). Only SHOUTING gets rewritten.
//

// Tokens that are genuinely upper case, not shouting.
const KEEP_UPPER = new Set([
  'DJ', 'MC', 'VJ', 'LP', 'EP', 'UK', 'US', 'USA', 'NYC', 'LA', 'MTL', 'QC',
  'II', 'III', 'IV', 'VI', 'VII', 'VIII', 'IX', 'XI', 'XII',
  'AM', 'PM', 'RIP', 'NRG', 'TBA', 'VIP',
]);

// Back-to-back and friends read as lower case between two names.
const LOWER_JOINERS = new Set(['B2B', 'B3B', 'VS', 'X', 'FT', 'FEAT', 'AND', 'WITH', 'PRESENTS']);

/** Capitalises one word, respecting hyphens and name apostrophes. */
function capitalizeWord(word) {
  // O'BRIEN → O'Brien, but DON'T → Don't
  const apostrophe = word.replace(/([A-ZÀ-Þ][A-ZÀ-Þa-zß-ÿ]*)(['’])([A-ZÀ-Þ][A-ZÀ-Þa-zß-ÿ]*)/g,
    (_, head, mark, tail) =>
      head.length <= 2 ? cap(head) + mark + cap(tail) : cap(head) + mark + tail.toLowerCase());
  if (apostrophe !== word) return apostrophe;
  // JEAN-MICHEL → Jean-Michel
  return word.split('-').map(cap).join('-');
}

function cap(part) {
  if (!part) return part;
  return part.charAt(0).toUpperCase() + part.slice(1).toLowerCase();
}

/**
 * "SARA LANDRY" → "Sara Landry". "jigitz" → "jigitz". "MK" → "MK".
 * Returns the input untouched for anything that isn't shouting.
 */
function normalizeArtistName(value) {
  if (typeof value !== 'string') return value;
  const name = value.trim().replace(/\s+/g, ' ');
  if (!name) return name;

  // Already styled by its owner — hands off.
  if (/[a-zß-ÿ]/.test(name)) return name;
  // Nothing to case (numbers, symbols, non-Latin scripts).
  if (!/[A-ZÀ-Þ]/.test(name)) return name;

  return name.split(' ').map(token => {
    const bare = token.replace(/[^A-ZÀ-Þ0-9]/g, '');
    if (KEEP_UPPER.has(bare)) return token;
    if (LOWER_JOINERS.has(bare)) return token.toLowerCase();
    // Short all-caps tokens are acronyms and initials, not shouting: MK, KDA.
    if (bare.length > 0 && bare.length <= 3 && /^[A-ZÀ-Þ]+$/.test(bare)) return token;
    return capitalizeWord(token);
  }).join(' ');
}

module.exports = { normalizeArtistName };
