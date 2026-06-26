export type SearchConceptVariantPurpose = "search" | "highlight";

export const searchIrregularTokenVariants: Record<string, string[]> = {
  leak: ["leaks", "leaky", "leaking", "water intrusion"],
  leaks: ["leak", "leaky", "leaking", "leakage", "water intrusion"],
  leaky: ["leak", "leaks", "leaking", "leakage", "water intrusion"],
  leaking: ["leak", "leaks", "leaky", "leakage", "water intrusion"],
  leakage: ["leak", "leaks", "leaky", "leaking", "water intrusion"],
  malfunction: ["malfunctioning", "malfunctioned", "broken", "not working"],
  malfunctioning: ["malfunction", "malfunctioned", "broken", "not working"],
  malfunctioned: ["malfunction", "malfunctioning", "broken", "not working"],
  mouse: ["mice"],
  mice: ["mouse"],
  child: ["children", "kid", "kids"],
  children: ["child", "kid", "kids"],
  kid: ["kids", "child", "children"],
  kids: ["kid", "child", "children"],
  person: ["people"],
  people: ["person"],
  ant: ["ants"],
  ants: ["ant"],
  flea: ["fleas"],
  fleas: ["flea"],
  package: ["packages"],
  packages: ["package"],
  mailbox: ["mailboxes"],
  mailboxes: ["mailbox"],
  window: ["windows"],
  windows: ["window"],
  stair: ["stairs"],
  stairs: ["stair"],
  supply: ["supplies"],
  supplies: ["supply"],
  allergy: ["allergies"],
  allergies: ["allergy"]
};

type SearchConceptVariantRule = {
  pattern: RegExp;
  search: string[];
  highlight?: string[];
};

export const searchConceptVariantRules: SearchConceptVariantRule[] = [
  {
    pattern: /^pipes?$/,
    search: ["pipe", "pipes", "plumbing", "radiator", "radiators", "boiler", "steam heat", "heating system"],
    highlight: ["pipe", "pipes", "plumbing", "boiler", "radiator", "radiators", "steam heat", "heating system"]
  },
  {
    pattern: /^nois(?:e|es|y)$/,
    search: ["noise", "noises", "noisy", "humming", "hum", "banging", "clanging", "sound", "sounds", "vibration", "vibrating"],
    highlight: ["noise", "noises", "noisy", "humming", "banging", "clanging", "gurgling", "hissing", "tapping"]
  },
  {
    pattern: /^(heat|heater|heaters|heating)$/,
    search: ["heat", "heater", "heaters", "heating", "boiler", "radiator", "radiators", "steam heat", "heating system"]
  },
  {
    pattern: /^boilers?$/,
    search: ["boiler", "boilers", "heat", "heating", "heating system", "steam heat"]
  },
  {
    pattern: /^radiators?$/,
    search: ["radiator", "radiators", "heat", "heating", "heating system", "steam heat"]
  },
  {
    pattern: /^malfunction(?:ing|ed)?$/,
    search: ["malfunction", "malfunctioning", "malfunctioned", "broken", "not working", "not functioning", "failed", "failure", "problem", "repair", "replace"],
    highlight: ["malfunction", "malfunctioning", "malfunctioned", "broken", "not working", "not functioning", "failed", "failure", "repair"]
  },
  {
    pattern: /^winter$/,
    search: ["winter", "cold", "cold weather", "minimum room temperature", "70 degrees", "heat", "heating"],
    highlight: ["winter", "cold", "cold weather", "minimum room temperature", "room temperature"]
  },
  {
    pattern: /^mold$/,
    search: ["mold", "mildew"]
  },
  {
    pattern: /^mildew$/,
    search: ["mildew", "mold"]
  },
  {
    pattern: /^leak(?:s|y|ing|age)?$/,
    search: ["leak", "leaks", "leaky", "leaking", "leakage", "water intrusion", "water damage", "water"]
  },
  {
    pattern: /^roofs?$/,
    search: ["roof", "roofs", "ceiling", "ceilings", "exterior wall", "water intrusion"]
  },
  {
    pattern: /^ceilings?$/,
    search: ["ceiling", "ceilings", "roof", "roofs", "overhead", "water intrusion"]
  },
  {
    pattern: /^bedrooms?$/,
    search: ["bedroom", "bedrooms", "room", "rooms"]
  },
  {
    pattern: /^locks?$|^locking$|^locked$/,
    search: ["lock", "locks", "locking", "locked", "latch", "deadbolt"]
  },
  {
    pattern: /^doors?$/,
    search: ["door", "doors", "front door", "entry door"]
  },
  {
    pattern: /^electrical$|^electric$/,
    search: ["electrical", "electric", "outlet", "outlets", "wiring"]
  },
  {
    pattern: /^outlets?$/,
    search: ["outlet", "outlets", "electrical outlet", "electrical outlets", "working electrical outlet", "working electrical outlets"]
  },
  {
    pattern: /^working$/,
    search: ["working", "not working", "non working", "non-working", "not functioning", "properly functioning", "good working order"]
  },
  {
    pattern: /^broken$/,
    search: ["broken", "not working", "non working", "non-working", "not functioning", "malfunctioning", "repair", "replace"]
  },
  {
    pattern: /^rotten$|^rotted$/,
    search: ["rotten", "rotted", "rot", "dry rot", "soft", "damaged", "deteriorated"]
  },
  {
    pattern: /^floors?$|^flooring$|^boards?$/,
    search: ["floor", "floors", "flooring", "floor boards", "floorboards", "boards"]
  },
  {
    pattern: /^trash$|^garbage$|^rubbish$|^refuse$/,
    search: ["trash", "garbage", "rubbish", "refuse", "waste", "debris"]
  },
  {
    pattern: /^chutes?$/,
    search: ["chute", "chutes", "trash chute", "garbage chute", "refuse chute"]
  },
  {
    pattern: /^odou?rs?$|^smells?$|^smelly$|^stench$/,
    search: ["odor", "odors", "odour", "odours", "smell", "smells", "smelly", "stench", "foul odor", "offensive odor", "noxious odor"]
  },
  {
    pattern: /^sewers?$|^sewage$/,
    search: ["sewer", "sewers", "sewage", "waste line", "waste pipe", "plumbing"]
  },
  {
    pattern: /^drains?$|^drainage$/,
    search: ["drain", "drains", "drainage", "plumbing", "sewer", "waste line", "waste pipe"]
  },
  {
    pattern: /^clogg(?:ed|ing)?$|^clogs?$|^blocked$|^blockage$/,
    search: ["clog", "clogs", "clogged", "clogging", "blocked", "blockage", "stoppage", "obstructed"]
  },
  {
    pattern: /^back(?:ing|ed)?$|^backup$|^backups$|^overflow(?:ed|ing)?$/,
    search: ["backing", "backing up", "backed up", "backup", "backups", "overflow", "overflowed", "overflowing", "sewage backing up"]
  },
  {
    pattern: /^hallways?$|^halls?$|^corridors?$/,
    search: ["hallway", "hallways", "hall", "halls", "corridor", "corridors", "common area", "common areas"]
  },
  {
    pattern: /^bathroom$/,
    search: ["bathroom", "bath", "shower", "toilet", "sink"],
    highlight: ["bathroom", "bath", "shower", "toilet"]
  },
  {
    pattern: /^showers?$|^bathtubs?$|^tubs?$/,
    search: ["shower", "showers", "bathtub", "bathtubs", "tub", "tubs", "bath"]
  },
  {
    pattern: /^windows?$/,
    search: ["window", "windows", "window sash", "window latch", "window lock", "weatherstrip", "draft"],
    highlight: ["window", "windows", "window sash", "window frame", "window latch", "window lock", "weatherstrip"]
  },
  {
    pattern: /^kitchen$/,
    search: ["kitchen"]
  }
];

function normalizeConceptToken(value: string) {
  return String(value || "").toLowerCase().replace(/\s+/g, " ").trim();
}

export function conceptVariantsForToken(token: string, purpose: SearchConceptVariantPurpose = "search"): string[] {
  const normalized = normalizeConceptToken(token);
  if (!normalized) return [];

  const variants = new Set<string>();
  for (const rule of searchConceptVariantRules) {
    if (!rule.pattern.test(normalized)) continue;
    const values = purpose === "highlight" && rule.highlight ? rule.highlight : rule.search;
    for (const value of values) {
      const item = normalizeConceptToken(value);
      if (item) variants.add(item);
    }
  }
  return Array.from(variants).filter(Boolean);
}
