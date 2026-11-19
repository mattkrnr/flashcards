"""Builds the starter decks in decks/*.json.

Card formats:
  ("front", "back")                               basic card
  ("front", "back", ["wrong", "wrong", "wrong"])  multiple choice
  map cards are built in GEO_MAP below
Run: python3 tools/build_decks.py
"""
import json, os

ROOT = os.path.join(os.path.dirname(__file__), "..", "decks")

def card(t):
    if len(t) == 2:
        return {"front": t[0], "back": t[1], "kind": "basic"}
    return {"front": t[0], "back": t[1], "kind": "choice", "choices": t[2]}

# ───────────────────────── System design ─────────────────────────
SYSTEM_DESIGN = [
    # Caching
    ("Cache-aside (lazy loading): who loads data into the cache?", "The application: on a miss it reads the database, then writes the result into the cache."),
    ("Write-through vs. write-back caching", "Write-through writes the cache and the database together (safe, slower writes). Write-back writes the cache first and flushes to the database later (fast, can lose data on a crash)."),
    ("What is a cache stampede (thundering herd)?", "A hot key expires and many requests miss at once, all hitting the database together."),
    ("Three ways to prevent a cache stampede", "Request coalescing (one request recomputes, others wait), a lock or lease per key, and early probabilistic refresh / jittered TTLs."),
    ("Why add random jitter to cache TTLs?", "So keys written together don't all expire together and cause a burst of misses."),
    ("LRU vs. LFU eviction", "LRU evicts what was used longest ago; LFU evicts what is used least often. LFU keeps steady favorites; LRU adapts faster to new patterns."),
    ("Which eviction policy fits a scan-heavy workload that would flush an LRU cache?", "A scan-resistant policy such as LFU, ARC, or segmented LRU (or 2Q / W-TinyLFU).", ["Plain LRU", "FIFO", "Random"]),
    ("What does a CDN cache, and where?", "Static (and some dynamic) responses at edge servers near users, cutting latency and origin load."),
    ("What is negative caching?", "Caching 'not found' results so repeated lookups for missing keys don't hit the database."),
    ("Hardest part of caching, per the classic joke?", "Cache invalidation (and naming things, and off-by-one errors)."),
    ("Redis vs. Memcached: which supports data structures, persistence and replication?", "Redis", ["Memcached", "Both equally", "Neither"]),
    # Hashing
    ("Problem with hash(key) mod N for sharding", "When N changes, almost every key moves to a different server."),
    ("Consistent hashing in one sentence", "Servers and keys sit on a hash ring; each key goes to the next server clockwise, so adding or removing a server moves only about 1/N of keys."),
    ("Why use virtual nodes in consistent hashing?", "Each server gets many points on the ring, which evens out load and spreads a failed server's keys across many others."),
    ("What is rendezvous (highest random weight) hashing?", "For each key, score every server with hash(key, server) and pick the highest. Like consistent hashing, few keys move when servers change."),
    ("What does a Bloom filter answer, and what errors can it make?", "'Is X possibly in the set?' It can give false positives but never false negatives."),
    ("Why can't you delete from a standard Bloom filter?", "Clearing a bit might also remove other items that set it. A counting Bloom filter fixes this."),
    ("What is HyperLogLog used for?", "Estimating the number of distinct items (cardinality) using a tiny, fixed amount of memory."),
    ("What does a count-min sketch estimate?", "How often items appear in a stream (frequencies), possibly overestimating, never underestimating."),
    ("Cryptographic vs. non-cryptographic hash: which for a hash table?", "Non-cryptographic (e.g. MurmurHash, xxHash): much faster, and collision resistance against attackers isn't needed.", ["SHA-256", "bcrypt", "MD5"]),
    ("Why store passwords with bcrypt, scrypt or Argon2 instead of SHA-256?", "They're deliberately slow and salted, which makes brute-force and rainbow-table attacks expensive."),
    # Databases
    ("ACID stands for…", "Atomicity, Consistency, Isolation, Durability."),
    ("Read committed vs. repeatable read", "Read committed only shows committed data but a re-read can change. Repeatable read keeps rows you've read stable for the whole transaction."),
    ("What anomaly does serializable isolation prevent that snapshot isolation allows?", "Write skew: two transactions read overlapping data and write different rows, breaking a rule neither saw broken.", ["Dirty reads", "Lost updates on the same row", "Phantom reads only"]),
    ("What is MVCC?", "Multi-version concurrency control: writers create new row versions so readers see a consistent snapshot without blocking."),
    ("B-tree vs. LSM-tree: which favors write-heavy workloads?", "LSM-tree (buffers writes in memory, flushes sorted files, compacts later).", ["B-tree", "Hash index", "Bitmap index"]),
    ("What is write amplification in an LSM-tree?", "Each logical write is rewritten several times during compaction, so disk writes exceed app writes."),
    ("What is a covering index?", "An index containing every column a query needs, so the database never has to read the table rows."),
    ("Composite index on (a, b): can it serve a filter on b alone?", "Usually not efficiently. Composite indexes work left to right (leftmost prefix)."),
    ("Leader-follower replication: what is replication lag's classic user-facing bug?", "You write, then read from a stale follower and your own change seems missing. Fix with read-your-writes routing."),
    ("Synchronous vs. asynchronous replication trade-off", "Synchronous never loses acknowledged writes but adds latency and stalls on slow replicas. Async is fast but can lose recent writes on failover."),
    ("What does a quorum of W + R > N guarantee?", "Every read overlaps at least one replica holding the latest write."),
    ("CAP theorem in practice", "During a network partition, a system must choose between consistency and availability."),
    ("What does PACELC add to CAP?", "Else (no partition), there's still a trade-off between latency and consistency."),
    ("Range vs. hash partitioning", "Range keeps neighbors together (good scans, risk of hot spots). Hash spreads load evenly but range scans hit every shard."),
    ("What's a hot partition, and a common fix?", "One shard gets far more traffic than others. Fix by salting/splitting the hot key or choosing a better partition key."),
    ("Two-phase commit's big weakness", "If the coordinator dies after 'prepare', participants can block holding locks until it recovers."),
    ("What is the outbox pattern?", "Write the event to an outbox table in the same transaction as the data, then a relay publishes it. No dual-write inconsistency."),
    # Kafka & messaging
    ("Kafka: what is the unit of parallelism and ordering?", "The partition. Order is guaranteed only within a partition.", ["The topic", "The broker", "The consumer group"]),
    ("How does Kafka decide a message's partition?", "Hash of the message key (same key → same partition), or round-robin/sticky if there's no key."),
    ("Consumer group rule: max useful consumers for a topic with 6 partitions?", "6. Each partition goes to one consumer in the group; extras sit idle.", ["3", "12", "Unlimited"]),
    ("What is a consumer offset?", "The position of the next message to read in a partition, committed so a consumer can resume after a restart."),
    ("acks=all in a Kafka producer means…", "The leader waits for all in-sync replicas to have the write before acknowledging."),
    ("What does min.insync.replicas protect against?", "With acks=all, writes fail unless at least that many replicas are in sync, preventing silent data loss."),
    ("At-least-once vs. at-most-once delivery", "At-least-once commits offsets after processing (may duplicate). At-most-once commits before (may lose)."),
    ("How do you get effectively-once processing with at-least-once delivery?", "Make the consumer idempotent, e.g. dedupe by message ID or use upserts. Kafka also has idempotent producers and transactions."),
    ("What is log compaction in Kafka?", "Kafka keeps at least the latest value for each key and drops older ones, turning a topic into a durable changelog."),
    ("Kafka vs. a traditional queue (e.g. RabbitMQ): biggest model difference", "Kafka is a replayable log that consumers read at their own offsets. A queue removes messages once acknowledged."),
    ("What is a dead-letter queue?", "Where messages that repeatedly fail processing go, so they don't block the rest."),
    ("What is backpressure?", "A slow consumer signals producers to slow down (or buffers fill), instead of being overwhelmed."),
    # General
    ("Rate limiting: token bucket in one line", "Tokens refill at a steady rate up to a cap; each request spends one, which allows short bursts."),
    ("Why use exponential backoff with jitter on retries?", "Spreads retries out so many clients don't retry in lockstep and swamp a recovering service."),
    ("What is a circuit breaker?", "After repeated failures it stops calling a dependency for a while and fails fast, then tests with a few calls before closing."),
    ("What is an idempotency key?", "A client-supplied ID so a retried request (like a payment) is applied only once."),
    ("p99 latency means…", "99% of requests are faster than this. It reflects the slowest 1% that averages hide."),
]

# ───────────────────────── Geography ─────────────────────────
CONTINENTS = ["Africa", "Asia", "Europe", "North America", "South America", "Oceania"]
COUNTRY_CONTINENT = [
    ("Suriname", "South America"), ("Guyana", "South America"), ("Paraguay", "South America"), ("Uruguay", "South America"),
    ("Belize", "North America"), ("Honduras", "North America"), ("Nicaragua", "North America"), ("Barbados", "North America"),
    ("Dominica", "North America"), ("Grenada", "North America"), ("Panama", "North America"),
    ("Bhutan", "Asia"), ("Laos", "Asia"), ("Brunei", "Asia"), ("Maldives", "Asia"), ("Kyrgyzstan", "Asia"),
    ("Tajikistan", "Asia"), ("Turkmenistan", "Asia"), ("Bahrain", "Asia"), ("Oman", "Asia"), ("Jordan", "Asia"),
    ("Eritrea", "Africa"), ("Djibouti", "Africa"), ("Burkina Faso", "Africa"), ("Benin", "Africa"), ("Togo", "Africa"),
    ("Lesotho", "Africa"), ("Eswatini", "Africa"), ("Malawi", "Africa"), ("Mauritius", "Africa"), ("Seychelles", "Africa"),
    ("Comoros", "Africa"), ("Cape Verde", "Africa"), ("São Tomé and Príncipe", "Africa"), ("Gabon", "Africa"),
    ("Moldova", "Europe"), ("North Macedonia", "Europe"), ("Montenegro", "Europe"), ("Liechtenstein", "Europe"),
    ("Andorra", "Europe"), ("San Marino", "Europe"), ("Slovenia", "Europe"), ("Latvia", "Europe"),
    ("Vanuatu", "Oceania"), ("Palau", "Oceania"), ("Nauru", "Oceania"), ("Kiribati", "Oceania"), ("Tonga", "Oceania"), ("Samoa", "Oceania"),
]
# Distractors: nearby or commonly confused continents first
CONFUSE = {
    "South America": ["North America", "Africa", "Oceania"], "North America": ["South America", "Europe", "Oceania"],
    "Asia": ["Africa", "Oceania", "Europe"], "Africa": ["Asia", "Oceania", "South America"],
    "Europe": ["Asia", "Africa", "North America"], "Oceania": ["Asia", "Africa", "South America"],
}
GEO_CONTINENT = [{"front": f"Which continent is {c} in?", "back": k, "kind": "choice", "choices": CONFUSE[k]} for c, k in COUNTRY_CONTINENT]

# (country, lat, lng, zoom, [three nearby countries as wrong answers])
MAP = [
    ("Mongolia", 46.9, 103.8, 3.5, ["Kazakhstan", "China", "Russia"]),
    ("Kazakhstan", 48.0, 67.0, 3.5, ["Uzbekistan", "Mongolia", "Turkmenistan"]),
    ("Uzbekistan", 41.4, 64.6, 4.5, ["Turkmenistan", "Kazakhstan", "Kyrgyzstan"]),
    ("Afghanistan", 33.9, 67.7, 4.5, ["Pakistan", "Iran", "Tajikistan"]),
    ("Iran", 32.4, 53.7, 4, ["Iraq", "Afghanistan", "Pakistan"]),
    ("Pakistan", 30.4, 69.3, 4, ["Afghanistan", "India", "Iran"]),
    ("Myanmar", 21.9, 95.9, 4.5, ["Thailand", "Bangladesh", "Laos"]),
    ("Laos", 19.9, 102.5, 5, ["Vietnam", "Thailand", "Cambodia"]),
    ("Cambodia", 12.6, 104.9, 5, ["Laos", "Thailand", "Vietnam"]),
    ("Thailand", 15.9, 100.9, 4.5, ["Myanmar", "Cambodia", "Laos"]),
    ("Saudi Arabia", 23.9, 45.1, 4, ["Yemen", "Oman", "Iraq"]),
    ("Turkey", 39.0, 35.2, 4.5, ["Greece", "Syria", "Georgia"]),
    ("Ukraine", 48.4, 31.2, 4.5, ["Belarus", "Poland", "Romania"]),
    ("Poland", 52.0, 19.1, 5, ["Germany", "Czechia", "Belarus"]),
    ("Romania", 45.9, 25.0, 5, ["Bulgaria", "Hungary", "Moldova"]),
    ("Hungary", 47.2, 19.5, 5.5, ["Austria", "Slovakia", "Romania"]),
    ("Austria", 47.5, 14.6, 5.5, ["Switzerland", "Czechia", "Hungary"]),
    ("Serbia", 44.0, 20.9, 5.5, ["Croatia", "Bulgaria", "Bosnia and Herzegovina"]),
    ("Portugal", 39.6, -8.0, 5, ["Spain", "Morocco", "Andorra"]),
    ("Finland", 64.0, 26.0, 4, ["Sweden", "Norway", "Estonia"]),
    ("Algeria", 28.0, 2.6, 3.5, ["Libya", "Morocco", "Mali"]),
    ("Libya", 27.0, 17.0, 3.5, ["Egypt", "Algeria", "Chad"]),
    ("Mali", 17.6, -4.0, 4, ["Niger", "Mauritania", "Burkina Faso"]),
    ("Niger", 17.6, 8.1, 4, ["Chad", "Mali", "Nigeria"]),
    ("Chad", 15.5, 18.7, 4, ["Niger", "Sudan", "Central African Republic"]),
    ("Sudan", 15.5, 30.0, 4, ["South Sudan", "Chad", "Egypt"]),
    ("Ethiopia", 9.1, 40.5, 4, ["Somalia", "Kenya", "South Sudan"]),
    ("Kenya", 0.2, 37.9, 4.5, ["Tanzania", "Uganda", "Ethiopia"]),
    ("Tanzania", -6.4, 34.9, 4.5, ["Kenya", "Mozambique", "Zambia"]),
    ("Angola", -12.3, 17.5, 4, ["Zambia", "Namibia", "DR Congo"]),
    ("Namibia", -22.6, 17.1, 4, ["Botswana", "Angola", "South Africa"]),
    ("Botswana", -22.3, 24.7, 4.5, ["Namibia", "Zimbabwe", "South Africa"]),
    ("Madagascar", -19.4, 46.7, 4, ["Mozambique", "Mauritius", "Comoros"]),
    ("Bolivia", -16.5, -64.7, 4, ["Paraguay", "Peru", "Brazil"]),
    ("Paraguay", -23.4, -58.4, 4.5, ["Bolivia", "Uruguay", "Argentina"]),
    ("Ecuador", -1.5, -78.2, 5, ["Colombia", "Peru", "Venezuela"]),
    ("Peru", -9.2, -75.0, 4, ["Bolivia", "Ecuador", "Colombia"]),
    ("Guyana", 5.0, -58.9, 5, ["Suriname", "Venezuela", "Brazil"]),
    ("Honduras", 14.8, -86.6, 5.5, ["Nicaragua", "Guatemala", "El Salvador"]),
    ("Nicaragua", 12.9, -85.2, 5.5, ["Honduras", "Costa Rica", "Panama"]),
    ("Papua New Guinea", -6.3, 144.0, 4.5, ["Indonesia", "Solomon Islands", "Australia"]),
]
GEO_MAP = [{"front": "Which country is marked?", "back": c, "kind": "map", "choices": w, "lat": lat, "lng": lng, "zoom": z}
           for c, lat, lng, z, w in MAP]

# ───────────────────────── Minecraft (Java 1.21) ─────────────────────────
MINECRAFT = [
    ("How many game ticks are in one redstone tick?", "2 (a redstone tick is 0.1 s)", ["1", "4", "20"]),
    ("How long is a full Minecraft day in game ticks?", "24,000 ticks (20 minutes)"),
    ("How fast does a hopper move items?", "2.5 items per second (one every 8 game ticks)", ["1 item per second", "5 items per second", "8 items per second"]),
    ("How many items must a hopper hold for a comparator to output signal 15?", "320 (all 5 slots full of 64-stacks)"),
    ("Maximum number of blocks a piston can push", "12", ["8", "16", "24"]),
    ("Which block does NOT stick to slime or honey blocks, but can still be pushed?", "Glazed terracotta", ["Obsidian", "Observer", "Redstone block"]),
    ("Do slime blocks and honey blocks stick to each other?", "No. That's why they're used side by side in flying machines."),
    ("Which block under a note block gives the 'bell' (glockenspiel) sound?", "Gold block", ["Packed ice", "Clay", "Bone block"]),
    ("Note block on packed ice plays…", "Chimes", ["Flute", "Bell", "Xylophone"]),
    ("Note block on a bone block plays…", "Xylophone", ["Chimes", "Bass drum", "Guitar"]),
    ("What does a copper bulb do with each redstone pulse?", "Toggles its light on or off, like a T flip-flop."),
    ("Horizontal travel ratio between the Nether and the Overworld", "1 block in the Nether = 8 in the Overworld"),
    ("Overworld build range since 1.18", "y = -64 to y = 319"),
    ("At what y-level is ancient debris most commonly mined?", "Around y = 15", ["y = -59", "y = 64", "y = 110"]),
    ("Netherite ingot recipe", "4 netherite scrap + 4 gold ingots"),
    ("Where do netherite upgrade smithing templates generate?", "Bastion remnants", ["Nether fortresses", "Ancient cities", "End cities"]),
    ("Max light level at which hostile mobs spawn on blocks (1.18+)", "Block light 0", ["7", "3", "11"]),
    ("Default randomTickSpeed", "3", ["1", "10", "20"]),
    ("Overworld hostile mob cap (Java)", "70", ["50", "100", "120"]),
    ("Beyond what distance do most hostile mobs despawn instantly?", "128 blocks", ["64 blocks", "96 blocks", "256 blocks"]),
    ("How close must a player be for a monster spawner to activate?", "16 blocks", ["8 blocks", "32 blocks", "64 blocks"]),
    ("How long until a dropped item despawns?", "5 minutes (6,000 ticks) in loaded chunks"),
    ("After how long without sleeping can phantoms spawn?", "3 in-game days (72,000 ticks)"),
    ("Blocks needed for a full 4-level beacon pyramid", "164", ["120", "128", "196"]),
    ("Blocks needed to fully power a conduit", "42 prismarine-family blocks (16 is the minimum)"),
    ("Items smelted by one lava bucket", "100", ["64", "80", "200"]),
    ("Items smelted by one blaze rod", "12", ["8", "16", "20"]),
    ("Items smelted by one dried kelp block", "20", ["8", "12", "16"]),
    ("Which mob is the only source of tridents?", "Drowned (only ones that spawn holding a trident can drop it)"),
    ("Which trident enchantment can't be combined with Loyalty or Channeling?", "Riptide", ["Impaling", "Mending", "Unbreaking"]),
    ("Which two bow enchantments are mutually exclusive?", "Infinity and Mending", ["Power and Punch", "Flame and Infinity", "Punch and Mending"]),
    ("What makes a shulker duplicate (1.17+)?", "Another shulker's bullet hitting a shulker, if there's room nearby"),
    ("Which block can't be obtained in survival even with Silk Touch?", "Budding amethyst", ["Sculk shrieker", "Sculk catalyst", "Small amethyst bud"]),
    ("Reinforced deepslate generates where?", "Around the portal frame in ancient cities (unobtainable in survival)"),
    ("Health of the Warden", "500 HP (250 hearts)", ["300 HP", "200 HP", "1,024 HP"]),
    ("Recovery compass recipe", "8 echo shards around a compass"),
    ("Chance of a blue axolotl when breeding", "1 in 1,200", ["1 in 100", "1 in 500", "1 in 10,000"]),
    ("How do you turn a red mooshroom brown?", "It has to be struck by lightning"),
    ("A temperate (orange) frog eating a small magma cube drops…", "Ochre froglight", ["Verdant froglight", "Pearlescent froglight", "Nothing"]),
    ("Mace recipe", "Heavy core + breeze rod"),
    ("How many wind charges does one breeze rod craft?", "4", ["1", "2", "8"]),
    ("Which enchantment is exclusive to the mace?", "Breach (also Density and Wind Burst)", ["Impaling", "Sweeping Edge", "Piercing"]),
    ("What item opens an ominous vault?", "An ominous trial key", ["A trial key", "A heavy core", "A breeze rod"]),
    ("Max anvil cost before 'Too Expensive!' in survival", "39 levels (40+ is too expensive)"),
]

# ───────────────────────── Tolkien ─────────────────────────
TOLKIEN = [
    ("Gandalf's name in Valinor, as a Maia", "Olórin", ["Mithrandir", "Curumo", "Aiwendil"]),
    ("Sauron's original name", "Mairon", ["Gorthaur", "Annatar", "Melkor"]),
    ("Name Sauron used in disguise to teach the Elven-smiths of Eregion", "Annatar (Lord of Gifts)", ["Mairon", "Thû", "Zigûr"]),
    ("Saruman's original Maia name", "Curumo (Curunír among the Elves)"),
    ("Radagast's original Maia name", "Aiwendil", ["Olórin", "Pallando", "Alatar"]),
    ("The two Blue Wizards", "Alatar and Pallando"),
    ("Who made the Silmarils?", "Fëanor", ["Celebrimbor", "Fingolfin", "Aulë"]),
    ("The Two Trees of Valinor", "Telperion (silver) and Laurelin (gold)"),
    ("Who destroyed the Two Trees?", "Melkor and Ungoliant"),
    ("Which Vala sang the Two Trees into being?", "Yavanna", ["Varda", "Manwë", "Nienna"]),
    ("Varda's name among the Elves of Middle-earth", "Elbereth (Gilthoniel)"),
    ("Vala of the waters", "Ulmo", ["Ossë", "Manwë", "Aulë"]),
    ("Who created the Dwarves?", "Aulë", ["Yavanna", "Eru", "Manwë"]),
    ("Kinslaying at Alqualondë: who attacked whom?", "Fëanor's Noldor attacked the Teleri to take their ships"),
    ("How many sons did Fëanor have?", "Seven", ["Five", "Three", "Nine"]),
    ("Morgoth's great fortress in the north", "Angband, beneath Thangorodrim"),
    ("Who fought Morgoth in single combat and wounded him seven times?", "Fingolfin", ["Fingon", "Maedhros", "Turgon"]),
    ("Dagor Bragollach means…", "The Battle of Sudden Flame", ["The Battle of Unnumbered Tears", "The War of Wrath", "The Glorious Battle"]),
    ("Nirnaeth Arnoediad means…", "The Battle of Unnumbered Tears"),
    ("Kingdom of Thingol and Melian", "Doriath, protected by the Girdle of Melian"),
    ("Lúthien's mother", "Melian the Maia", ["Galadriel", "Idril", "Nienna"]),
    ("Who cut a Silmaril from Morgoth's crown?", "Beren (with Lúthien)"),
    ("The wolf that bit off Beren's hand", "Carcharoth", ["Draugluin", "Huan", "Glaurung"]),
    ("The Hound of Valinor who aided Beren and Lúthien", "Huan", ["Carcharoth", "Draugluin", "Garm"]),
    ("Who founded Nargothrond?", "Finrod Felagund", ["Turgon", "Thingol", "Orodreth"]),
    ("Who founded Gondolin?", "Turgon", ["Finrod", "Fingolfin", "Ecthelion"]),
    ("Who betrayed Gondolin?", "Maeglin", ["Saeros", "Mîm", "Eöl"]),
    ("Which Elf of Gondolin killed Gothmog, Lord of Balrogs?", "Ecthelion of the Fountain", ["Glorfindel", "Tuor", "Fingon"]),
    ("Glorfindel died killing a Balrog during the fall of which city?", "Gondolin"),
    ("Túrin's black sword", "Gurthang (Anglachel reforged)", ["Narsil", "Aranrúth", "Anguirel"]),
    ("The dragon Túrin slew", "Glaurung", ["Ancalagon", "Smaug", "Scatha"]),
    ("Greatest of the winged dragons, slain by Eärendil", "Ancalagon the Black"),
    ("Eärendil's ship", "Vingilótë", ["Alcarondas", "Eärrámë", "Lindar"]),
    ("Elrond's twin brother, first King of Númenor", "Elros (Tar-Minyatur)"),
    ("Last king of Númenor, who sailed against Valinor", "Ar-Pharazôn", ["Tar-Palantir", "Ar-Adûnakhôr", "Elendil"]),
    ("Elendil's sons", "Isildur and Anárion"),
    ("Gil-galad's spear", "Aeglos", ["Narsil", "Anglachel", "Dramborleg"]),
    ("Who forged the Three Elven Rings?", "Celebrimbor", ["Fëanor", "Sauron", "Telchar"]),
    ("Who gave Narya, the Ring of Fire, to Gandalf?", "Círdan the Shipwright", ["Elrond", "Galadriel", "Celebrimbor"]),
    ("Which Elven ring did Galadriel bear?", "Nenya (Ring of Water)", ["Vilya", "Narya", "None"]),
    ("Which Elven ring did Elrond bear?", "Vilya (Ring of Air)", ["Nenya", "Narya", "None"]),
    ("Aragorn's name when serving Gondor and Rohan in disguise", "Thorongil", ["Strider", "Estel", "Elessar"]),
    ("Aragorn's childhood name in Rivendell", "Estel ('hope')"),
    ("Arwen's mother", "Celebrían", ["Galadriel", "Idril", "Nimrodel"]),
    ("Who found the One Ring in the Anduin?", "Déagol", ["Sméagol", "Isildur", "Bilbo"]),
    ("The three kinds of Hobbits", "Harfoots, Stoors and Fallohides"),
    ("The tree that trapped Merry and Pippin in the Old Forest", "Old Man Willow"),
    ("Who killed Saruman?", "Gríma Wormtongue", ["Legolas", "Treebeard", "Frodo"]),
    ("Whose blade, taken from the Barrow-downs, let Éowyn's stroke kill the Witch-king?", "Merry's"),
    ("Théoden's horse", "Snowmane", ["Hasufel", "Arod", "Firefoot"]),
    ("Mount Doom's two Sindarin names", "Orodruin and Amon Amarth"),
]

# ───────────────────────── Bike repair ─────────────────────────
BIKE = [
    ("First thing to do when you flat during a race", "Pull safely off the course, out of the way of other riders."),
    ("Before removing the rear wheel, shift into…", "The smallest rear cog (hardest gear), so the wheel drops out easily.", ["The biggest rear cog", "The small chainring", "It doesn't matter"]),
    ("Rim brakes: what to open before removing the wheel?", "The brake caliper's quick-release, so the tire clears the pads."),
    ("Disc brakes: what not to do with the wheel out?", "Don't squeeze the brake lever, or the pads close together. Use a pad spacer if you have one."),
    ("Essential race flat kit", "Spare tube, 2 tire levers, CO2 inflator + cartridge(s) or a mini pump, a tire boot, and a multitool."),
    ("Why carry a tire boot?", "To cover a sidewall or tread cut so the new tube doesn't bulge out and burst."),
    ("Emergency tire boot if you have none", "A folded dollar bill, gel wrapper, or energy-bar wrapper."),
    ("Before installing the new tube, always…", "Run your fingers carefully around the inside of the tire to find and remove whatever caused the flat."),
    ("Why add a little air to the new tube before installing it?", "It gives the tube shape so it doesn't twist or get pinched under the tire bead."),
    ("Seating the bead: where do you start, and where do you finish?", "Start at the valve, work both ways around, and finish opposite it. Then push the valve up into the tire so the tube isn't trapped under the bead."),
    ("Trick for the last, tight section of bead", "Push the already-seated bead into the rim's center channel all the way around to gain slack."),
    ("Why avoid tire levers for the last bit of bead?", "Levers can pinch and puncture the new tube."),
    ("After seating the tire, before full inflation, check…", "That no tube is caught under the bead and the bead line sits evenly all around."),
    ("Presta valve: what to do before inflating?", "Unscrew the small knurled nut at the tip (and tap it to free it)."),
    ("Deep-section race wheels need what from your spare tube?", "A long enough valve stem (e.g. 60–80 mm), or a valve extender."),
    ("Why hold a CO2 cartridge with a sleeve or gloves?", "It gets extremely cold as it discharges and can freeze-burn your fingers."),
    ("After the race, what should you do with a CO2-filled tube?", "Let it out and refill with air. CO2 seeps through butyl tubes quickly."),
    ("Roughly how much does one 16 g CO2 cartridge inflate?", "About one road tire to riding pressure. Carry two in case the first one gets wasted.", ["Half a road tire", "Three road tires", "A mountain bike tire to 100 psi"]),
    ("Two parallel slits in a tube ('snakebite') mean…", "A pinch flat: the tube got pinched against the rim, usually from low pressure.", ["Glass puncture", "Valve failure", "Rim tape failure"]),
    ("A puncture on the inner side of the tube (rim side) usually means…", "Rim tape problem: a spoke hole or sharp edge is exposed.", ["Glass in the tread", "Pinch flat", "Over-inflation"]),
    ("Tubeless: what fixes a hole too big for sealant?", "A tire plug (a 'bacon strip'), then re-inflate."),
    ("Tubeless sealant hissing: how to help it seal?", "Spin the wheel so the hole is at the bottom, letting sealant pool over it, then ride a bit."),
    ("Tubeless tire won't hold after plugging: last resort?", "Remove the valve and install a spare inner tube."),
    ("Reinstalling the rear wheel: where should the chain go?", "Over the smallest rear cog, then drop the wheel into the dropouts."),
    ("Quick-release skewer: correct closing technique", "Close the lever firmly enough to leave an imprint in your palm. Don't spin it like a wing nut."),
    ("Last check before riding off after a rim-brake wheel change", "Close the brake caliper quick-release again and test both brakes."),
    ("Chain dropped off the small chainring: fastest fix?", "Shift to the small ring, push the rear derailleur cage forward for slack, and lift the chain on by hand."),
    ("Why line the tire logo up with the valve?", "So the hole in a tube points you to the matching spot on the tire to search for the cause."),
    ("Patch or replace the tube mid-race?", "Replace. It's much faster. Patch the old tube later.", ["Patch", "Use sealant in a butyl tube", "Ride flat to the finish"]),
    ("Triathlon rule on mechanical help", "Usually no outside assistance: you fix it yourself. Check your race's rules."),
    ("Best way to get fast at flat changes", "Practice at home and time yourself, with the exact kit you race with."),
    ("Where should spare-tube kit go on a tri bike?", "Somewhere you can reach with the bike upside down, e.g. a saddle pack or behind-seat mount, secured so it can't eject."),
    ("Tire direction arrows: does it matter?", "Yes, on many tires. Follow the arrow for intended grip and wear."),
    ("Rolling but rubbing after reinstalling the wheel?", "The wheel probably isn't fully seated in the dropouts. Reopen, push it fully in, then close."),
]

DECKS = [
    ("system-design", "System design: caching, hashing, databases, Kafka", "How the pieces of backend systems work and trade off.", [card(t) for t in SYSTEM_DESIGN]),
    ("geo-continents", "Geography: which continent?", "Multiple choice. Countries people often get wrong.", GEO_CONTINENT),
    ("geo-map", "Geography: name the country on the map", "Multiple choice with a map. Each country's neighbors are the wrong answers.", GEO_MAP),
    ("minecraft", "Minecraft: niche mechanics (Java 1.21)", "Redstone timings, spawning rules, rare items and recipes.", [card(t) for t in MINECRAFT]),
    ("tolkien", "Tolkien: Lord of the Rings & the Silmarillion", "From the First Age to the War of the Ring.", [card(t) for t in TOLKIEN]),
    ("bike-flats", "Bike repair: fixing a flat in a race", "Road and tri bikes: tubes, tubeless, CO2, and getting back on fast.", [card(t) for t in BIKE]),
]

os.makedirs(ROOT, exist_ok=True)
index = []
for slug, name, desc, cards in DECKS:
    for c in cards:
        if c["kind"] != "basic":
            assert len(c["choices"]) == 3 and c["back"] not in c["choices"], (slug, c)
    with open(os.path.join(ROOT, slug + ".json"), "w") as f:
        json.dump({"id": slug, "name": name, "description": desc, "cards": cards}, f, ensure_ascii=False, indent=1)
    index.append({"id": slug, "name": name, "description": desc, "count": len(cards)})
with open(os.path.join(ROOT, "index.json"), "w") as f:
    json.dump(index, f, ensure_ascii=False, indent=1)
for d in index:
    print(f"{d['count']:3d}  {d['name']}")
