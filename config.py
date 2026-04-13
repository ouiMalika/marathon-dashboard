"""Configuration constants shared by the producer, consumer, and dashboard."""

# Kafka connection
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "marathon-events"
NUM_PARTITIONS = 3

# Local SQLite database file
DB_PATH = "marathon.db"

# Race identifier and synthetic race-start epoch used for event timestamps
RACE_ID = "boston_2015"
RACE_START_EPOCH = 1_700_000_000

# Replay speed multiplier applied by the producer between events.
#   1   = real time (~4 hours)
#   60  = 1 race-hour per wall-clock minute (~4 minutes total)
#   600 = compressed demo (~25 seconds total)
SPEEDUP = 60

# Map of checkpoint label to distance in kilometers
CHECKPOINTS = {
    "5K": 5.0,
    "10K": 10.0,
    "15K": 15.0,
    "20K": 20.0,
    "Half": 21.0975,
    "25K": 25.0,
    "30K": 30.0,
    "35K": 35.0,
    "40K": 40.0,
}

# Checkpoints listed in the order runners cross them
SEGMENT_ORDER = ["5K", "10K", "15K", "20K", "Half", "25K", "30K", "35K", "40K"]

# Maximum relative difference between first-half and second-span pace
# for a runner to be classified as having an even pace
EVEN_SPLIT_TOLERANCE = 0.01

# Runner cohorts defined by first-5K pace in seconds per kilometer
COHORTS = [
    ("elite",        0,   210),   # under 3:30 / km
    ("competitive",  210, 270),   # 3:30 – 4:30 / km
    ("recreational", 270, 360),   # 4:30 – 6:00 / km
    ("back_of_pack", 360, 9999),  # slower than 6:00 / km
]

# Pace thresholds (seconds per kilometer) used to flag implausible segments.
# Faster than 160 s/km exceeds the men's marathon world-record pace and
# indicates a likely timing error. Slower than 900 s/km (15 min/km) indicates
# a stop, walking break, or non-running event rather than a continuous segment.
ANOMALY_FAST_PACE_SEC_PER_KM = 160
ANOMALY_SLOW_PACE_SEC_PER_KM = 900