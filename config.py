"""Shared config across producer, consumer, dashboard."""
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "marathon-events"
NUM_PARTITIONS = 3
DB_PATH = "marathon.db"

RACE_ID = "boston_2015"
RACE_START_EPOCH = 1_700_000_000  # synthetic race start (any fixed epoch)

# Replay speed multiplier. 600 = 1 hour of race in 6 seconds.
# Set to 1 for real-time, 60 for demo, 600 for fast dev loops.
SPEEDUP = 600

# Map dataset split column names -> kilometer marks
CHECKPOINTS = {
    "5K": 5.0, "10K": 10.0, "15K": 15.0, "20K": 20.0,
    "Half": 21.0975, "25K": 25.0, "30K": 30.0,
    "35K": 35.0, "40K": 40.0,
}
SEGMENT_ORDER = ["5K", "10K", "15K", "20K", "Half", "25K", "30K", "35K", "40K"]
