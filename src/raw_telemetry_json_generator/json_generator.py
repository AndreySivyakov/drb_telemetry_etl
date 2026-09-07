import json
import random
import time
import uuid
from datetime import datetime, timedelta

# --- Configuration ---
num_events = random.randint(150, 900)

config_path = "/Workspace/Shared/env_config.json"
with open(config_path, "r", encoding="utf-8") as file:
    config = json.load(file)
env_type = config["env_type"]
json_base_path = (
    f"/Volumes/dtbrtelemetry{env_type}"
    "/raw/telemetry_json/incoming"
)  # volume path for JSON files

# --- Reference data ---
app_names = ["A", "B", "C"]
device_types = ["Mobile", "Tablet", "Desktop", "SmartTV", "Wearable"]
geo_areas = ["North America", "Europe", "Asia Pacific", "South America", "Africa", "Middle East"]
os_options = ["iOS", "Android", "Windows", "macOS", "Linux", "ChromeOS"]

# --- Generate timestamps from the previous day ---
today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
yesterday_start = today - timedelta(days=1)

def random_timestamp_yesterday():
    """Return a random timestamp from the previous calendar day."""
    offset_seconds = random.randint(0, 86399)  # 0 to 23:59:59
    return yesterday_start + timedelta(seconds=offset_seconds)

# --- Build events (with 0.5-1% erroneous lines) ---
error_rate = random.uniform(0.005, 0.01)
num_bad = max(1, round(num_events * error_rate))
bad_indices = set(random.sample(range(num_events), num_bad))

events = []
for i in range(num_events):
    app_name = random.choice(app_names)
    action_id = random.randint(1, 250)
    event = {
        "eventId": str(uuid.uuid4()),
        "userId": random.randint(1, 100),
        "event_time": random_timestamp_yesterday().strftime("%Y-%m-%dT%H:%M:%S"),
        "actionId": f"{app_name}_{action_id}",
        "appName": app_name,
        "deviceType": random.choice(device_types),
        "geoArea": random.choice(geo_areas),
        "os": random.choice(os_options),
    }
    # Inject errors: missing userId or faulty date (before 1900)
    if i in bad_indices:
        if random.random() < 0.5:
            del event["userId"]
        else:
            faulty_year = random.randint(1600, 1899)
            event["event_time"] = f"{faulty_year}-{random.randint(1,12):02d}-{random.randint(1,28):02d}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
    events.append(event)

print(f"Generated {len(events)} telemetry events for {yesterday_start.strftime('%Y-%m-%d')} ({num_bad} erroneous, {error_rate:.2%} error rate)")
print("Sample event:", json.dumps(events[0], indent=2))


# --- Clean up JSON files older than 7 days ---
cutoff_ms = (time.time() - 7 * 86400) * 1000
removed = 0
try:
    for f in dbutils.fs.ls(json_base_path):
        if f.name.endswith(".json") and f.modificationTime < cutoff_ms:
            dbutils.fs.rm(f.path)
            print(f"Removed: {f.name}")
            removed += 1
    print(f"Cleanup complete — removed {removed} file(s).")
except Exception:
    print("No existing data — first run.")

# --- Write a single JSON file with a descriptive name ---
file_name = f"events_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
file_path = f"{json_base_path}/{file_name}"

# Write as newline-delimited JSON (one object per line, compatible with Spark JSON reader)
ndjson_content = "\n".join(json.dumps(e) for e in events)
dbutils.fs.put(file_path, ndjson_content, overwrite=False)

print(f"{num_events} events written to: {file_path}")
'''
# --- Clean up silver Delta tables (rows older than 7 full days) ---
SCHEMA = "dbdefaultws.telemetry_medalion"

for tbl in ["eu_telemetry_events", "row_telemetry_events"]:
    fqn = f"{SCHEMA}.{tbl}"
    try:
        spark.sql(f"DELETE FROM {fqn} WHERE event_time < current_date() - INTERVAL 7 DAYS")
        spark.sql(f"VACUUM {fqn}")
        print(f"Cleaned: {tbl}")
    except Exception as e:
        print(f"Skipped {tbl}: {e}")

print("Silver table cleanup complete.")
'''
# --- Verify: list all JSON files ---
json_files = [f for f in dbutils.fs.ls(json_base_path) if f.name.endswith(".json")]
print(f"\nJSON files in directory: {len(json_files)}")
for f in json_files:
    mod_str = datetime.fromtimestamp(f.modificationTime / 1000).strftime("%Y-%m-%d %H:%M:%S")
    print(f"  {f.name}  ({f.size:,} bytes, modified {mod_str})")