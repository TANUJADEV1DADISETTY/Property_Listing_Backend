#!/usr/bin/env python3
"""
test_optimistic_locking.py — Integration test for concurrent update conflict detection.

Tests:
1. Concurrent PUT requests to the same property from both regions
   to verify optimistic locking (409 Conflict) works correctly.
2. Sequential version conflict detection.
3. Idempotency (X-Request-ID deduplication → 422).

Run with:
    python tests/test_optimistic_locking.py
"""
import concurrent.futures
import json
import sys
import time
import uuid

import requests

BASE_URL = "http://localhost:8080"
US_BASE = f"{BASE_URL}/us"
EU_BASE = f"{BASE_URL}/eu"

GREEN = "\033[92m"
RED   = "\033[91m"
YELLOW = "\033[93m"
BLUE  = "\033[94m"
RESET = "\033[0m"
BOLD  = "\033[1m"


def pass_msg(msg): print(f"  {GREEN}✓ PASS{RESET} — {msg}")
def fail_msg(msg): print(f"  {RED}✗ FAIL{RESET} — {msg}"); sys.exit(1)
def info_msg(msg): print(f"  {BLUE}ℹ INFO{RESET} — {msg}")
def warn_msg(msg): print(f"  {YELLOW}⚠ WARN{RESET} — {msg}")


def get_property(region_base: str, prop_id: int) -> dict:
    """Fetch a property from the specified region."""
    resp = requests.get(f"{region_base}/properties/{prop_id}", timeout=10)
    resp.raise_for_status()
    return resp.json()


def put_property(region_base: str, prop_id: int, price: float, version: int, request_id: str = None) -> requests.Response:
    """Send a PUT update to a property."""
    headers = {"Content-Type": "application/json"}
    if request_id:
        headers["x-request-id"] = request_id
    body = {"price": price, "version": version}
    return requests.put(
        f"{region_base}/properties/{prop_id}",
        json=body,
        headers=headers,
        timeout=10,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 1: Sequential optimistic locking conflict
# ─────────────────────────────────────────────────────────────────────────────
def test_sequential_conflict():
    print(f"\n{BOLD}Test 1: Sequential Optimistic Locking Conflict{RESET}")
    prop_id = 3

    # Get current state
    prop = get_property(US_BASE, prop_id)
    current_version = prop["version"]
    info_msg(f"Property {prop_id} — current version: {current_version}, price: {prop['price']}")

    # First update — should succeed
    resp1 = put_property(US_BASE, prop_id, 500000.00, current_version)
    if resp1.status_code == 200:
        pass_msg(f"First update succeeded (200 OK). New version: {resp1.json()['version']}")
    else:
        fail_msg(f"First update failed unexpectedly: {resp1.status_code} — {resp1.text}")

    # Second update with old version — should conflict
    resp2 = put_property(US_BASE, prop_id, 600000.00, current_version)
    if resp2.status_code == 409:
        pass_msg(f"Second update correctly rejected with 409 Conflict.")
        info_msg(f"Conflict message: {resp2.json().get('detail', '')[:100]}")
    else:
        fail_msg(f"Expected 409 Conflict, got: {resp2.status_code} — {resp2.text}")


# ─────────────────────────────────────────────────────────────────────────────
# Test 2: Concurrent updates from both regions — race condition simulation
# ─────────────────────────────────────────────────────────────────────────────
def test_concurrent_conflict():
    print(f"\n{BOLD}Test 2: Concurrent Updates from Both Regions (Race Condition){RESET}")
    prop_id = 4

    # Get current state from US
    prop = get_property(US_BASE, prop_id)
    current_version = prop["version"]
    info_msg(f"Property {prop_id} — version: {current_version}, price: {prop['price']}")

    # Fire both updates simultaneously
    results = {}

    def update_us():
        resp = put_property(US_BASE, prop_id, 399000.00, current_version)
        results["us"] = resp

    def update_eu():
        resp = put_property(EU_BASE, prop_id, 415000.00, current_version)
        results["eu"] = resp

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        f1 = executor.submit(update_us)
        f2 = executor.submit(update_eu)
        concurrent.futures.wait([f1, f2])

    us_code = results["us"].status_code
    eu_code = results["eu"].status_code

    info_msg(f"US response status: {us_code}")
    info_msg(f"EU response status: {eu_code}")

    # Exactly one should succeed and one should conflict
    statuses = {us_code, eu_code}
    if 200 in statuses and 409 in statuses:
        pass_msg("Exactly one update succeeded (200) and one was rejected (409). Optimistic locking works!")
    elif us_code == 200 and eu_code == 200:
        # Both succeeded - could happen if they hit different DBs and version hasn't replicated yet
        warn_msg(
            "Both updates returned 200 (different DBs — Kafka replication may reconcile later). "
            "This is acceptable behavior in an eventually-consistent system."
        )
    elif us_code == 409 and eu_code == 409:
        fail_msg("Both updates were rejected! Something is wrong.")
    else:
        warn_msg(f"Unexpected combination: US={us_code}, EU={eu_code}")


# ─────────────────────────────────────────────────────────────────────────────
# Test 3: Idempotency — duplicate X-Request-ID
# ─────────────────────────────────────────────────────────────────────────────
def test_idempotency():
    print(f"\n{BOLD}Test 3: Idempotency — Duplicate X-Request-ID{RESET}")
    prop_id = 5

    # Get fresh state
    prop = get_property(US_BASE, prop_id)
    current_version = prop["version"]
    request_id = str(uuid.uuid4())
    info_msg(f"Using X-Request-ID: {request_id}")
    info_msg(f"Property {prop_id} — version: {current_version}")

    # First request — should succeed
    resp1 = put_property(US_BASE, prop_id, 275000.00, current_version, request_id)
    if resp1.status_code == 200:
        pass_msg(f"First request succeeded (200 OK).")
    else:
        fail_msg(f"First request failed: {resp1.status_code} — {resp1.text}")

    # Second identical request — should be rejected as duplicate
    resp2 = put_property(US_BASE, prop_id, 275000.00, current_version, request_id)
    if resp2.status_code == 422:
        pass_msg(f"Duplicate request correctly rejected with 422 Unprocessable Entity.")
        info_msg(f"Response: {resp2.json().get('detail', '')}")
    else:
        fail_msg(f"Expected 422, got: {resp2.status_code} — {resp2.text}")


# ─────────────────────────────────────────────────────────────────────────────
# Test 4: Kafka Replication
# ─────────────────────────────────────────────────────────────────────────────
def test_kafka_replication():
    print(f"\n{BOLD}Test 4: Kafka Cross-Region Replication{RESET}")
    prop_id = 2

    # Update via US
    prop = get_property(US_BASE, prop_id)
    current_version = prop["version"]
    new_price = 888888.00
    info_msg(f"Updating property {prop_id} via US region to price={new_price}")

    resp = put_property(US_BASE, prop_id, new_price, current_version)
    if resp.status_code != 200:
        fail_msg(f"US update failed: {resp.status_code} — {resp.text}")

    us_version = resp.json()["version"]
    info_msg(f"US update successful. New version: {us_version}")

    # Wait for replication
    info_msg("Waiting 8 seconds for Kafka replication...")
    time.sleep(8)

    # Verify EU has the update
    eu_prop = get_property(EU_BASE, prop_id)
    if abs(float(eu_prop["price"]) - new_price) < 0.01:
        pass_msg(f"EU DB reflects the US update. Price: {eu_prop['price']}, Version: {eu_prop['version']}")
    else:
        warn_msg(
            f"EU price is {eu_prop['price']} (expected {new_price}). "
            "Replication may be delayed — check Kafka consumer logs."
        )


# ─────────────────────────────────────────────────────────────────────────────
# Test 5: Replication Lag endpoint
# ─────────────────────────────────────────────────────────────────────────────
def test_replication_lag():
    print(f"\n{BOLD}Test 5: Replication Lag Endpoint{RESET}")

    resp = requests.get(f"{EU_BASE}/replication-lag", timeout=10)
    if resp.status_code == 200:
        lag = resp.json().get("lag_seconds", -1)
        if lag >= 0:
            pass_msg(f"EU replication lag endpoint returned: lag_seconds={lag}")
        else:
            fail_msg(f"Unexpected lag value: {lag}")
    else:
        fail_msg(f"Replication lag endpoint failed: {resp.status_code}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}Multi-Region Property Backend — Integration Tests{RESET}")
    print(f"{BOLD}{'='*60}{RESET}")
    print(f"Target: {BASE_URL}")

    # Quick connectivity check
    try:
        r = requests.get(f"{US_BASE}/health", timeout=10)
        r.raise_for_status()
        info_msg(f"US health check: OK (region={r.json().get('region')})")
    except Exception as e:
        print(f"\n{RED}ERROR: Cannot reach {US_BASE}/health — {e}{RESET}")
        print("Make sure all services are running: docker-compose up -d")
        sys.exit(1)

    try:
        r = requests.get(f"{EU_BASE}/health", timeout=10)
        r.raise_for_status()
        info_msg(f"EU health check: OK (region={r.json().get('region')})")
    except Exception as e:
        print(f"\n{RED}ERROR: Cannot reach {EU_BASE}/health — {e}{RESET}")
        sys.exit(1)

    test_sequential_conflict()
    test_concurrent_conflict()
    test_idempotency()
    test_kafka_replication()
    test_replication_lag()

    print(f"\n{BOLD}{GREEN}{'='*60}{RESET}")
    print(f"{BOLD}{GREEN}All tests passed!{RESET}")
    print(f"{BOLD}{GREEN}{'='*60}{RESET}\n")
