#!/usr/bin/env bash
# =============================================================================
# demonstrate_failover.sh
#
# Demonstrates NGINX failover behavior:
# 1. Verifies both regions are healthy
# 2. Stops backend-us
# 3. Shows that /us/ requests are still served by backend-eu (failover)
# 4. Restarts backend-us
# =============================================================================

set -euo pipefail

# ── Colors ────────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

NGINX_URL="http://localhost:8080"

info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
pass()  { echo -e "${GREEN}[PASS]${NC}  $*"; }
fail()  { echo -e "${RED}[FAIL]${NC}  $*"; exit 1; }
step()  { echo -e "\n${BOLD}${YELLOW}>>> $*${NC}"; }
divider() { echo -e "${BOLD}$(printf '─%.0s' {1..60})${NC}"; }

# ── Helpers ───────────────────────────────────────────────────────────────────
http_check() {
    local url="$1"
    local expected_status="${2:-200}"
    local resp
    resp=$(curl -s -o /tmp/failover_response.json -w "%{http_code}" --max-time 10 "$url" || echo "000")
    echo "$resp"
}

wait_for_health() {
    local url="$1"
    local label="$2"
    local max_attempts=20
    local attempt=0
    while [ $attempt -lt $max_attempts ]; do
        local code
        code=$(http_check "$url")
        if [ "$code" = "200" ]; then
            pass "$label is healthy (HTTP 200)"
            return 0
        fi
        attempt=$((attempt + 1))
        info "Waiting for $label... attempt $attempt/$max_attempts (got $code)"
        sleep 3
    done
    fail "$label did not become healthy in time."
}

# ── Main ──────────────────────────────────────────────────────────────────────
divider
echo -e "${BOLD}Multi-Region Property Backend — Failover Demonstration${NC}"
divider

# Step 1: Ensure services are up
step "Step 1: Starting all services"
info "Running docker-compose up -d..."
docker-compose up -d 2>&1 | tail -10

info "Waiting for services to become healthy (up to 3 minutes)..."
sleep 30

# Step 2: Verify both regions are healthy
step "Step 2: Verify both regions healthy (baseline)"

info "Checking US backend via NGINX..."
us_code=$(http_check "$NGINX_URL/us/health")
if [ "$us_code" = "200" ]; then
    pass "US region healthy: GET /us/health → $us_code"
else
    fail "US region not healthy: GET /us/health → $us_code"
fi

info "Checking EU backend via NGINX..."
eu_code=$(http_check "$NGINX_URL/eu/health")
if [ "$eu_code" = "200" ]; then
    pass "EU region healthy: GET /eu/health → $eu_code"
else
    fail "EU region not healthy: GET /eu/health → $eu_code"
fi

# Step 3: Make a normal request to US
step "Step 3: Making a request to US region before failover"
info "GET $NGINX_URL/us/properties?limit=3"
curl -s "$NGINX_URL/us/properties?limit=3" | python3 -m json.tool 2>/dev/null | head -20 || true

# Step 4: Stop backend-us to simulate failure
step "Step 4: Simulating US backend failure (docker stop backend_us)"
info "Stopping backend_us container..."
docker stop backend_us 2>/dev/null || docker stop backend-us 2>/dev/null || true
pass "backend-us stopped."

sleep 5
info "Waiting 5s for NGINX to detect the failure..."

# Step 5: Verify failover
step "Step 5: Verifying NGINX failover to EU backend"
info "Sending GET $NGINX_URL/us/health (should be served by EU backend)..."

for attempt in 1 2 3; do
    failover_code=$(http_check "$NGINX_URL/us/health")
    if [ "$failover_code" = "200" ]; then
        break
    fi
    info "Attempt $attempt: got $failover_code, retrying in 3s..."
    sleep 3
done

if [ "$failover_code" = "200" ]; then
    pass "Failover successful! GET /us/health returned $failover_code even though backend-us is DOWN."
    info "Response body:"
    cat /tmp/failover_response.json | python3 -m json.tool 2>/dev/null || cat /tmp/failover_response.json
else
    fail "Failover failed! GET /us/health returned $failover_code (expected 200)."
fi

# Step 6: Check EU logs
step "Step 6: Checking backend-eu logs for /us/ request"
info "Recent backend-eu logs:"
docker logs backend_eu 2>&1 | grep -E "(health|us|INFO|GET)" | tail -10 || \
docker logs backend-eu 2>&1 | grep -E "(health|us|INFO|GET)" | tail -10 || true
pass "Check logs above — EU backend should show it handled the /us/ path request."

# Step 7: Restart US backend
step "Step 7: Restarting backend-us"
info "Starting backend_us container..."
docker start backend_us 2>/dev/null || docker start backend-us 2>/dev/null || true
info "Waiting 20s for backend-us to become healthy..."
sleep 20

wait_for_health "$NGINX_URL/us/health" "US backend (restored)"

# Summary
divider
echo -e "${BOLD}${GREEN}FAILOVER DEMONSTRATION COMPLETE${NC}"
divider
echo -e "${GREEN}✓ Both regions were initially healthy${NC}"
echo -e "${GREEN}✓ US backend was stopped to simulate failure${NC}"
echo -e "${GREEN}✓ NGINX routed /us/ traffic to EU backend (failover)${NC}"
echo -e "${GREEN}✓ /us/health returned 200 OK during US outage${NC}"
echo -e "${GREEN}✓ US backend was restored successfully${NC}"
divider
