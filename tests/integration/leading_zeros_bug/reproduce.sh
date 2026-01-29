#!/bin/bash
#
# Reproduction script for issue #394: Leading zeros stripped from VARCHAR columns
#
# This script demonstrates that Greenmask strips leading zeros from VARCHAR
# columns containing numeric-looking strings (like GTINs) when a transformer
# processes the column.
#
# Usage: ./reproduce.sh [--with-fix]
#   --with-fix    Run with the fixed version (expects greenmask binary in PATH)
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=============================================="
echo "Issue #394: Leading Zeros Bug Reproduction"
echo "=============================================="
echo ""

# Check for required tools
command -v docker >/dev/null 2>&1 || { echo "Docker is required but not installed."; exit 1; }
command -v docker-compose >/dev/null 2>&1 || command -v docker >/dev/null 2>&1 || { echo "Docker Compose is required."; exit 1; }

# Check for greenmask binary
GREENMASK_BIN="${GREENMASK_BIN:-greenmask}"
if ! command -v "$GREENMASK_BIN" >/dev/null 2>&1; then
    echo -e "${YELLOW}Warning: greenmask binary not found in PATH${NC}"
    echo "Set GREENMASK_BIN environment variable to the path of the greenmask binary"
    echo ""
    echo "To build greenmask from source:"
    echo "  cd /path/to/greenmask"
    echo "  go build -o greenmask ./cmd/greenmask"
    echo "  export GREENMASK_BIN=/path/to/greenmask/greenmask"
    echo ""
    exit 1
fi

cleanup() {
    echo ""
    echo "Cleaning up..."
    docker-compose down -v 2>/dev/null || true
    rm -rf /tmp/greenmask_dump 2>/dev/null || true
}
trap cleanup EXIT

echo "Step 1: Starting PostgreSQL..."
docker-compose up -d
echo "Waiting for PostgreSQL to be ready..."
sleep 5

# Wait for PostgreSQL to be ready
for i in {1..30}; do
    if docker-compose exec -T postgres pg_isready -U test -d testdb >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

echo ""
echo "Step 2: Showing original data (before Greenmask)..."
echo ""
echo "Products table (GTIN column is VARCHAR(14)):"
docker-compose exec -T postgres psql -U test -d testdb -c \
    "SELECT id, name, gtin, LENGTH(gtin) as len FROM products ORDER BY id;"

echo ""
echo "Identifiers table (code column is TEXT):"
docker-compose exec -T postgres psql -U test -d testdb -c \
    "SELECT id, code, LENGTH(code) as len, type FROM identifiers ORDER BY id;"

echo ""
echo "Step 3: Creating dump directory..."
mkdir -p /tmp/greenmask_dump

echo ""
echo "Step 4: Running Greenmask dump with Template transformer..."
echo "(This triggers the encode/decode cycle that causes the bug)"
echo ""

# Create a simplified config for local testing
cat > /tmp/greenmask_config.yml << 'EOF'
common:
  tmp_dir: "/tmp/greenmask_tmp"

storage:
  type: "directory"
  directory:
    path: "/tmp/greenmask_dump"

dump:
  pg_dump_options:
    dbname: "host=localhost port=15432 user=test password=test dbname=testdb"
    jobs: 1

  transformation:
    - schema: "public"
      name: "products"
      transformers:
        # Pass-through Template that triggers the bug
        - name: "Template"
          params:
            column: "gtin"
            template: "{{ .GetColumnValue \"gtin\" }}"

    - schema: "public"
      name: "identifiers"
      transformers:
        - name: "Template"
          params:
            column: "code"
            template: "{{ .GetColumnValue \"code\" }}"
EOF

mkdir -p /tmp/greenmask_tmp

# Run greenmask dump
"$GREENMASK_BIN" dump --config /tmp/greenmask_config.yml 2>&1 || {
    echo -e "${RED}Greenmask dump failed${NC}"
    exit 1
}

echo ""
echo "Step 5: Examining the dump output..."
echo ""

# Find and display the products table dump
PRODUCTS_DUMP=$(find /tmp/greenmask_dump -name "*products*" -type f 2>/dev/null | head -1)
if [ -n "$PRODUCTS_DUMP" ]; then
    echo "Products table dump content:"
    echo "---"
    cat "$PRODUCTS_DUMP" | head -20
    echo "---"
fi

# Find and display the identifiers table dump
IDENTIFIERS_DUMP=$(find /tmp/greenmask_dump -name "*identifiers*" -type f 2>/dev/null | head -1)
if [ -n "$IDENTIFIERS_DUMP" ]; then
    echo ""
    echo "Identifiers table dump content:"
    echo "---"
    cat "$IDENTIFIERS_DUMP" | head -20
    echo "---"
fi

echo ""
echo "Step 6: Checking for leading zeros bug..."
echo ""

# Check if leading zeros were preserved
BUG_FOUND=false

if [ -n "$PRODUCTS_DUMP" ]; then
    # Check for the original GTIN with leading zeros
    if grep -q "00001402417161" "$PRODUCTS_DUMP"; then
        echo -e "${GREEN}PASS: GTIN '00001402417161' preserved in products dump${NC}"
    else
        echo -e "${RED}BUG CONFIRMED: GTIN '00001402417161' NOT found in dump${NC}"
        if grep -q "1402417161" "$PRODUCTS_DUMP"; then
            echo -e "${RED}  -> Found '1402417161' instead (leading zeros stripped!)${NC}"
        fi
        BUG_FOUND=true
    fi

    if grep -q "00000012345678" "$PRODUCTS_DUMP"; then
        echo -e "${GREEN}PASS: GTIN '00000012345678' preserved in products dump${NC}"
    else
        echo -e "${RED}BUG CONFIRMED: GTIN '00000012345678' NOT found in dump${NC}"
        BUG_FOUND=true
    fi
fi

if [ -n "$IDENTIFIERS_DUMP" ]; then
    if grep -q "000123" "$IDENTIFIERS_DUMP"; then
        echo -e "${GREEN}PASS: Code '000123' preserved in identifiers dump${NC}"
    else
        echo -e "${RED}BUG CONFIRMED: Code '000123' NOT found in dump${NC}"
        if grep -q "123" "$IDENTIFIERS_DUMP"; then
            echo -e "${RED}  -> Found '123' instead (leading zeros stripped!)${NC}"
        fi
        BUG_FOUND=true
    fi
fi

echo ""
echo "=============================================="
if [ "$BUG_FOUND" = true ]; then
    echo -e "${RED}BUG REPRODUCED: Leading zeros were stripped!${NC}"
    echo ""
    echo "This confirms issue #394:"
    echo "VARCHAR/TEXT columns containing numeric-looking strings"
    echo "lose their leading zeros when processed by transformers."
    exit 1
else
    echo -e "${GREEN}NO BUG: Leading zeros were preserved correctly.${NC}"
    echo ""
    echo "If running with the fix applied, this is expected."
    echo "If running without the fix, please verify the dump files manually."
    exit 0
fi
