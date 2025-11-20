#!/usr/bin/env bash
# Run all MetaFuse examples

set -e  # Exit on error

echo "============================================"
echo "  Running MetaFuse Examples"
echo "============================================"
echo ""

# Initialize catalog
echo "Initializing catalog..."
cargo run --bin metafuse -- init --force > /dev/null 2>&1 || true
echo "Catalog initialized"
echo ""

# Run simple_pipeline example
echo "============================================"
echo "  Example 1: Simple Pipeline"
echo "============================================"
cargo run --example simple_pipeline
echo ""
echo "Press Enter to continue to next example..."
read -r

# Run lineage_tracking example
echo "============================================"
echo "  Example 2: Lineage Tracking"
echo "============================================"
cargo run --example lineage_tracking
echo ""

# Show summary
echo "============================================"
echo "  All Examples Complete!"
echo "============================================"
echo ""
echo "Catalog Summary:"
cargo run --bin metafuse -- stats
echo ""
echo "List all datasets:"
cargo run --bin metafuse -- list
echo ""
echo "Next steps:"
echo "  - View dataset details: metafuse show <dataset_name>"
echo "  - View lineage: metafuse show <dataset_name> --lineage"
echo "  - Search: metafuse search <query>"
echo "  - Start API: cargo run --bin metafuse-api"
echo ""
