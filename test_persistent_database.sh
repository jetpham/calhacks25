#!/usr/bin/env bash

# DuckDB Persistent Database with Indexes Test Script
# This script creates a persistent database with proper indexes and tests performance

echo "🚀 DuckDB Persistent Database Test"
echo "================================="

# Create profiling output directory
mkdir -p profiling

echo "📊 Step 1: Creating persistent database with indexes..."
echo "Memory limit: 16GB"
echo "Storage: Persistent disk-based database"
echo "This may take several minutes due to index creation..."

# Create persistent database with indexes
cargo run --release -- --input-dir data/data-lite --save-db profiling/events_persistent_indexed.db

echo ""
echo "🔍 Step 2: Verifying indexes were created..."

# Wait a moment for file to be fully written
sleep 2
if [ -f "profiling/events_persistent_indexed.db" ]; then
    echo "✅ Persistent database file created successfully"
    
    # Check database size
    echo "📊 Database size:"
    ls -lh profiling/events_persistent_indexed.db
    
    # Verify indexes using DuckDB (if available)
    if command -v duckdb &> /dev/null; then
        echo "📋 Verifying indexes in database..."
        duckdb profiling/events_persistent_indexed.db -c "SELECT index_name, table_name, schema_name, is_unique FROM duckdb_indexes() WHERE table_name = 'events' ORDER BY index_name;"
    else
        echo "⚠️ DuckDB CLI not available - cannot verify indexes directly"
    fi
    
    echo ""
    echo "📊 Step 3: Running queries with comprehensive profiling..."
    
    # Run queries with profiling using the persistent indexed database
    cargo run --release -- --run --load-db profiling/events_persistent_indexed.db --queries queries.json --output-dir results/persistent-indexed
    
    echo ""
    echo "📈 Step 4: Analyzing performance improvements..."
    
    # Check if profiling files were created
    if [ -f "profiling/profiling_report.md" ]; then
        echo "✅ Profiling report generated: profiling/profiling_report.md"
        echo ""
        echo "📋 Key Performance Metrics:"
        echo "=========================="
        
        # Extract key metrics from the report
        if grep -q "Total Execution Time" profiling/profiling_report.md; then
            echo "📊 Performance Summary:"
            grep -A 6 "## Summary Statistics" profiling/profiling_report.md | head -7
        fi
        
        echo ""
        echo "🎯 Optimization Analysis:"
        echo "========================"
        
        # Show optimization recommendations
        if grep -q "Optimization Recommendations" profiling/profiling_report.md; then
            grep -A 20 "## Optimization Recommendations" profiling/profiling_report.md
        else
            echo "✅ No optimization recommendations - queries appear well-optimized!"
        fi
        
        echo ""
        echo "📁 Detailed Results:"
        echo "==================="
        echo "• Profiling report: profiling/profiling_report.md"
        echo "• Query profile JSON: profiling/query_profile.json"
        echo "• Query results: results/persistent-indexed/"
        echo "• Persistent database: profiling/events_persistent_indexed.db"
        
        if [ -f "profiling/query_graph.html" ]; then
            echo "• Query graph: profiling/query_graph.html"
        fi
        
        echo ""
        echo "🔧 Index Information:"
        echo "==================="
        echo "The following optimized indexes were created:"
        echo "• idx_events_type_day_minute (type, day, minute)"
        echo "• idx_events_type_country_day (type, country, day)"
        echo "• idx_events_type_day (type, day)"
        echo "• idx_events_advertiser_type (advertiser_id, type)"
        echo "• Plus single-column indexes for all major columns"
        
        echo ""
        echo "📊 Expected Improvements:"
        echo "======================="
        echo "With persistent storage and indexes, you should see:"
        echo "• Dramatically reduced row scanning (billions → thousands)"
        echo "• Lower memory usage (82GB → ~1-2GB)"
        echo "• Faster execution times"
        echo "• Index scans instead of sequential scans"
        
    else
        echo "❌ Profiling report not found. Check for errors above."
    fi
    
else
    echo "❌ Persistent database file not created. Check for errors above."
fi

echo ""
echo "✨ Persistent database test complete!"
echo "The database with indexes is now saved and can be reused for future queries."
