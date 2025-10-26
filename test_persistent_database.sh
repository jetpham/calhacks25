#!/usr/bin/env bash

# DuckDB Persistent Database Test Script
# This script creates a persistent database and tests performance

echo "🚀 DuckDB Persistent Database Test"
echo "================================="

# Create profiling output directory
mkdir -p profiling

echo "📊 Step 1: Creating persistent database..."
echo "Memory limit: 16GB"
echo "Storage: Persistent disk-based database"

# Create persistent database
cargo run --release -- --input-dir data/data-lite --save-db profiling/events_persistent.db

echo ""
echo "🔍 Step 2: Verifying database was created..."

# Wait a moment for file to be fully written
sleep 2
if [ -f "profiling/events_persistent.db" ]; then
    echo "✅ Persistent database file created successfully"
    
    # Check database size
    echo "📊 Database size:"
    ls -lh profiling/events_persistent.db
    
    echo ""
    echo "📊 Step 3: Running queries with comprehensive profiling..."
    
    # Run queries with profiling using the persistent database
    cargo run --release -- --run --load-db profiling/events_persistent.db --queries queries.json --output-dir results/persistent
    
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
        echo "• Query results: results/persistent/"
        echo "• Persistent database: profiling/events_persistent.db"
        
        if [ -f "profiling/query_graph.html" ]; then
            echo "• Query graph: profiling/query_graph.html"
        fi
        
        echo ""
        echo "📊 Expected Improvements:"
        echo "======================="
        echo "With persistent storage, you should see:"
        echo "• Reduced memory usage"
        echo "• Faster subsequent query execution"
        echo "• Persistent data storage"
        
    else
        echo "❌ Profiling report not found. Check for errors above."
    fi
    
else
    echo "❌ Persistent database file not created. Check for errors above."
fi

echo ""
echo "✨ Persistent database test complete!"
echo "The database is now saved and can be reused for future queries."
