use datafusion::prelude::*;
use anyhow::Result;
use futures::future;

/// Execute queries and return results for timing measurement
/// Uses concurrent execution for maximum performance with DataFusion's multi-threaded engine
pub async fn execute_queries_for_timing(
    ctx: &SessionContext,
    sql_queries: &[String],
) -> Result<Vec<Vec<datafusion::arrow::array::RecordBatch>>> {
    // Create futures for all queries to execute them concurrently
    let futures = sql_queries.iter().enumerate().map(|(index, sql)| {
        let ctx = ctx.clone();
        let sql = sql.clone();
        async move {
            let df = ctx.sql(&sql).await?;
            let batches = df.collect().await?;
            Ok::<(usize, Vec<datafusion::arrow::array::RecordBatch>), anyhow::Error>((index, batches))
        }
    });
    
    // Execute all queries concurrently using Tokio's join_all
    let results = future::join_all(futures).await;
    
    // Convert results and sort by index to maintain original query order
    let mut indexed_results: Vec<_> = results.into_iter().collect::<Result<Vec<_>>>()?;
    indexed_results.sort_by_key(|(index, _)| *index);
    
    // Extract just the batches, maintaining original order
    let all_results = indexed_results
        .into_iter()
        .map(|(_, batches)| batches)
        .collect();
    
    Ok(all_results)
}
