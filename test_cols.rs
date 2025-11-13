use duckdb::Connection;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let con = Connection::open("duck1.db")?;
    let mut stmt = con.prepare("SELECT advertiser_id, type, SUM(count_rows) AS \"count_star()\" FROM mv_advertiser_id_fast GROUP BY advertiser_id, type ORDER BY \"count_star()\" DESC LIMIT 1")?;
    let mut rows = stmt.query([])?;
    if let Some(row) = rows.next()? {
        let stmt_ref = rows.as_ref().ok_or("no stmt ref")?;
        for i in 0..stmt_ref.column_count() {
            println!("Column {}: '{}'", i, stmt_ref.column_name(i)?);
        }
    }
    Ok(())
}
