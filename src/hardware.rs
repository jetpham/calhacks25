use std::sync::OnceLock;

pub struct HardwareInfo {
    pub num_threads: usize,
    pub available_memory_gb: f64,
}

static HARDWARE_INFO: OnceLock<HardwareInfo> = OnceLock::new();

impl HardwareInfo {
    pub fn detect() -> Self {
        let num_threads = num_cpus::get();
        
        // Try to detect memory (fallback to reasonable defaults)
        let available_memory_gb = if cfg!(target_os = "linux") {
            // Try to read from /proc/meminfo
            if let Ok(content) = std::fs::read_to_string("/proc/meminfo") {
                let mut available_kb = 0;
                
                for line in content.lines() {
                    if line.starts_with("MemAvailable:") {
                        if let Some(kb_str) = line.split_whitespace().nth(1) {
                            available_kb = kb_str.parse().unwrap_or(0);
                        }
                    }
                }
                
                available_kb as f64 / 1_048_576.0 // KB to GB
            } else {
                16.0 // Fallback
            }
        } else {
            16.0 // Fallback for non-Linux
        };
        
        Self {
            num_threads,
            available_memory_gb,
        }
    }
    
    pub fn get() -> &'static HardwareInfo {
        HARDWARE_INFO.get_or_init(|| Self::detect())
    }
    
    /// Calculate optimal row group size for Parquet
    /// Winner's approach: aim for ~20 row groups per thread
    /// With 10 threads: ROW_GROUP_SIZE = 1M
    /// Formula: target ~20 row groups per thread
    pub fn optimal_row_group_size(&self, total_rows: usize) -> usize {
        // Winner's approach: with 10 threads, use 1M row groups
        // This gives ~20 row groups per thread (245M rows / 10 threads / 1M = ~24.5 groups)
        // For our hardware, adjust based on thread count
        
        let target_groups_per_thread = 20;
        let rows_per_thread = total_rows / self.num_threads.max(1);
        let optimal_size = rows_per_thread / target_groups_per_thread;
        
        // Clamp to reasonable values
        optimal_size.max(500_000).min(2_000_000)
    }
    
    /// Calculate cost function weights based on hardware
    /// More RAM = can scan more rows efficiently
    /// More threads = rollup is cheaper (parallel aggregation)
    pub fn cost_weights(&self) -> (f64, f64) {
        // Base weights (winner's values)
        let base_scan_weight = 1.0;
        let base_rollup_weight = 32.0;
        
        // Adjust based on available memory
        // Winner had 18GB, we have more - can afford to scan more
        let memory_factor = (self.available_memory_gb / 18.0).min(2.0).max(0.5);
        let scan_weight = base_scan_weight / memory_factor; // Lower weight = prefer scanning
        
        // Adjust rollup weight based on thread count
        // More threads = parallel aggregation is cheaper
        let thread_factor = (self.num_threads as f64 / 10.0).min(2.0).max(0.5);
        let rollup_weight = base_rollup_weight / thread_factor; // Lower weight = rollup cheaper
        
        (scan_weight, rollup_weight)
    }
}

pub fn get_hardware_info() -> &'static HardwareInfo {
    HardwareInfo::get()
}

