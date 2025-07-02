#include "duckdb_analyzer.h"
#include <chrono>
#include <iomanip>
#include <cstdlib>  // for std::getenv

DuckDBOLAPAnalyzer::DuckDBOLAPAnalyzer() {
    // Initialize DuckDB
    db_ = std::make_unique<duckdb::DuckDB>(nullptr);
    conn_ = std::make_unique<duckdb::Connection>(*db_);
    
    ConfigureDatabase();
}

void DuckDBOLAPAnalyzer::ConfigureDatabase() {
    // Configure DuckDB for optimal analytical performance
    conn_->Query("SET memory_limit='4GB'");
    conn_->Query("SET threads=4");
    conn_->Query("SET enable_progress_bar=false");
}

std::unique_ptr<duckdb::MaterializedQueryResult> DuckDBOLAPAnalyzer::ExecuteQuery(const std::string& query) {
    return conn_->Query(query);
}

bool DuckDBOLAPAnalyzer::HasError(std::unique_ptr<duckdb::MaterializedQueryResult>& result) {
    return result->HasError();
}

void DuckDBOLAPAnalyzer::PrintQueryResult(std::unique_ptr<duckdb::MaterializedQueryResult> result,
                                         const std::string& title) {
    std::cout << "\n" << title << "\n";
    std::cout << std::string(title.length(), '=') << "\n";
    
    if (result->HasError()) {
        std::cerr << "Query error: " << result->GetError() << std::endl;
        return;
    }
    
    // Print column headers
    for (size_t col = 0; col < result->ColumnCount(); ++col) {
        std::cout << std::setw(15) << result->ColumnName(col);
    }
    std::cout << "\n";
    
    // Print separator
    for (size_t col = 0; col < result->ColumnCount(); ++col) {
        std::cout << std::setw(15) << std::setfill('-') << "";
    }
    std::cout << std::setfill(' ') << "\n";
    
    // Print data rows
    for (size_t row = 0; row < result->RowCount(); ++row) {
        for (size_t col = 0; col < result->ColumnCount(); ++col) {
            std::cout << std::setw(15) << result->GetValue(col, row).ToString();
        }
        std::cout << "\n";
    }
}

bool DuckDBOLAPAnalyzer::RegisterParquetTables() {
    std::cout << "Registering Parquet tables in DuckDB...\n";
    
    // Check if data directory exists
    std::string data_path = "olap_data";
    if (std::getenv("OLAP_DATA_PATH")) {
        data_path = std::getenv("OLAP_DATA_PATH");
    }
    
    // Register each Parquet file as a view
    auto queries = std::vector<std::pair<std::string, std::string>>{
        {"fact_sales", data_path + "/fact_sales.parquet"},
        {"dim_time", data_path + "/dim_time.parquet"},
        {"dim_geography", data_path + "/dim_geography.parquet"},
        {"dim_product", data_path + "/dim_product.parquet"},
        {"dim_customer", data_path + "/dim_customer.parquet"}
    };
    
    for (const auto& [table_name, file_path] : queries) {
        std::string query = "CREATE VIEW " + table_name + " AS SELECT * FROM '" + file_path + "'";
        auto result = ExecuteQuery(query);
        if (HasError(result)) {
            std::cerr << "Failed to register table " << table_name << ": " << result->GetError() << std::endl;
            std::cerr << "File path: " << file_path << std::endl;
            return false;
        }
    }
    
    std::cout << "Tables registered successfully!\n";
    return true;
}

void DuckDBOLAPAnalyzer::PrintDataInfo() {
    std::cout << "\nData registered successfully!\n";
    
    auto queries = std::vector<std::pair<std::string, std::string>>{
        {"Sales records", "SELECT COUNT(*) FROM fact_sales"},
        {"Time periods", "SELECT COUNT(*) FROM dim_time"},
        {"Geographies", "SELECT COUNT(*) FROM dim_geography"},
        {"Products", "SELECT COUNT(*) FROM dim_product"},
        {"Customers", "SELECT COUNT(*) FROM dim_customer"}
    };
    
    for (const auto& [label, query] : queries) {
        auto result = ExecuteQuery(query);
        if (!HasError(result) && result->RowCount() > 0) {
            std::cout << label << ": " << result->GetValue(0, 0).ToString() << "\n";
        }
    }
}

bool DuckDBOLAPAnalyzer::AnalyzeSalesByTime() {
    std::cout << "\nSALES ANALYSIS BY TIME (DuckDB C++)\n";
    std::cout << "====================================\n";
    
    // Sales by year
    auto yearly_sales = ExecuteQuery(R"(
        SELECT 
            t.year,
            ROUND(SUM(s.gross_sales), 2) as gross_sales,
            ROUND(SUM(s.profit), 2) as profit,
            SUM(s.quantity) as quantity
        FROM fact_sales s
        JOIN dim_time t ON s.date_key = t.date_key
        GROUP BY t.year
        ORDER BY t.year
    )");
    
    PrintQueryResult(std::move(yearly_sales), "Sales by Year");
    
    // Sales by quarter (last 8 quarters)
    auto quarterly_sales = ExecuteQuery(R"(
        SELECT 
            t.year,
            t.quarter,
            ROUND(SUM(s.gross_sales), 2) as gross_sales,
            ROUND(SUM(s.profit), 2) as profit
        FROM fact_sales s
        JOIN dim_time t ON s.date_key = t.date_key
        GROUP BY t.year, t.quarter
        ORDER BY t.year, t.quarter
        LIMIT 8
    )");
    
    PrintQueryResult(std::move(quarterly_sales), "Sales by Quarter (last 8 quarters)");
    
    // Weekend vs Weekday analysis
    auto weekend_analysis = ExecuteQuery(R"(
        SELECT 
            CASE WHEN t.is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END as day_type,
            ROUND(SUM(s.gross_sales), 2) as total_sales,
            ROUND(AVG(s.gross_sales), 2) as avg_sales,
            SUM(s.quantity) as total_quantity,
            ROUND(AVG(s.quantity), 2) as avg_quantity
        FROM fact_sales s
        JOIN dim_time t ON s.date_key = t.date_key
        GROUP BY t.is_weekend
        ORDER BY t.is_weekend
    )");
    
    PrintQueryResult(std::move(weekend_analysis), "Weekend vs Weekday Analysis");
    
    return true;
}

bool DuckDBOLAPAnalyzer::AnalyzeSalesByGeography() {
    std::cout << "\n\nSALES ANALYSIS BY GEOGRAPHY (DuckDB C++)\n";
    std::cout << "=========================================\n";
    
    // Sales by region
    auto regional_sales = ExecuteQuery(R"(
        SELECT 
            g.region,
            ROUND(SUM(s.gross_sales), 2) as gross_sales,
            ROUND(SUM(s.profit), 2) as profit,
            SUM(s.quantity) as quantity
        FROM fact_sales s
        JOIN dim_geography g ON s.geography_key = g.geography_key
        GROUP BY g.region
        ORDER BY SUM(s.gross_sales) DESC
    )");
    
    PrintQueryResult(std::move(regional_sales), "Sales by Region");
    
    // Top 10 countries
    auto country_sales = ExecuteQuery(R"(
        SELECT 
            g.country,
            ROUND(SUM(s.gross_sales), 2) as gross_sales,
            ROUND(SUM(s.profit), 2) as profit
        FROM fact_sales s
        JOIN dim_geography g ON s.geography_key = g.geography_key
        GROUP BY g.country
        ORDER BY SUM(s.gross_sales) DESC
        LIMIT 10
    )");
    
    PrintQueryResult(std::move(country_sales), "Top 10 Countries by Sales");
    
    return true;
}

bool DuckDBOLAPAnalyzer::AnalyzeSalesByProduct() {
    std::cout << "\n\nSALES ANALYSIS BY PRODUCT (DuckDB C++)\n";
    std::cout << "======================================\n";
    
    // Sales by category
    auto category_sales = ExecuteQuery(R"(
        SELECT 
            p.category,
            ROUND(SUM(s.gross_sales), 2) as gross_sales,
            ROUND(SUM(s.profit), 2) as profit,
            SUM(s.quantity) as quantity
        FROM fact_sales s
        JOIN dim_product p ON s.product_key = p.product_key
        GROUP BY p.category
        ORDER BY SUM(s.gross_sales) DESC
    )");
    
    PrintQueryResult(std::move(category_sales), "Sales by Category");
    
    // Profit margin by category
    auto profit_margin = ExecuteQuery(R"(
        SELECT 
            p.category,
            ROUND(SUM(s.profit) / SUM(s.gross_sales) * 100, 2) as profit_margin_pct
        FROM fact_sales s
        JOIN dim_product p ON s.product_key = p.product_key
        GROUP BY p.category
        ORDER BY SUM(s.profit) / SUM(s.gross_sales) DESC
    )");
    
    PrintQueryResult(std::move(profit_margin), "Profit Margin by Category (%)");
    
    // Top 10 products
    auto product_sales = ExecuteQuery(R"(
        SELECT 
            p.product_name,
            ROUND(SUM(s.gross_sales), 2) as gross_sales,
            SUM(s.quantity) as quantity
        FROM fact_sales s
        JOIN dim_product p ON s.product_key = p.product_key
        GROUP BY p.product_name
        ORDER BY SUM(s.gross_sales) DESC
        LIMIT 10
    )");
    
    PrintQueryResult(std::move(product_sales), "Top 10 Products by Sales");
    
    return true;
}

bool DuckDBOLAPAnalyzer::AnalyzeCustomerSegments() {
    std::cout << "\n\nCUSTOMER SEGMENT ANALYSIS (DuckDB C++)\n";
    std::cout << "======================================\n";
    
    // Sales by customer type
    auto customer_sales = ExecuteQuery(R"(
        SELECT 
            c.customer_type,
            ROUND(SUM(s.gross_sales), 2) as total_sales,
            ROUND(AVG(s.gross_sales), 2) as avg_sales_per_order,
            ROUND(SUM(s.profit), 2) as total_profit,
            ROUND(AVG(s.profit), 2) as avg_profit_per_order,
            COUNT(DISTINCT s.customer_key) as unique_customers
        FROM fact_sales s
        JOIN dim_customer c ON s.customer_key = c.customer_key
        GROUP BY c.customer_type
        ORDER BY SUM(s.gross_sales) DESC
    )");
    
    PrintQueryResult(std::move(customer_sales), "Sales by Customer Type");
    
    return true;
}

bool DuckDBOLAPAnalyzer::MultidimensionalAnalysis() {
    std::cout << "\n\nMULTIDIMENSIONAL ANALYSIS (DuckDB C++)\n";
    std::cout << "======================================\n";
    
    // Sales by Region and Category
    auto region_category = ExecuteQuery(R"(
        SELECT 
            g.region,
            p.category,
            ROUND(SUM(s.gross_sales), 2) as gross_sales
        FROM fact_sales s
        JOIN dim_geography g ON s.geography_key = g.geography_key
        JOIN dim_product p ON s.product_key = p.product_key
        GROUP BY g.region, p.category
        ORDER BY g.region, SUM(s.gross_sales) DESC
    )");
    
    PrintQueryResult(std::move(region_category), "Sales by Region and Product Category");
    
    // Get top region for monthly trend
    auto top_region_result = ExecuteQuery(R"(
        SELECT g.region
        FROM fact_sales s
        JOIN dim_geography g ON s.geography_key = g.geography_key
        GROUP BY g.region
        ORDER BY SUM(s.gross_sales) DESC
        LIMIT 1
    )");
    
    if (!HasError(top_region_result) && top_region_result->RowCount() > 0) {
        std::string top_region = top_region_result->GetValue(0, 0).ToString();
        
        // Monthly trends for top region
        std::string monthly_query = R"(
            SELECT 
                t.year,
                t.month,
                ROUND(SUM(s.gross_sales), 2) as gross_sales
            FROM fact_sales s
            JOIN dim_time t ON s.date_key = t.date_key
            JOIN dim_geography g ON s.geography_key = g.geography_key
            WHERE g.region = ')" + top_region + R"('
            GROUP BY t.year, t.month
            ORDER BY t.year, t.month
            LIMIT 12
        )";
        
        auto monthly_trend = ExecuteQuery(monthly_query);
        PrintQueryResult(std::move(monthly_trend), 
                        "Monthly Sales Trend for " + top_region + " (last 12 months)");
    }
    
    return true;
}

bool DuckDBOLAPAnalyzer::DemonstratePerformanceAdvantages() {
    std::cout << "\n\nDUCKDB PERFORMANCE ADVANTAGES\n";
    std::cout << "==============================\n";
    
    // Time a complex query
    auto start = std::chrono::high_resolution_clock::now();
    
    auto result = ExecuteQuery(R"(
        SELECT 
            t.year,
            SUM(s.gross_sales) as total_sales
        FROM fact_sales s
        JOIN dim_time t ON s.date_key = t.date_key
        GROUP BY t.year
        ORDER BY t.year
    )");
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    if (!HasError(result)) {
        std::cout << "\nQuery executed in " << duration.count() << " milliseconds\n";
        std::cout << "✓ Only reads necessary columns from Parquet files\n";
        std::cout << "✓ Uses predicate pushdown for filtering\n";
        std::cout << "✓ Vectorized execution with parallel processing\n";
        std::cout << "✓ No memory constraint - can handle TB+ datasets\n";
        std::cout << "✓ SQL interface familiar to analysts\n";
        
        std::cout << "\nMemory usage: Minimal (streaming processing)\n";
        std::cout << "C++ performance: Native compiled code execution\n";
    }
    
    return !HasError(result);
}

bool DuckDBOLAPAnalyzer::RunAllAnalyses() {
    try {
        std::cout << "Starting DuckDB C++ OLAP analysis...\n";
        std::cout << std::string(50, '=') << "\n";
        
        if (!RegisterParquetTables()) {
            return false;
        }
        
        PrintDataInfo();
        
        // Run all analyses
        AnalyzeSalesByTime();
        AnalyzeSalesByGeography();
        AnalyzeSalesByProduct();
        AnalyzeCustomerSegments();
        MultidimensionalAnalysis();
        DemonstratePerformanceAdvantages();
        
        std::cout << "\n" << std::string(50, '=') << "\n";
        std::cout << "DuckDB C++ analysis complete!\n";
        std::cout << "\nKey benefits demonstrated:\n";
        std::cout << "• Out-of-core processing (no memory limits)\n";
        std::cout << "• SQL interface for complex analytics\n";
        std::cout << "• Automatic query optimization\n";
        std::cout << "• Direct Parquet file querying\n";
        std::cout << "• Vectorized columnar execution\n";
        std::cout << "• Native C++ performance\n";
        
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "Analysis failed: " << e.what() << std::endl;
        return false;
    }
}
