#pragma once

#include <duckdb.hpp>
#include <iostream>
#include <vector>
#include <string>
#include <memory>

/**
 * OLAP Analyzer using DuckDB C++ for SQL-based analytics.
 * Demonstrates scalable out-of-core processing with familiar SQL interface.
 * Can handle datasets larger than memory through DuckDB's optimization.
 */
class DuckDBOLAPAnalyzer {
private:
    std::unique_ptr<duckdb::DuckDB> db_;
    std::unique_ptr<duckdb::Connection> conn_;

    // Helper methods
    void ConfigureDatabase();
    void PrintQueryResult(std::unique_ptr<duckdb::MaterializedQueryResult> result,
                         const std::string& title);
    std::vector<std::vector<std::string>> GetQueryData(const std::string& query);

public:
    DuckDBOLAPAnalyzer();
    ~DuckDBOLAPAnalyzer() = default;

    // Main interface methods
    bool RegisterParquetTables();
    bool AnalyzeSalesByTime();
    bool AnalyzeSalesByGeography();
    bool AnalyzeSalesByProduct();
    bool AnalyzeCustomerSegments();
    bool MultidimensionalAnalysis();
    bool DemonstratePerformanceAdvantages();
    
    // Utility methods
    void PrintDataInfo();
    bool RunAllAnalyses();
    
    // Query execution
    std::unique_ptr<duckdb::MaterializedQueryResult> ExecuteQuery(const std::string& query);
    bool HasError(std::unique_ptr<duckdb::MaterializedQueryResult>& result);
};
