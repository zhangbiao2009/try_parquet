#pragma once

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <parquet/arrow/reader.h>
#include <arrow/table.h>
#include <arrow/io/file.h>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

/**
 * OLAP Analyzer using Apache Arrow C++ for columnar processing.
 * Demonstrates high-performance analytics on Parquet files using Arrow's
 * vectorized operations and columnar data structures.
 */
class ArrowOLAPAnalyzer {
private:
    std::shared_ptr<arrow::Table> sales_table_;
    std::shared_ptr<arrow::Table> time_table_;
    std::shared_ptr<arrow::Table> geography_table_;
    std::shared_ptr<arrow::Table> product_table_;
    std::shared_ptr<arrow::Table> customer_table_;

    // Helper methods
    arrow::Status LoadParquetFile(const std::string& filename, 
                                 std::shared_ptr<arrow::Table>& table);
    
    arrow::Result<std::shared_ptr<arrow::Table>> JoinTables(
        std::shared_ptr<arrow::Table> left,
        std::shared_ptr<arrow::Table> right,
        const std::string& left_key,
        const std::string& right_key);
    
    arrow::Result<std::shared_ptr<arrow::Table>> GroupByAndSum(
        std::shared_ptr<arrow::Table> table,
        const std::vector<std::string>& group_columns,
        const std::vector<std::string>& sum_columns);
    
    void PrintTable(std::shared_ptr<arrow::Table> table, 
                   const std::string& title,
                   int max_rows = 10);
    
    arrow::Result<std::shared_ptr<arrow::Array>> GetColumnAsArray(
        std::shared_ptr<arrow::Table> table,
        const std::string& column_name);

public:
    ArrowOLAPAnalyzer() = default;
    ~ArrowOLAPAnalyzer() = default;

    // Main interface methods
    arrow::Status LoadAllTables();
    arrow::Status AnalyzeSalesByTime();
    arrow::Status AnalyzeSalesByGeography();
    arrow::Status AnalyzeSalesByProduct();
    arrow::Status AnalyzeCustomerSegments();
    arrow::Status MultidimensionalAnalysis();
    
    // Utility methods
    void PrintDataInfo();
    arrow::Status RunAllAnalyses();
};
