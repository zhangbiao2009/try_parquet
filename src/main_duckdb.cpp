#include "duckdb_analyzer.h"
#include <iostream>

int main() {
    std::cout << "DuckDB C++ OLAP Analysis Demo\n";
    std::cout << "==============================\n";
    
    try {
        DuckDBOLAPAnalyzer analyzer;
        
        bool success = analyzer.RunAllAnalyses();
        if (!success) {
            std::cerr << "Analysis failed!" << std::endl;
            return 1;
        }
        
        std::cout << "\n🎯 DuckDB C++ provides:\n";
        std::cout << "• Familiar SQL interface for analytics\n";
        std::cout << "• Out-of-core processing capabilities\n";
        std::cout << "• Automatic query optimization\n";
        std::cout << "• Native C++ performance\n";
        std::cout << "• Direct Parquet file querying\n";
        std::cout << "• Vectorized columnar execution\n";
        std::cout << "• Embedded analytical database\n";
        
        std::cout << "\n🚀 Perfect for:\n";
        std::cout << "• Large-scale analytical workloads\n";
        std::cout << "• ETL and data processing pipelines\n";
        std::cout << "• Interactive analytical applications\n";
        std::cout << "• Replacing pandas for big data\n";
        
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "Unexpected error: " << e.what() << std::endl;
        return 1;
    }
}
