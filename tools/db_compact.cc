//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// db_compact: A tool to compact RocksDB databases with flexible options

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run db_compact\n");
  return 1;
}
#else

#include <cstdio>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/options_util.h"
#include "util/gflags_compat.h"
#include "util/string_util.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::SetUsageMessage;

// Database path (required)
DEFINE_string(db, "", "Path to the RocksDB database (required)");

// Column families to compact (optional)
DEFINE_string(column_families, "",
              "Comma-separated list of column families to compact. "
              "If empty, compacts all column families. "
              "Example: --column_families=cf1,cf2,cf3");

// Compression type
DEFINE_string(compression_type, "lz4",
              "Compression algorithm to use: none, snappy, zlib, bz2, lz4, "
              "lz4hc, xpress, zstd");

// Compaction parameters
DEFINE_int32(
    max_background_jobs, 16,
    "Maximum number of concurrent background jobs (compactions and flushes)");

DEFINE_int32(max_subcompactions, 4,
             "Maximum number of threads for a single compaction job");

DEFINE_bool(exclusive_manual_compaction, false,
            "If true, no other compaction will run at the same time");

DEFINE_bool(
    change_level, false,
    "If true, compacted files will be moved to the minimum level capable");

DEFINE_int32(
    target_level, -1,
    "Target level for compacted files (only used with --change_level=true)");

DEFINE_string(
    bottommost_level_compaction, "kIfHaveCompactionFilter",
    "Bottommost level compaction strategy: kSkip, kIfHaveCompactionFilter, "
    "kForce, kForceOptimized");

// I/O and performance options
DEFINE_int32(max_open_files, -1,
             "Maximum number of open files (-1 means unlimited)");

DEFINE_uint64(compaction_readahead_size, 2097152,  // 2MB default
              "Compaction readahead size in bytes (0 to disable)");

// Display options
DEFINE_bool(verbose, false, "Enable verbose output");

DEFINE_bool(stats, true,
            "Display database statistics before and after compaction");

namespace ROCKSDB_NAMESPACE {

class DBCompactor {
 public:
  DBCompactor() = default;

  int Run() {
    if (!ValidateFlags()) {
      return 1;
    }

    Status s = OpenDatabase();
    if (!s.ok()) {
      fprintf(stderr, "Failed to open database: %s\n", s.ToString().c_str());
      return 1;
    }

    if (FLAGS_stats) {
      PrintStats("BEFORE COMPACTION");
    }

    s = PerformCompaction();
    if (!s.ok()) {
      fprintf(stderr, "Compaction failed: %s\n", s.ToString().c_str());
      CloseDatabase();
      return 1;
    }

    if (FLAGS_stats) {
      PrintStats("AFTER COMPACTION");
    }

    CloseDatabase();

    printf("\n✓ Compaction completed successfully!\n");
    return 0;
  }

 private:
  DB* db_ = nullptr;
  std::vector<ColumnFamilyHandle*> cf_handles_;
  std::vector<std::string> cf_names_;

  bool ValidateFlags() {
    if (FLAGS_db.empty()) {
      fprintf(stderr, "Error: --db is required\n");
      return false;
    }

    if (FLAGS_max_background_jobs <= 0) {
      fprintf(stderr, "Error: --max_background_jobs must be positive\n");
      return false;
    }

    if (FLAGS_max_subcompactions <= 0) {
      fprintf(stderr, "Error: --max_subcompactions must be positive\n");
      return false;
    }

    return true;
  }

  Status OpenDatabase() {
    printf("Opening database: %s\n", FLAGS_db.c_str());

    // List all column families in the database
    DBOptions db_options;
    Status s = DB::ListColumnFamilies(db_options, FLAGS_db, &cf_names_);
    if (!s.ok()) {
      fprintf(stderr, "Failed to list column families: %s\n",
              s.ToString().c_str());
      return s;
    }

    printf("Found %zu column families:\n", cf_names_.size());
    for (const auto& name : cf_names_) {
      printf("  - %s\n", name.c_str());
    }

    // Parse user-specified column families
    std::vector<std::string> target_cf_names;
    if (!FLAGS_column_families.empty()) {
      std::stringstream ss(FLAGS_column_families);
      std::string cf_name;
      while (std::getline(ss, cf_name, ',')) {
        // Trim whitespace
        cf_name.erase(0, cf_name.find_first_not_of(" \t\n\r\f\v"));
        cf_name.erase(cf_name.find_last_not_of(" \t\n\r\f\v") + 1);

        if (!cf_name.empty()) {
          target_cf_names.push_back(cf_name);
        }
      }

      // Validate specified CF names exist
      for (const auto& name : target_cf_names) {
        if (std::find(cf_names_.begin(), cf_names_.end(), name) ==
            cf_names_.end()) {
          fprintf(stderr, "Error: Column family '%s' does not exist\n",
                  name.c_str());
          return Status::InvalidArgument("Column family not found");
        }
      }

      printf("\nWill compact %zu column families:\n", target_cf_names.size());
      for (const auto& name : target_cf_names) {
        printf("  - %s\n", name.c_str());
      }
    } else {
      target_cf_names = cf_names_;
      printf("\nWill compact all column families\n");
    }

    // Prepare column family descriptors
    std::vector<ColumnFamilyDescriptor> cf_descriptors;
    Options options = BuildOptions();

    for (const auto& name : cf_names_) {
      cf_descriptors.emplace_back(name, options);
    }

    // Open database with all column families
    s = DB::Open(options, FLAGS_db, cf_descriptors, &cf_handles_, &db_);
    if (!s.ok()) {
      return s;
    }

    printf("\n✓ Database opened successfully\n");
    return Status::OK();
  }

  Options BuildOptions() {
    Options options;

    // DB options
    DBOptions db_options;
    db_options.max_background_jobs = FLAGS_max_background_jobs;
    db_options.max_open_files = FLAGS_max_open_files;
    db_options.compaction_readahead_size = FLAGS_compaction_readahead_size;
    db_options.max_subcompactions =
        static_cast<uint32_t>(FLAGS_max_subcompactions);

    // CF options
    ColumnFamilyOptions cf_options;
    // Parse compression type
    CompressionType compression = kLZ4Compression;
    std::string comp_lower = FLAGS_compression_type;
    std::transform(comp_lower.begin(), comp_lower.end(), comp_lower.begin(),
                   ::tolower);

    if (comp_lower == "none" || comp_lower == "no") {
      compression = kNoCompression;
    } else if (comp_lower == "snappy") {
      compression = kSnappyCompression;
    } else if (comp_lower == "zlib") {
      compression = kZlibCompression;
    } else if (comp_lower == "bz2" || comp_lower == "bzip2") {
      compression = kBZip2Compression;
    } else if (comp_lower == "lz4") {
      compression = kLZ4Compression;
    } else if (comp_lower == "lz4hc") {
      compression = kLZ4HCCompression;
    } else if (comp_lower == "xpress") {
      compression = kXpressCompression;
    } else if (comp_lower == "zstd") {
      compression = kZSTD;
    } else {
      fprintf(stderr, "Warning: Unknown compression type '%s', using LZ4\n",
              FLAGS_compression_type.c_str());
    }

    cf_options.compression = compression;

    options = Options(db_options, cf_options);
    return options;
  }

  Status PerformCompaction() {
    printf("\n=== Starting Compaction ===\n");
    printf("Configuration:\n");
    printf("  max_background_jobs: %d\n", FLAGS_max_background_jobs);
    printf("  max_subcompactions: %d\n", FLAGS_max_subcompactions);
    printf("  compression_type: %s\n", FLAGS_compression_type.c_str());
    printf("  exclusive: %s\n",
           FLAGS_exclusive_manual_compaction ? "true" : "false");
    printf("\n");

    // Parse bottommost level compaction
    BottommostLevelCompaction bottommost =
        BottommostLevelCompaction::kIfHaveCompactionFilter;
    std::string bottommost_str = FLAGS_bottommost_level_compaction;
    if (bottommost_str == "kSkip") {
      bottommost = BottommostLevelCompaction::kSkip;
    } else if (bottommost_str == "kForce") {
      bottommost = BottommostLevelCompaction::kForce;
    } else if (bottommost_str == "kForceOptimized") {
      bottommost = BottommostLevelCompaction::kForceOptimized;
    }

    // Prepare compact range options
    CompactRangeOptions compact_options;
    compact_options.max_subcompactions =
        static_cast<uint32_t>(FLAGS_max_subcompactions);
    compact_options.exclusive_manual_compaction =
        FLAGS_exclusive_manual_compaction;
    compact_options.change_level = FLAGS_change_level;
    compact_options.target_level = FLAGS_target_level;
    compact_options.bottommost_level_compaction = bottommost;

    // Parse target column families
    std::vector<std::string> target_cf_names;
    if (!FLAGS_column_families.empty()) {
      std::stringstream ss(FLAGS_column_families);
      std::string cf_name;
      while (std::getline(ss, cf_name, ',')) {
        cf_name.erase(0, cf_name.find_first_not_of(" \t\n\r\f\v"));
        cf_name.erase(cf_name.find_last_not_of(" \t\n\r\f\v") + 1);
        if (!cf_name.empty()) {
          target_cf_names.push_back(cf_name);
        }
      }
    } else {
      target_cf_names = cf_names_;
    }

    // Compact each column family
    for (size_t i = 0; i < cf_handles_.size(); ++i) {
      const std::string& cf_name = cf_names_[i];

      // Skip if not in target list
      if (!target_cf_names.empty() &&
          std::find(target_cf_names.begin(), target_cf_names.end(), cf_name) ==
              target_cf_names.end()) {
        if (FLAGS_verbose) {
          printf("Skipping column family: %s\n", cf_name.c_str());
        }
        continue;
      }

      printf("Compacting column family: %s ... ", cf_name.c_str());
      fflush(stdout);

      auto start = std::chrono::steady_clock::now();

      Status s =
          db_->CompactRange(compact_options, cf_handles_[i], nullptr, nullptr);

      auto end = std::chrono::steady_clock::now();
      auto duration =
          std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

      if (!s.ok()) {
        printf("FAILED (%s)\n", s.ToString().c_str());
        return s;
      }

      printf("OK (%.2f seconds)\n", duration.count() / 1000.0);
    }

    return Status::OK();
  }

  void PrintStats(const char* label) {
    printf("\n=== %s ===\n", label);

    for (size_t i = 0; i < cf_handles_.size(); ++i) {
      std::string stats;
      if (db_->GetProperty(cf_handles_[i], "rocksdb.stats", &stats)) {
        printf("\nColumn Family: %s\n", cf_names_[i].c_str());
        printf("%s\n", stats.c_str());
      }

      // Print level sizes
      std::string level_stats;
      if (db_->GetProperty(cf_handles_[i], "rocksdb.levelstats",
                           &level_stats)) {
        printf("Level Stats:\n%s\n", level_stats.c_str());
      }

      // Print number of files
      std::string num_files;
      if (db_->GetProperty(cf_handles_[i], "rocksdb.num-files-at-level0",
                           &num_files)) {
        printf("L0 files: %s\n", num_files.c_str());
      }
    }
  }

  void CloseDatabase() {
    if (db_) {
      for (auto* handle : cf_handles_) {
        if (handle) {
          db_->DestroyColumnFamilyHandle(handle);
        }
      }
      cf_handles_.clear();

      delete db_;
      db_ = nullptr;

      printf("\n✓ Database closed\n");
    }
  }
};

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  SetUsageMessage(
      "db_compact - Compact RocksDB databases with flexible options\n"
      "\n"
      "Usage:\n"
      "  db_compact --db=/path/to/db [options]\n"
      "\n"
      "Examples:\n"
      "  # Compact all column families with default options\n"
      "  db_compact --db=/data/mydb\n"
      "\n"
      "  # Compact specific column families\n"
      "  db_compact --db=/data/mydb --column_families=cf1,cf2,cf3\n"
      "\n"
      "  # Use ZSTD compression with high parallelism\n"
      "  db_compact --db=/data/mydb --compression_type=zstd \\\n"
      "             --max_background_jobs=32 --max_subcompactions=8\n"
      "\n"
      "  # Exclusive compaction (no other compactions run)\n"
      "  db_compact --db=/data/mydb --exclusive_manual_compaction=true\n");

  ParseCommandLineFlags(&argc, &argv, true);

  ROCKSDB_NAMESPACE::DBCompactor compactor;
  return compactor.Run();
}

#endif  // GFLAGS
