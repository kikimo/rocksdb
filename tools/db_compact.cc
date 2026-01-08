//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// db_compact: A tool to compact RocksDB databases with flexible options

#include "rocksdb/status.h"
#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run db_compact\n");
  return 1;
}
#else

#include <algorithm>
#include <cstdio>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/options_util.h"
#include "util/gflags_compat.h"
#include "util/string_util.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::SetUsageMessage;

// Database path (required)
DEFINE_string(db, "", "Path to the RocksDB database (required)");

// Options file path (optional)
DEFINE_string(option_file, "",
              "Path to a RocksDB OPTIONS file to use for opening the database. "
              "If empty, uses the latest OPTIONS file from the database directory. "
              "Example: --option_file=/path/to/OPTIONS-123456");

// Column families to compact (optional)
DEFINE_string(column_families, "",
              "Comma-separated list of column families to compact. "
              "If empty, compacts all column families. "
              "Example: --column_families=cf1,cf2,cf3");

// Exclusive mode (optional)
DEFINE_bool(exclusive, false,
            "If true, compact all column families EXCEPT those listed in "
            "--column_families. This flag inverts the selection logic. "
            "Requires --column_families to be non-empty.");

// Compression type
DEFINE_string(compression_type, "lz4",
              "Compression algorithm to use: none, snappy, zlib, bz2, lz4, "
              "lz4hc, xpress, zstd");

// Compaction parameters
DEFINE_int32(
    max_background_jobs, 16,
    "Maximum number of concurrent background jobs (compactions and flushes)");

DEFINE_int32(max_subcompactions, 16,
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

DEFINE_uint64(compaction_readahead_size, 2097152,  // 2MB default
              "Compaction readahead size in bytes (0 to disable)");

DEFINE_int32(block_size, 8, "Block size in KB for SST files (default: 8KB)");

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

    if (FLAGS_exclusive && FLAGS_column_families.empty()) {
      fprintf(stderr,
              "Error: --exclusive requires --column_families to be non-empty\n");
      fprintf(stderr,
              "       Use --exclusive with --column_families to compact all CFs "
              "EXCEPT the listed ones\n");
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

    if (FLAGS_block_size <= 0) {
      fprintf(stderr, "Error: --block_size must be positive\n");
      return false;
    }

    if (FLAGS_block_size > 1024) {
      fprintf(stderr,
              "Warning: --block_size=%dKB is very large (max recommended: "
              "1024KB)\n",
              FLAGS_block_size);
    }

    return true;
  }

  Status OpenDatabase() {
    printf("Opening database: %s\n", FLAGS_db.c_str());

    // Load database options
    DBOptions loaded_db_options;
    std::vector<ColumnFamilyDescriptor> loaded_cf_descs;
    ConfigOptions config_options;
    Status s;

    if (!FLAGS_option_file.empty()) {
      // Load options from specified OPTIONS file
      printf("Loading options from file: %s\n", FLAGS_option_file.c_str());
      s = LoadOptionsFromFile(config_options, FLAGS_option_file,
                              &loaded_db_options, &loaded_cf_descs);
      if (!s.ok()) {
        fprintf(stderr, "Failed to load options from file '%s': %s\n",
                FLAGS_option_file.c_str(), s.ToString().c_str());
        return s;
      }
      printf("Successfully loaded options from specified file\n");
    } else {
      // Load latest OPTIONS file from database directory
      printf("Loading latest options from database directory\n");
      s = LoadLatestOptions(config_options, FLAGS_db, &loaded_db_options,
                            &loaded_cf_descs);
      if (!s.ok()) {
        fprintf(stderr, "Failed to load existing database options: %s\n",
                s.ToString().c_str());
        fprintf(stderr, "Cannot proceed without existing options.\n");
        return s;
      }
      printf("Successfully loaded existing database options\n");
    }

    // Extract column family names from loaded descriptors
    cf_names_.clear();
    for (const auto& cf_desc : loaded_cf_descs) {
      cf_names_.push_back(cf_desc.name);
    }

    printf("Found %zu column families:\n", cf_names_.size());
    for (const auto& name : cf_names_) {
      printf("  - %s\n", name.c_str());
    }

    // Parse user-specified column families
    std::vector<std::string> specified_cf_names;
    std::vector<std::string> target_cf_names;

    if (!FLAGS_column_families.empty()) {
      std::stringstream ss(FLAGS_column_families);
      std::string cf_name;
      while (std::getline(ss, cf_name, ',')) {
        // Trim whitespace
        cf_name.erase(0, cf_name.find_first_not_of(" \t\n\r\f\v"));
        cf_name.erase(cf_name.find_last_not_of(" \t\n\r\f\v") + 1);

        if (!cf_name.empty()) {
          specified_cf_names.push_back(cf_name);
        }
      }

      // Validate specified CF names exist
      for (const auto& name : specified_cf_names) {
        if (std::find(cf_names_.begin(), cf_names_.end(), name) ==
            cf_names_.end()) {
          fprintf(stderr, "Error: Column family '%s' does not exist\n",
                  name.c_str());
          return Status::InvalidArgument("Column family not found");
        }
      }

      // Determine target CFs based on exclusive flag
      if (FLAGS_exclusive) {
        // Exclusive mode: compact all CFs EXCEPT the specified ones
        for (const auto& name : cf_names_) {
          if (std::find(specified_cf_names.begin(), specified_cf_names.end(),
                        name) == specified_cf_names.end()) {
            target_cf_names.push_back(name);
          }
        }
        printf("\nExclusive mode: Will compact all CFs EXCEPT:\n");
        for (const auto& name : specified_cf_names) {
          printf("  - %s (excluded)\n", name.c_str());
        }
        printf("\nActual CFs to compact (%zu):\n", target_cf_names.size());
        for (const auto& name : target_cf_names) {
          printf("  - %s\n", name.c_str());
        }
      } else {
        // Normal mode: compact only the specified CFs
        target_cf_names = specified_cf_names;
        printf("\nWill compact %zu column families:\n", target_cf_names.size());
        for (const auto& name : target_cf_names) {
          printf("  - %s\n", name.c_str());
        }
      }
    } else {
      target_cf_names = cf_names_;
      printf("\nWill compact all column families\n");
    }

    // Override performance-related DB options
    loaded_db_options.max_background_jobs = FLAGS_max_background_jobs;
    loaded_db_options.compaction_readahead_size =
        FLAGS_compaction_readahead_size;
    loaded_db_options.max_subcompactions =
        static_cast<uint32_t>(FLAGS_max_subcompactions);

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

    // Override compression ONLY for user-specified column families
    for (auto& cf_desc : loaded_cf_descs) {
      // Check if this CF is in the target list (or if we're compacting all CFs)
      if (target_cf_names.empty() ||
          std::find(target_cf_names.begin(), target_cf_names.end(),
                    cf_desc.name) != target_cf_names.end()) {
        printf("Setting compression to %s for CF: %s\n",
               FLAGS_compression_type.c_str(), cf_desc.name.c_str());
        cf_desc.options.compression = compression;

        // Set block_size for BlockBasedTable
        BlockBasedTableOptions table_options;

        // Try to get existing table options if available
        if (cf_desc.options.table_factory == nullptr) {
          fprintf(stderr, "Failed loading table option for column family'%s\n",
                  cf_desc.name.c_str());
          return Status::InvalidArgument("No table option found");
        }
        auto* existing_options =
            cf_desc.options.table_factory->GetOptions<BlockBasedTableOptions>();
        if (existing_options == nullptr) {
          fprintf(stderr, "Failed loading table option for column family'%s\n",
                  cf_desc.name.c_str());
          return Status::InvalidArgument("No table option found");
        }
        table_options = *existing_options;  // Copy existing options

        // Override block_size
        table_options.block_size =
            FLAGS_block_size * 1024;  // Convert KB to bytes

        // Create new table factory with updated options
        cf_desc.options.table_factory.reset(
            NewBlockBasedTableFactory(table_options));

        printf("Setting block_size to %dKB for CF: %s\n", FLAGS_block_size,
               cf_desc.name.c_str());
      } else {
        printf("Keeping original compression for CF: %s\n",
               cf_desc.name.c_str());
      }
    }

    // Open database with modified options
    s = DB::Open(loaded_db_options, FLAGS_db, loaded_cf_descs, &cf_handles_,
                 &db_);
    if (!s.ok()) {
      return s;
    }

    printf("\n✓ Database opened successfully\n");
    return Status::OK();
  }

  Status PerformCompaction() {
    printf("\n=== Starting Compaction ===\n");
    printf("Configuration:\n");
    printf("  max_background_jobs: %d\n", FLAGS_max_background_jobs);
    printf("  max_subcompactions: %d\n", FLAGS_max_subcompactions);
    printf("  compression_type: %s\n", FLAGS_compression_type.c_str());
    printf("  block_size: %dKB\n", FLAGS_block_size);
    printf("  exclusive_manual_compaction: %s\n",
           FLAGS_exclusive_manual_compaction ? "true" : "false");
    printf("  exclusive (invert CF selection): %s\n",
           FLAGS_exclusive ? "true" : "false");
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

    // Parse specified column families and compute target CFs
    std::vector<std::string> specified_cf_names;
    std::vector<std::string> target_cf_names;

    if (!FLAGS_column_families.empty()) {
      std::stringstream ss(FLAGS_column_families);
      std::string cf_name;
      while (std::getline(ss, cf_name, ',')) {
        cf_name.erase(0, cf_name.find_first_not_of(" \t\n\r\f\v"));
        cf_name.erase(cf_name.find_last_not_of(" \t\n\r\f\v") + 1);
        if (!cf_name.empty()) {
          specified_cf_names.push_back(cf_name);
        }
      }

      // Determine target CFs based on exclusive flag
      if (FLAGS_exclusive) {
        // Exclusive mode: compact all CFs EXCEPT the specified ones
        for (const auto& name : cf_names_) {
          if (std::find(specified_cf_names.begin(), specified_cf_names.end(),
                        name) == specified_cf_names.end()) {
            target_cf_names.push_back(name);
          }
        }
      } else {
        // Normal mode: compact only the specified CFs
        target_cf_names = specified_cf_names;
      }
    } else {
      target_cf_names = cf_names_;
    }

    // Compact each column family
    for (size_t i = 0; i < cf_handles_.size(); ++i) {
      const std::string& cf_name = cf_names_[i];

      // Check if this CF should be compacted
      bool should_compact =
          std::find(target_cf_names.begin(), target_cf_names.end(), cf_name) !=
          target_cf_names.end();

      if (!should_compact) {
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
      "  # Compact using a specific OPTIONS file\n"
      "  db_compact --db=/data/mydb --option_file=/path/to/OPTIONS-123456\n"
      "\n"
      "  # Compact specific column families\n"
      "  db_compact --db=/data/mydb --column_families=cf1,cf2,cf3\n"
      "\n"
      "  # Compact all CFs EXCEPT cf1 and cf2 (exclusive mode)\n"
      "  db_compact --db=/data/mydb --column_families=cf1,cf2 --exclusive=true\n"
      "\n"
      "  # Use a specific OPTIONS file with exclusive mode\n"
      "  db_compact --db=/data/mydb --option_file=/backup/OPTIONS-old \\\n"
      "             --column_families=default --exclusive=true\n"
      "\n"
      "  # Use ZSTD compression with high parallelism\n"
      "  db_compact --db=/data/mydb --compression_type=zstd \\\n"
      "             --max_background_jobs=32 --max_subcompactions=8\n"
      "\n"
      "  # Use custom block size (16KB)\n"
      "  db_compact --db=/data/mydb --block_size=16\n"
      "\n"
      "  # Exclusive manual compaction (no other compactions run)\n"
      "  db_compact --db=/data/mydb --exclusive_manual_compaction=true\n"
      "\n"
      "  # Compact all CFs except 'default' and 'metadata' with ZSTD\n"
      "  db_compact --db=/data/mydb --column_families=default,metadata \\\n"
      "             --exclusive=true --compression_type=zstd\n");

  ParseCommandLineFlags(&argc, &argv, true);

  ROCKSDB_NAMESPACE::DBCompactor compactor;
  return compactor.Run();
}

#endif  // GFLAGS
