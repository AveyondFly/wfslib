/*
 * Copyright (C) 2017 koolkdev
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE file for details.
 */

#include "directory.h"

#include <bit>
#include <cstring>
#include <ctime>
#include <utility>

#include "device_encryption.h"
#include "directory_leaf_tree.h"
#include "file.h"
#include "quota_area.h"

Directory::Directory(std::string name,
                     MetadataRef metadata,
                     std::shared_ptr<QuotaArea> quota,
                     std::shared_ptr<Block> block)
    : Entry(std::move(name), std::move(metadata)),
      quota_(std::move(quota)),
      block_(std::move(block)),
      map_{quota_, block_} {}

std::expected<std::shared_ptr<Entry>, WfsError> Directory::GetEntry(std::string_view name) const {
  try {
    auto it = find(name);
    if (it.is_end()) {
      return std::unexpected(WfsError::kEntryNotFound);
    }
    return (*it).entry;
  } catch (const WfsException& e) {
    return std::unexpected(e.error());
  }
}

std::expected<std::shared_ptr<Directory>, WfsError> Directory::GetDirectory(std::string_view name) const {
  auto entry = GetEntry(name);
  if (!entry.has_value())
    return std::unexpected(entry.error());
  if (!(*entry)->is_directory()) {
    // Not a directory
    return std::unexpected(kNotDirectory);
  }
  return std::dynamic_pointer_cast<Directory>(*entry);
}

std::expected<std::shared_ptr<File>, WfsError> Directory::GetFile(std::string_view name) const {
  auto entry = GetEntry(name);
  if (!entry.has_value())
    return std::unexpected(entry.error());
  if (!(*entry)->is_file()) {
    // Not a file
    return std::unexpected(kNotFile);
  }
  return std::dynamic_pointer_cast<File>(*entry);
}

Directory::iterator Directory::find(std::string_view key) const {
  std::string lowercase_key{key};
  // to lowercase
  std::ranges::transform(lowercase_key, lowercase_key.begin(), [](char c) { return std::tolower(c); });
  return {map_.find(lowercase_key)};
}

std::expected<std::shared_ptr<File>, WfsError> Directory::CreateFile(std::string_view name,
                                                                        std::span<const std::byte> data) {
  // Check if entry already exists
  if (!find(name).is_end()) {
    return std::unexpected(WfsError::kEntryAlreadyExists);
  }

  // Convert name to lowercase for storage
  std::string lowercase_name{name};
  std::ranges::transform(lowercase_name, lowercase_name.begin(), [](char c) { return std::tolower(c); });

  // Calculate base metadata size (without data)
  size_t case_bitmap_size = (lowercase_name.size() + 7) / 8;
  size_t base_metadata_size = offsetof(EntryMetadata, case_bitmap) + case_bitmap_size;

  // Determine category based on data size
  // Category 0: data stored directly in metadata (limited space)
  // Category 1: data in up to 5 single blocks
  // Category 2: data in up to 5 large blocks (8 blocks each)
  // Category 3: data in up to 4 clusters (64 blocks each)
  // Category 4: indirect block lists (large files)

  size_t block_size = quota_->block_size();
  size_t large_block_size = block_size << log2_size(BlockType::Large);   // 8 blocks
  size_t cluster_block_size = block_size << log2_size(BlockType::Cluster);  // 64 blocks

  uint8_t category = 0;
  uint32_t size_on_disk = static_cast<uint32_t>(data.size());

  // Calculate available space in metadata for category 0
  // Metadata size must be power of 2, and we need space for data after EntryMetadata
  size_t max_category0_data_size = 0;
  for (size_t log2_size = std::bit_width(base_metadata_size); log2_size <= 12; ++log2_size) {
    size_t available = (size_t{1} << log2_size) - base_metadata_size;
    if (available >= data.size()) {
      max_category0_data_size = available;
      break;
    }
  }

  if (data.size() <= max_category0_data_size) {
    category = 0;
    size_on_disk = static_cast<uint32_t>(data.size());
  } else if (data.size() <= 5 * block_size) {
    category = 1;
    size_on_disk = static_cast<uint32_t>(data.size());
  } else if (data.size() <= 5 * large_block_size) {
    category = 2;
    size_on_disk = static_cast<uint32_t>(data.size());
  } else if (data.size() <= 4 * cluster_block_size) {
    category = 3;
    size_on_disk = static_cast<uint32_t>(data.size());
  } else {
    category = 4;
    size_on_disk = static_cast<uint32_t>(data.size());
  }

  // Calculate total metadata size including data block descriptors
  size_t metadata_data_size = 0;
  if (category == 0) {
    metadata_data_size = data.size();
  } else if (category == 1 || category == 2) {
    // DataBlockMetadata for each block (reversed in memory)
    size_t blocks_needed = div_ceil(data.size(), (category == 1) ? block_size : large_block_size);
    metadata_data_size = blocks_needed * sizeof(DataBlockMetadata);
  } else if (category == 3) {
    // DataBlocksClusterMetadata for each cluster
    size_t clusters_needed = div_ceil(data.size(), cluster_block_size);
    metadata_data_size = clusters_needed * sizeof(DataBlocksClusterMetadata);
  }
  // Category 4 would need indirect block handling - not implemented in this version

  size_t total_metadata_size = base_metadata_size + metadata_data_size;
  // Align to power of 2, minimum size is 32 bytes (2^5)
  size_t aligned_metadata_size = size_t{1} << std::max<size_t>(5, std::bit_width(total_metadata_size - 1));

  // Allocate and prepare data blocks for category 1+
  std::vector<uint32_t> allocated_blocks;
  std::vector<std::vector<uint8_t>> block_hashes;

  if (category >= 1 && category <= 2 && !data.empty()) {
    BlockType block_type = (category == 1) ? BlockType::Single : BlockType::Large;
    size_t data_block_size = (category == 1) ? block_size : large_block_size;
    size_t blocks_needed = div_ceil(data.size(), data_block_size);

    auto blocks = quota_->AllocDataBlocks(static_cast<uint32_t>(blocks_needed), block_type);
    if (!blocks.has_value()) {
      return std::unexpected(blocks.error());
    }
    allocated_blocks = std::move(*blocks);

    // Write data to each block and calculate hash
    for (size_t i = 0; i < allocated_blocks.size(); ++i) {
      size_t offset = i * data_block_size;
      size_t chunk_size = std::min(data.size() - offset, data_block_size);

      // Load the data block for writing
      auto data_block = quota_->LoadDataBlock(
          allocated_blocks[i],
          static_cast<BlockSize>(quota_->block_size_log2()),
          block_type,
          static_cast<uint32_t>(chunk_size),
          {nullptr, 0},  // hash will be calculated on flush
          true,  // encrypted
          true   // new_block
      );
      if (!data_block.has_value()) {
        // Rollback allocated blocks
        for (uint32_t blk : allocated_blocks) {
          quota_->DeleteBlocks(blk, 1);
        }
        return std::unexpected(data_block.error());
      }

      // Copy data to block
      auto mutable_data = (*data_block)->mutable_data();
      std::copy(data.begin() + offset, data.begin() + offset + chunk_size, mutable_data.begin());

      // Calculate and store hash
      std::vector<uint8_t> hash(DeviceEncryption::DIGEST_SIZE);
      DeviceEncryption::CalculateHash(
          std::span<const std::byte>(mutable_data.data(), chunk_size),
          std::span<std::byte>(reinterpret_cast<std::byte*>(hash.data()), hash.size())
      );
      block_hashes.push_back(std::move(hash));

      // Block will be flushed when it goes out of scope
    }
  }

  // Create EntryMetadata
  EntryMetadata metadata{};
  std::memset(&metadata, 0, sizeof(metadata));
  metadata.flags = 0;  // Regular file (no DIRECTORY flag)
  metadata.size_on_disk = size_on_disk;
  metadata.file_size = size_on_disk;
  metadata.size_category = category;
  metadata.ctime = static_cast<uint32_t>(std::time(nullptr));
  metadata.mtime = metadata.ctime;
  metadata.directory_block_number = 0;
  metadata.permissions.owner = 0;
  metadata.permissions.group = 0;
  metadata.permissions.mode = 0644;  // Default file permissions
  metadata.filename_length = static_cast<uint8_t>(lowercase_name.size());
  metadata.metadata_log2_size = static_cast<uint8_t>(std::bit_width(aligned_metadata_size - 1));

  // Build case bitmap: uppercase letters have their bit set
  uint8_t case_bitmap = 0;
  for (size_t i = 0; i < name.size(); ++i) {
    if (std::isupper(static_cast<unsigned char>(name[i]))) {
      case_bitmap |= (1 << (i % 8));
    }
    if (i % 8 == 7) {
      break;
    }
  }
  metadata.case_bitmap = case_bitmap;

  // Note: For category 0, data is stored after EntryMetadata in the same allocation
  // For category 1+, DataBlockMetadata list is stored after EntryMetadata
  // The actual data block info needs to be written after the base metadata
  // This is handled by the DirectoryMap::insert which copies the metadata

  // For now, we need to handle this differently - we need to allocate space in the directory tree
  // that includes the data block metadata

  // Insert into directory map
  if (!map_.insert(lowercase_name, &metadata)) {
    // Rollback allocated blocks
    for (uint32_t blk : allocated_blocks) {
      quota_->DeleteBlocks(blk, 1);
    }
    return std::unexpected(WfsError::kNoSpace);
  }

  // After successful insert, we need to update the metadata with block info
  // Re-fetch the entry to get the actual metadata location
  auto entry_result = GetFile(name);
  if (!entry_result.has_value()) {
    return std::unexpected(entry_result.error());
  }

  auto file = entry_result.value();
  EntryMetadata* file_metadata = file->mutable_metadata();

  // Write data block info to metadata for category 1+
  if (category >= 1 && category <= 2 && !allocated_blocks.empty()) {
    // The DataBlockMetadata array is stored at the end of the metadata, reversed
    size_t blocks_count = allocated_blocks.size();
    std::byte* metadata_end = reinterpret_cast<std::byte*>(file_metadata) + aligned_metadata_size;

    // DataBlockMetadata is stored reversed at the end of metadata
    for (size_t i = 0; i < blocks_count; ++i) {
      DataBlockMetadata* block_meta = reinterpret_cast<DataBlockMetadata*>(
          metadata_end - (i + 1) * sizeof(DataBlockMetadata)
      );
      block_meta->block_number = allocated_blocks[i];
      std::memcpy(block_meta->hash, block_hashes[i].data(), DeviceEncryption::DIGEST_SIZE);
    }
  }

  // For category 0, copy data directly after base metadata
  if (category == 0 && !data.empty()) {
    std::byte* data_start = reinterpret_cast<std::byte*>(file_metadata) + base_metadata_size;
    std::copy(data.begin(), data.end(), data_start);
  }

  return file;
}

std::expected<std::shared_ptr<Directory>, WfsError> Directory::CreateDirectory(std::string_view name) {
  // Check if entry already exists
  if (!find(name).is_end()) {
    return std::unexpected(WfsError::kEntryAlreadyExists);
  }

  // Convert name to lowercase for storage
  std::string lowercase_name{name};
  std::ranges::transform(lowercase_name, lowercase_name.begin(), [](char c) { return std::tolower(c); });

  // Allocate a new block for the directory
  auto new_block = quota_->AllocMetadataBlock();
  if (!new_block.has_value()) {
    return std::unexpected(new_block.error());
  }

  // Initialize the new directory block as an empty leaf tree
  DirectoryLeafTree new_tree{*new_block};
  new_tree.Init(/*is_root=*/true);

  // Get the area block number for the new directory
  uint32_t dir_block_number = quota_->to_area_block_number((*new_block)->physical_block_number());

  // Create EntryMetadata for the new directory
  EntryMetadata metadata{};
  std::memset(&metadata, 0, sizeof(metadata));
  metadata.flags = EntryMetadata::Flags::DIRECTORY;
  metadata.size_on_disk = 0;
  metadata.file_size = 0;
  metadata.size_category = 0;
  metadata.ctime = static_cast<uint32_t>(std::time(nullptr));
  metadata.mtime = metadata.ctime;
  metadata.directory_block_number = dir_block_number;
  metadata.permissions.owner = 0;
  metadata.permissions.group = 0;
  metadata.permissions.mode = 0755;  // Default directory permissions
  metadata.filename_length = static_cast<uint8_t>(lowercase_name.size());

  // Calculate metadata size
  size_t case_bitmap_size = (lowercase_name.size() + 7) / 8;
  size_t metadata_size = offsetof(EntryMetadata, case_bitmap) + case_bitmap_size;
  metadata.metadata_log2_size = static_cast<uint8_t>(std::bit_width(metadata_size - 1));

  // Build case bitmap
  uint8_t case_bitmap = 0;
  for (size_t i = 0; i < name.size(); ++i) {
    if (std::isupper(static_cast<unsigned char>(name[i]))) {
      case_bitmap |= (1 << (i % 8));
    }
    if (i % 8 == 7) {
      break;
    }
  }
  metadata.case_bitmap = case_bitmap;

  // Insert into directory map
  if (!map_.insert(lowercase_name, &metadata)) {
    // Failed to insert, free the allocated block
    quota_->DeleteBlocks(dir_block_number, 1);
    return std::unexpected(WfsError::kNoSpace);
  }

  // Return the newly created directory
  return GetDirectory(name);
}

std::expected<void, WfsError> Directory::DeleteFile(std::string_view name) {
  // Find the entry
  auto it = find(name);
  if (it.is_end()) {
    return std::unexpected(WfsError::kEntryNotFound);
  }

  auto entry_opt = (*it).entry;
  if (!entry_opt.has_value() || !entry_opt.value()->is_file()) {
    return std::unexpected(WfsError::kNotFile);
  }

  auto file = std::dynamic_pointer_cast<File>(entry_opt.value());
  uint8_t category = file->metadata()->size_category.value();

  // Get block size info
  size_t block_size = quota_->block_size();
  size_t large_block_size = block_size << log2_size(BlockType::Large);   // 8 blocks
  size_t cluster_block_size = block_size << log2_size(BlockType::Cluster);  // 64 blocks

  // Free data blocks based on category
  if (category >= 1 && category <= 2) {
    // Category 1-2: Data in single or large blocks
    BlockType block_type = (category == 1) ? BlockType::Single : BlockType::Large;
    size_t data_block_size = (category == 1) ? block_size : large_block_size;
    size_t blocks_count = div_ceil(file->Size(), data_block_size);

    // Get the metadata location
    size_t aligned_size = size_t{1} << file->metadata()->metadata_log2_size.value();
    std::byte* metadata_end = reinterpret_cast<std::byte*>(file->mutable_metadata()) + aligned_size;

    // DataBlockMetadata is stored reversed at the end of metadata
    for (size_t i = 0; i < blocks_count; ++i) {
      DataBlockMetadata* block_meta = reinterpret_cast<DataBlockMetadata*>(
          metadata_end - (i + 1) * sizeof(DataBlockMetadata)
      );
      uint32_t block_number = block_meta->block_number.value();
      uint32_t blocks_to_delete = (block_type == BlockType::Large) ? 8 : 1;
      quota_->DeleteBlocks(block_number, blocks_to_delete);
    }
  } else if (category == 3) {
    // Category 3: Data in clusters
    size_t clusters_count = div_ceil(file->Size(), cluster_block_size);

    size_t aligned_size = size_t{1} << file->metadata()->metadata_log2_size.value();
    std::byte* metadata_end = reinterpret_cast<std::byte*>(file->mutable_metadata()) + aligned_size;

    for (size_t i = 0; i < clusters_count; ++i) {
      DataBlocksClusterMetadata* cluster_meta = reinterpret_cast<DataBlocksClusterMetadata*>(
          metadata_end - (i + 1) * sizeof(DataBlocksClusterMetadata)
      );
      uint32_t block_number = cluster_meta->block_number.value();
      // A cluster is 64 blocks
      quota_->DeleteBlocks(block_number, 64);
    }
  } else if (category == 4) {
    // Category 4: Indirect block lists
    size_t clusters_per_block = (block_size - sizeof(MetadataBlockHeader)) / sizeof(DataBlocksClusterMetadata);
    clusters_per_block = std::min(clusters_per_block, size_t{48});
    size_t clusters_count = div_ceil(file->Size(), cluster_block_size);
    size_t metadata_blocks_count = div_ceil(clusters_count, clusters_per_block);

    size_t aligned_size = size_t{1} << file->metadata()->metadata_log2_size.value();
    std::byte* metadata_end = reinterpret_cast<std::byte*>(file->mutable_metadata()) + aligned_size;

    // Get the list of metadata block numbers
    for (size_t meta_idx = 0; meta_idx < metadata_blocks_count; ++meta_idx) {
      uint32_be_t* meta_block_ptr = reinterpret_cast<uint32_be_t*>(
          metadata_end - (meta_idx + 1) * sizeof(uint32_be_t)
      );
      uint32_t meta_block_number = meta_block_ptr->value();

      // Load the metadata block to get cluster info
      auto meta_block = quota_->LoadMetadataBlock(meta_block_number);
      if (!meta_block.has_value()) {
        continue;  // Skip corrupted metadata block
      }

      // Get clusters from this metadata block
      size_t clusters_in_this_block = std::min(clusters_count - meta_idx * clusters_per_block, clusters_per_block);
      auto* clusters = (*meta_block)->get_object<DataBlocksClusterMetadata>(sizeof(MetadataBlockHeader));

      for (size_t cluster_idx = 0; cluster_idx < clusters_in_this_block; ++cluster_idx) {
        uint32_t block_number = clusters[cluster_idx].block_number.value();
        quota_->DeleteBlocks(block_number, 64);
      }

      // Delete the metadata block itself
      quota_->DeleteBlocks(meta_block_number, 1);
    }
  }
  // Category 0: Data is inline in metadata, no separate blocks to free

  // Remove from directory map
  std::string name_str(name);
  if (!map_.erase(name_str)) {
    return std::unexpected(WfsError::kEntryNotFound);
  }

  return {};
}

std::expected<void, WfsError> Directory::DeleteDirectory(std::string_view name, bool recursive) {
  // Find the entry
  auto it = find(name);
  if (it.is_end()) {
    return std::unexpected(WfsError::kEntryNotFound);
  }

  auto entry_opt = (*it).entry;
  if (!entry_opt.has_value() || !entry_opt.value()->is_directory()) {
    return std::unexpected(WfsError::kNotDirectory);
  }

  auto dir = std::dynamic_pointer_cast<Directory>(entry_opt.value());

  // Check if directory is empty
  if (!dir->empty()) {
    if (!recursive) {
      return std::unexpected(WfsError::kDirectoryNotEmpty);
    }

    // Recursive delete: delete all contents first
    // Collect all entry names first to avoid iterator invalidation
    std::vector<std::string> entries_to_delete;
    for (const auto& item : *dir) {
      if (item.entry.has_value()) {
        entries_to_delete.push_back(std::string(item.name));
      }
    }

    // Delete each entry
    for (const auto& entry_name : entries_to_delete) {
      auto sub_entry = dir->GetEntry(entry_name);
      if (!sub_entry.has_value()) {
        continue;
      }

      if ((*sub_entry)->is_directory()) {
        auto result = dir->DeleteDirectory(entry_name, true);
        if (!result.has_value()) {
          return result;
        }
      } else if ((*sub_entry)->is_file()) {
        auto result = dir->DeleteFile(entry_name);
        if (!result.has_value()) {
          return result;
        }
      }
    }
  }

  // Get the directory block number
  uint32_t dir_block_number = dir->metadata()->directory_block_number.value();

  // Remove from directory map
  std::string name_str(name);
  if (!map_.erase(name_str)) {
    return std::unexpected(WfsError::kEntryNotFound);
  }

  // Free the directory block
  quota_->DeleteBlocks(dir_block_number, 1);

  return {};
}
