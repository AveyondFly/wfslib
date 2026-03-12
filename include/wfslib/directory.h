/*
 * Copyright (C) 2017 koolkdev
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE file for details.
 */

#pragma once

#include <expected>
#include <memory>
#include <string>

#include "directory_iterator.h"
#include "directory_map.h"
#include "entry.h"
#include "wfs_errors.h"

class Area;
class Block;
class File;

class Directory : public Entry, public std::enable_shared_from_this<Directory> {
 public:
  using iterator = DirectoryIterator;

  // TODO: Replace name with tree iterator?
  Directory(std::string name, MetadataRef metadata, std::shared_ptr<QuotaArea> quota, std::shared_ptr<Block> block);

  std::expected<std::shared_ptr<Entry>, WfsError> GetEntry(std::string_view name) const;
  std::expected<std::shared_ptr<Directory>, WfsError> GetDirectory(std::string_view name) const;
  std::expected<std::shared_ptr<File>, WfsError> GetFile(std::string_view name) const;

  size_t size() const { return map_.size(); }

  iterator begin() const { return {map_.begin()}; }
  iterator end() const { return {map_.end()}; }

  iterator find(std::string_view key) const;

  // Create a new file in this directory with optional initial data
  // If data is empty, creates an empty file (category 0)
  // Otherwise, allocates appropriate blocks and writes the data
  std::expected<std::shared_ptr<File>, WfsError> CreateFile(std::string_view name,
                                                            std::span<const std::byte> data = {});

  // Create a new empty directory in this directory
  std::expected<std::shared_ptr<Directory>, WfsError> CreateDirectory(std::string_view name);

  // Delete a file from this directory
  // Returns true if the file was deleted, false if not found
  std::expected<void, WfsError> DeleteFile(std::string_view name);

  // Delete a directory from this directory
  // If recursive is true, deletes all contents recursively
  // If recursive is false, only deletes empty directories
  std::expected<void, WfsError> DeleteDirectory(std::string_view name, bool recursive = false);

  // Check if the directory is empty
  bool empty() const { return map_.size() == 0; }

  const std::shared_ptr<QuotaArea>& quota() const { return quota_; }

 private:
  friend class Recovery;

  std::shared_ptr<QuotaArea> quota_;
  std::shared_ptr<Block> block_;

  DirectoryMap map_;
};
