#include "gcc_compat.h"
/*
 * Copyright (C) 2017 koolkdev
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE file for details.
 */

#include "area.h"

#include <random>

#include "wfs_device.h"

Area::Area(std::shared_ptr<WfsDevice> wfs_device, std::shared_ptr<Block> header_block)
    : wfs_device_(std::move(wfs_device)), header_block_(std::move(header_block)) {}

std::span<const WfsAreaFragmentInfo> Area::get_fragments() const {
  return header()->first_fragments;
}

size_t Area::get_fragments_count() const {
  auto fragments = get_fragments();
  size_t count = 0;
  for (const auto& frag : fragments) {
    if (frag.blocks_count.value() == 0)
      break;
    ++count;
  }
  return count;
}

uint32_t Area::to_physical_block_number(uint32_t area_block_number) const {
  if (is_root_area()) {
    return to_physical_blocks_count(area_block_number);
  }

  // Check if fragments are initialized (fragments_log2_block_size > 0 means fragments are valid)
  size_t frag_block_size_log2 = fragments_log2_block_size();
  if (frag_block_size_log2 == 0) {
    // No fragments info, use direct mapping
    return header_block_->physical_block_number() + to_physical_blocks_count(area_block_number);
  }

  auto fragments = get_fragments();
  size_t frag_count = get_fragments_count();
  if (frag_count == 0) {
    // No fragments, use direct mapping
    return header_block_->physical_block_number() + to_physical_blocks_count(area_block_number);
  }

  size_t area_block_size_log2 = block_size_log2();

  // Convert area block number to fragment block units
  uint32_t offset_in_frag_blocks;
  uint32_t offset_within_frag_block;

  if (area_block_size_log2 >= frag_block_size_log2) {
    // Area block >= fragment block: one area block contains multiple fragment blocks
    uint32_t shift = static_cast<uint32_t>(area_block_size_log2 - frag_block_size_log2);
    offset_in_frag_blocks = area_block_number << shift;
    offset_within_frag_block = 0;
  } else {
    // Area block < fragment block: one fragment block contains multiple area blocks
    uint32_t shift = static_cast<uint32_t>(frag_block_size_log2 - area_block_size_log2);
    offset_in_frag_blocks = area_block_number >> shift;
    offset_within_frag_block = area_block_number & ((1 << shift) - 1);
  }

  // Find which fragment contains this block
  uint32_t cumulative_blocks = 0;
  for (size_t i = 0; i < frag_count; ++i) {
    uint32_t frag_blocks = fragments[i].blocks_count.value();
    if (offset_in_frag_blocks < cumulative_blocks + frag_blocks) {
      uint32_t offset_in_this_frag = offset_in_frag_blocks - cumulative_blocks;
      uint32_t physical_frag_block = fragments[i].block_number.value() + offset_in_this_frag;
      // Convert to physical block units
      uint32_t physical_block = (physical_frag_block << (frag_block_size_log2 - log2_size(BlockSize::Physical))) +
                                to_physical_blocks_count(offset_within_frag_block);
      return physical_block;
    }
    cumulative_blocks += frag_blocks;
  }

  // Fallback (should not reach here for valid area_block_number)
  return header_block_->physical_block_number() + to_physical_blocks_count(area_block_number);
}

uint32_t Area::to_area_block_number(uint32_t physical_block_number) const {
  if (is_root_area()) {
    return to_area_blocks_count(physical_block_number);
  }

  // Check if fragments are initialized
  size_t frag_block_size_log2 = fragments_log2_block_size();
  if (frag_block_size_log2 == 0) {
    // No fragments info, use direct mapping
    return to_area_blocks_count(physical_block_number - header_block_->physical_block_number());
  }

  auto fragments = get_fragments();
  size_t frag_count = get_fragments_count();
  if (frag_count == 0) {
    // No fragments, use direct mapping
    return to_area_blocks_count(physical_block_number - header_block_->physical_block_number());
  }

  size_t area_block_size_log2 = block_size_log2();

  // Convert physical block to fragment block units
  uint32_t physical_in_frag_blocks = physical_block_number >> (frag_block_size_log2 - log2_size(BlockSize::Physical));
  uint32_t offset_within_frag_block =
      physical_block_number & ((1 << (frag_block_size_log2 - log2_size(BlockSize::Physical))) - 1);

  // Find which fragment contains this physical block
  uint32_t cumulative_area_blocks = 0;
  for (size_t i = 0; i < frag_count; ++i) {
    uint32_t frag_start = fragments[i].block_number.value();
    uint32_t frag_blocks = fragments[i].blocks_count.value();
    if (physical_in_frag_blocks >= frag_start && physical_in_frag_blocks < frag_start + frag_blocks) {
      uint32_t offset_in_frag = physical_in_frag_blocks - frag_start;
      // Convert to area block units
      uint32_t area_block;
      if (area_block_size_log2 >= frag_block_size_log2) {
        uint32_t shift = static_cast<uint32_t>(area_block_size_log2 - frag_block_size_log2);
        area_block = (cumulative_area_blocks + offset_in_frag) << shift;
      } else {
        uint32_t shift = static_cast<uint32_t>(frag_block_size_log2 - area_block_size_log2);
        area_block = cumulative_area_blocks + (offset_in_frag >> shift);
      }
      return area_block + to_area_blocks_count(offset_within_frag_block);
    }
    // Update cumulative area blocks for next fragment
    if (area_block_size_log2 >= frag_block_size_log2) {
      uint32_t shift = static_cast<uint32_t>(area_block_size_log2 - frag_block_size_log2);
      cumulative_area_blocks += frag_blocks << shift;
    } else {
      uint32_t shift = static_cast<uint32_t>(frag_block_size_log2 - area_block_size_log2);
      cumulative_area_blocks += frag_blocks >> shift;
    }
  }

  // Fallback
  return to_area_blocks_count(physical_block_number - header_block_->physical_block_number());
}

void Area::Init(std::shared_ptr<Area> parent_area, uint32_t blocks_count, BlockSize block_size) {
  std::random_device rand_device;
  std::default_random_engine rand_engine{rand_device()};
  std::uniform_int_distribution<uint32_t> random_iv_generator(std::numeric_limits<uint32_t>::min(),
                                                              std::numeric_limits<uint32_t>::max());

  auto* header = mutable_header();
  std::fill(reinterpret_cast<std::byte*>(header), reinterpret_cast<std::byte*>(header + 1), std::byte{0});
  header->iv = random_iv_generator(rand_engine);
  header->blocks_count = blocks_count;
  header->depth = parent_area ? parent_area->header()->depth.value() + 1 : 0;
  header->block_size_log2 = static_cast<uint8_t>(block_size);
  header->large_block_size_log2 = static_cast<uint8_t>(header->block_size_log2.value() + log2_size(BlockType::Large));
  header->cluster_block_size_log2 =
      static_cast<uint8_t>(header->block_size_log2.value() + log2_size(BlockType::Cluster));
  header->maybe_always_zero = 0;
  header->remainder_blocks_count = 0;
}

std::expected<std::shared_ptr<Area>, WfsError> Area::GetArea(uint32_t area_block_number, BlockSize size) {
  auto area_metadata_block = LoadMetadataBlock(area_block_number, size);
  if (!area_metadata_block.has_value())
    return std::unexpected(WfsError::kAreaHeaderCorrupted);
  return std::make_shared<Area>(wfs_device_, std::move(*area_metadata_block));
}

std::expected<std::shared_ptr<Block>, WfsError> Area::LoadMetadataBlock(uint32_t area_block_number,
                                                                        bool new_block) const {
  return LoadMetadataBlock(area_block_number, static_cast<BlockSize>(block_size_log2()), new_block);
}

std::expected<std::shared_ptr<Block>, WfsError> Area::LoadMetadataBlock(uint32_t area_block_number,
                                                                        BlockSize block_size,
                                                                        bool new_block) const {
  return wfs_device_->LoadMetadataBlock(this, to_physical_block_number(area_block_number), block_size, new_block);
}

std::expected<std::shared_ptr<Block>, WfsError> Area::LoadDataBlock(uint32_t area_block_number,
                                                                    BlockSize block_size,
                                                                    BlockType block_type,
                                                                    uint32_t data_size,
                                                                    Block::HashRef data_hash,
                                                                    bool encrypted,
                                                                    bool new_block) const {
  return wfs_device_->LoadDataBlock(this, to_physical_block_number(area_block_number), block_size, block_type,
                                    data_size, std::move(data_hash), encrypted, new_block);
}
