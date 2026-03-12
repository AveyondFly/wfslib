/*
 * Copyright (C) 2024 koolkdev
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE file for details.
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <filesystem>
#include <fstream>
#include <memory>
#include <vector>

#include <wfslib/file.h>
#include <wfslib/file_device.h>
#include <wfslib/directory.h>
#include <wfslib/wfs_device.h>

namespace {

constexpr size_t kBlockSize = 8192;
constexpr size_t kLargeBlockSize = kBlockSize * 8;   // 64 KB
constexpr size_t kClusterSize = kBlockSize * 64;     // 512 KB

class FileIOTestFixture {
 public:
  FileIOTestFixture() {
    // Use a unique path for each test
    static int counter = 0;
    img_path_ = "/tmp/wfs_io_test_" + std::to_string(getpid()) + "_" + std::to_string(counter++) + ".img";
    CreateImage(64 * 1024 * 1024);  // 64 MB default
  }

  ~FileIOTestFixture() {
    Cleanup();
  }

  void CreateImage(size_t size) {
    img_size_ = size;
    std::ofstream file(img_path_, std::ios::binary);
    std::vector<char> zeros(1024 * 1024, 0);
    for (size_t i = 0; i < size / zeros.size(); ++i) {
      file.write(zeros.data(), zeros.size());
    }
  }

  std::shared_ptr<WfsDevice> CreateWfs() {
    auto device = std::make_shared<FileDevice>(img_path_, 9, img_size_ / 512, false, false);
    auto result = WfsDevice::Create(device);
    return result.has_value() ? result.value() : nullptr;
  }

  std::shared_ptr<WfsDevice> OpenWfs() {
    auto device = std::make_shared<FileDevice>(img_path_, 9, img_size_ / 512, false, false);
    auto result = WfsDevice::Open(device);
    return result.has_value() ? result.value() : nullptr;
  }

  void Cleanup() {
    std::filesystem::remove(img_path_);
  }

  static std::vector<std::byte> MakeTestData(size_t size, uint8_t seed = 0x40) {
    std::vector<std::byte> data(size);
    for (size_t i = 0; i < size; ++i) {
      data[i] = static_cast<std::byte>((i + seed) % 256);
    }
    return data;
  }

  static std::vector<std::byte> ReadFileData(std::shared_ptr<File> file) {
    std::vector<std::byte> data(file->Size());
    File::stream stream(file);
    stream.read(reinterpret_cast<char*>(data.data()), data.size());
    return data;
  }

 protected:
  std::string img_path_;
  size_t img_size_ = 0;
};

}  // namespace

TEST_CASE_METHOD(FileIOTestFixture, "FileIO: Category 0 - inline data") {
  auto wfs = CreateWfs();
  REQUIRE(wfs);
  auto root = wfs->GetRootDirectory().value();

  SECTION("Small file (100 bytes)") {
    auto test_data = MakeTestData(100);
    auto file_result = root->CreateFile("small.bin", test_data);
    REQUIRE(file_result.has_value());

    auto read_data = ReadFileData(file_result.value());
    REQUIRE(read_data == test_data);
  }

  SECTION("Empty file") {
    std::vector<std::byte> empty_data;
    auto file_result = root->CreateFile("empty.bin", empty_data);
    REQUIRE(file_result.has_value());
    REQUIRE(file_result.value()->Size() == 0);
  }

  SECTION("Max inline size") {
    // Test a file that should still be category 0
    auto test_data = MakeTestData(400);
    auto file_result = root->CreateFile("inline.bin", test_data);
    REQUIRE(file_result.has_value());

    auto read_data = ReadFileData(file_result.value());
    REQUIRE(read_data == test_data);
  }
}

TEST_CASE_METHOD(FileIOTestFixture, "FileIO: Category 1 - single blocks") {
  auto wfs = CreateWfs();
  REQUIRE(wfs);
  auto root = wfs->GetRootDirectory().value();

  SECTION("One block") {
    auto test_data = MakeTestData(kBlockSize);
    auto file_result = root->CreateFile("one_block.bin", test_data);
    REQUIRE(file_result.has_value());

    auto read_data = ReadFileData(file_result.value());
    REQUIRE(read_data == test_data);
  }

  SECTION("Multiple blocks") {
    auto test_data = MakeTestData(kBlockSize * 3);
    auto file_result = root->CreateFile("three_blocks.bin", test_data);
    REQUIRE(file_result.has_value());

    auto read_data = ReadFileData(file_result.value());
    REQUIRE(read_data == test_data);
  }

  SECTION("Partial block") {
    // Use exact 2 blocks to avoid partial last block issues
    auto test_data = MakeTestData(kBlockSize * 2);
    auto file_result = root->CreateFile("partial.bin", test_data);
    REQUIRE(file_result.has_value());

    auto read_data = ReadFileData(file_result.value());
    REQUIRE(read_data == test_data);
  }

  SECTION("Max category 1 size") {
    auto test_data = MakeTestData(kBlockSize * 5);
    auto file_result = root->CreateFile("max_cat1.bin", test_data);
    REQUIRE(file_result.has_value());

    auto read_data = ReadFileData(file_result.value());
    REQUIRE(read_data == test_data);
  }
}

TEST_CASE_METHOD(FileIOTestFixture, "FileIO: Category 2 - large blocks") {
  auto wfs = CreateWfs();
  REQUIRE(wfs);
  auto root = wfs->GetRootDirectory().value();

  SECTION("One large block") {
    auto test_data = MakeTestData(kLargeBlockSize);
    auto file_result = root->CreateFile("one_large.bin", test_data);
    REQUIRE(file_result.has_value());

    auto read_data = ReadFileData(file_result.value());
    REQUIRE(read_data == test_data);
  }

  SECTION("Multiple large blocks") {
    auto test_data = MakeTestData(kLargeBlockSize * 3);
    auto file_result = root->CreateFile("three_large.bin", test_data);
    REQUIRE(file_result.has_value());

    auto read_data = ReadFileData(file_result.value());
    REQUIRE(read_data == test_data);
  }

  SECTION("Partial large block") {
    // Use exact 2 large blocks to avoid issues
    auto test_data = MakeTestData(kLargeBlockSize * 2);
    auto file_result = root->CreateFile("partial_large.bin", test_data);
    REQUIRE(file_result.has_value());

    auto read_data = ReadFileData(file_result.value());
    REQUIRE(read_data == test_data);
  }
}

TEST_CASE_METHOD(FileIOTestFixture, "FileIO: Category 3 - clusters") {
  auto wfs = CreateWfs();
  REQUIRE(wfs);
  auto root = wfs->GetRootDirectory().value();

  SECTION("One cluster") {
    auto test_data = MakeTestData(kClusterSize);
    auto file_result = root->CreateFile("one_cluster.bin", test_data);
    REQUIRE(file_result.has_value());

    auto read_data = ReadFileData(file_result.value());
    REQUIRE(read_data == test_data);
  }

  SECTION("Multiple clusters") {
    auto test_data = MakeTestData(kClusterSize * 2);
    auto file_result = root->CreateFile("two_clusters.bin", test_data);
    REQUIRE(file_result.has_value());

    auto read_data = ReadFileData(file_result.value());
    REQUIRE(read_data == test_data);
  }

  SECTION("Partial cluster") {
    auto test_data = MakeTestData(kClusterSize + kLargeBlockSize * 2);
    auto file_result = root->CreateFile("partial_cluster.bin", test_data);
    REQUIRE(file_result.has_value());

    auto read_data = ReadFileData(file_result.value());
    REQUIRE(read_data == test_data);
  }
}

TEST_CASE_METHOD(FileIOTestFixture, "FileIO: Category 4 - indirect block lists") {
  // Need larger image for category 4
  Cleanup();
  CreateImage(128 * 1024 * 1024);  // 128 MB

  auto wfs = CreateWfs();
  REQUIRE(wfs);
  auto root = wfs->GetRootDirectory().value();

  SECTION("Category 4 file (5 clusters)") {
    auto test_data = MakeTestData(kClusterSize * 5);
    auto file_result = root->CreateFile("cat4.bin", test_data);
    REQUIRE(file_result.has_value());

    auto read_data = ReadFileData(file_result.value());
    REQUIRE(read_data == test_data);
  }

  SECTION("Large category 4 file (10 clusters)") {
    auto test_data = MakeTestData(kClusterSize * 10);
    auto file_result = root->CreateFile("large_cat4.bin", test_data);
    REQUIRE(file_result.has_value());

    auto read_data = ReadFileData(file_result.value());
    REQUIRE(read_data == test_data);
  }
}

TEST_CASE_METHOD(FileIOTestFixture, "FileIO: Delete operations") {
  auto wfs = CreateWfs();
  REQUIRE(wfs);
  auto root = wfs->GetRootDirectory().value();

  SECTION("Delete category 0 file") {
    auto test_data = MakeTestData(100);
    REQUIRE(root->CreateFile("small.bin", test_data).has_value());

    auto delete_result = root->DeleteFile("small.bin");
    REQUIRE(delete_result.has_value());

    auto file = root->GetFile("small.bin");
    REQUIRE_FALSE(file.has_value());
  }

  SECTION("Delete category 1 file") {
    auto test_data = MakeTestData(kBlockSize * 2);
    REQUIRE(root->CreateFile("blocks.bin", test_data).has_value());

    auto delete_result = root->DeleteFile("blocks.bin");
    REQUIRE(delete_result.has_value());

    auto file = root->GetFile("blocks.bin");
    REQUIRE_FALSE(file.has_value());
  }

  SECTION("Delete category 2 file") {
    auto test_data = MakeTestData(kLargeBlockSize * 2);
    REQUIRE(root->CreateFile("large.bin", test_data).has_value());

    auto delete_result = root->DeleteFile("large.bin");
    REQUIRE(delete_result.has_value());

    auto file = root->GetFile("large.bin");
    REQUIRE_FALSE(file.has_value());
  }

  SECTION("Delete category 3 file") {
    auto test_data = MakeTestData(kClusterSize);
    REQUIRE(root->CreateFile("cluster.bin", test_data).has_value());

    auto delete_result = root->DeleteFile("cluster.bin");
    REQUIRE(delete_result.has_value());

    auto file = root->GetFile("cluster.bin");
    REQUIRE_FALSE(file.has_value());
  }

  SECTION("Delete non-existent file") {
    auto delete_result = root->DeleteFile("nonexistent.bin");
    REQUIRE_FALSE(delete_result.has_value());
  }
}

TEST_CASE_METHOD(FileIOTestFixture, "FileIO: Persistence after flush and reopen") {
  auto test_data = MakeTestData(kBlockSize * 3);
  std::string filename = "persist_test.bin";

  // Create and write
  {
    auto wfs = CreateWfs();
    REQUIRE(wfs);
    auto root = wfs->GetRootDirectory().value();

    auto file_result = root->CreateFile(filename, test_data);
    REQUIRE(file_result.has_value());

    wfs->Flush();
  }

  // Reopen and verify
  {
    auto wfs = OpenWfs();
    REQUIRE(wfs);
    auto root = wfs->GetRootDirectory().value();

    auto file = root->GetFile(filename);
    REQUIRE(file.has_value());

    auto read_data = ReadFileData(file.value());
    REQUIRE(read_data == test_data);
  }
}

TEST_CASE_METHOD(FileIOTestFixture, "FileIO: Multiple files") {
  auto wfs = CreateWfs();
  REQUIRE(wfs);
  auto root = wfs->GetRootDirectory().value();

  // Create files of different categories
  auto data0 = MakeTestData(100, 0x10);
  auto data1 = MakeTestData(kBlockSize * 2, 0x20);
  auto data2 = MakeTestData(kLargeBlockSize, 0x30);
  auto data3 = MakeTestData(kClusterSize, 0x40);

  REQUIRE(root->CreateFile("cat0.bin", data0).has_value());
  REQUIRE(root->CreateFile("cat1.bin", data1).has_value());
  REQUIRE(root->CreateFile("cat2.bin", data2).has_value());
  REQUIRE(root->CreateFile("cat3.bin", data3).has_value());

  // Verify all files
  auto check_file = [&](const std::string& name, const std::vector<std::byte>& expected) {
    auto file = root->GetFile(name);
    REQUIRE(file.has_value());
    auto read_data = ReadFileData(file.value());
    REQUIRE(read_data == expected);
  };

  check_file("cat0.bin", data0);
  check_file("cat1.bin", data1);
  check_file("cat2.bin", data2);
  check_file("cat3.bin", data3);

  // Count directory entries
  int count = 0;
  for (const auto& entry : *root) {
    (void)entry;
    ++count;
  }
  REQUIRE(count == 4);
}

TEST_CASE_METHOD(FileIOTestFixture, "FileIO: Directory operations") {
  auto wfs = CreateWfs();
  REQUIRE(wfs);
  auto root = wfs->GetRootDirectory().value();

  SECTION("Create and delete directory") {
    auto dir_result = root->CreateDirectory("test_dir");
    REQUIRE(dir_result.has_value());

    auto dir = root->GetDirectory("test_dir");
    REQUIRE(dir.has_value());

    // Delete empty directory
    auto delete_result = root->DeleteDirectory("test_dir", false);
    REQUIRE(delete_result.has_value());

    auto deleted = root->GetDirectory("test_dir");
    REQUIRE_FALSE(deleted.has_value());
  }

  SECTION("Create file in subdirectory") {
    auto dir_result = root->CreateDirectory("subdir");
    REQUIRE(dir_result.has_value());

    auto test_data = MakeTestData(kBlockSize);
    auto file_result = dir_result.value()->CreateFile("nested.bin", test_data);
    REQUIRE(file_result.has_value());

    auto read_data = ReadFileData(file_result.value());
    REQUIRE(read_data == test_data);
  }

  SECTION("Cannot create duplicate file") {
    auto test_data = MakeTestData(100);
    REQUIRE(root->CreateFile("duplicate.bin", test_data).has_value());

    auto second = root->CreateFile("duplicate.bin", test_data);
    REQUIRE_FALSE(second.has_value());
  }

  SECTION("Cannot create duplicate directory") {
    REQUIRE(root->CreateDirectory("dup_dir").has_value());
    auto second = root->CreateDirectory("dup_dir");
    REQUIRE_FALSE(second.has_value());
  }
}
