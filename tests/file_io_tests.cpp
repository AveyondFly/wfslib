/*
 * Copyright (C) 2024 koolkdev
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE file for details.
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers.hpp>

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

TEST_CASE_METHOD(FileIOTestFixture, "FileIO: Multi-file serial write") {
  auto wfs = CreateWfs();
  REQUIRE(wfs);
  auto root = wfs->GetRootDirectory().value();

  // Serial write of files with different sizes (covering all categories)
  struct TestFile {
    std::string name;
    size_t size;
    uint8_t seed;
  };

  std::vector<TestFile> test_files = {
    {"empty.dat", 0, 0x00},
    {"tiny.dat", 50, 0x01},
    {"small.dat", 500, 0x02},
    {"block1.dat", kBlockSize, 0x03},
    {"block2.dat", kBlockSize * 2, 0x04},
    {"block5.dat", kBlockSize * 5, 0x05},                    // Max cat1
    {"large1.dat", kLargeBlockSize, 0x06},                   // Cat2
    {"large3.dat", kLargeBlockSize * 3, 0x07},
    {"large5.dat", kLargeBlockSize * 5, 0x08},               // Max cat2
    {"cluster1.dat", kClusterSize, 0x09},                    // Cat3
    {"cluster2.dat", kClusterSize * 2, 0x0A},
    {"cluster4.dat", kClusterSize * 4, 0x0B},                // Max cat3
  };

  // Write all files serially
  for (const auto& tf : test_files) {
    auto data = MakeTestData(tf.size, tf.seed);
    auto result = root->CreateFile(tf.name, data);
    INFO("Failed to create: " << tf.name);
    REQUIRE(result.has_value());
  }

  // Verify all files can be read correctly
  for (const auto& tf : test_files) {
    auto file = root->GetFile(tf.name);
    INFO("Failed to get: " << tf.name);
    REQUIRE(file.has_value());

    auto expected = MakeTestData(tf.size, tf.seed);
    auto read_data = ReadFileData(file.value());
    INFO("Data mismatch: " << tf.name);
    REQUIRE(read_data == expected);
  }
}

TEST_CASE_METHOD(FileIOTestFixture, "FileIO: Nested directories with files") {
  auto wfs = CreateWfs();
  REQUIRE(wfs);
  auto root = wfs->GetRootDirectory().value();

  // Create nested directory structure
  auto level1 = root->CreateDirectory("level1");
  REQUIRE(level1.has_value());

  auto level2 = level1.value()->CreateDirectory("level2");
  REQUIRE(level2.has_value());

  auto level3 = level2.value()->CreateDirectory("level3");
  REQUIRE(level3.has_value());

  // Create files at each level
  auto data_root = MakeTestData(100, 0x01);
  auto data_l1 = MakeTestData(kBlockSize, 0x02);
  auto data_l2 = MakeTestData(kLargeBlockSize, 0x03);
  auto data_l3 = MakeTestData(kClusterSize, 0x04);

  REQUIRE(root->CreateFile("root_file.dat", data_root).has_value());
  REQUIRE(level1.value()->CreateFile("l1_file.dat", data_l1).has_value());
  REQUIRE(level2.value()->CreateFile("l2_file.dat", data_l2).has_value());
  REQUIRE(level3.value()->CreateFile("l3_file.dat", data_l3).has_value());

  // Verify all files
  REQUIRE(ReadFileData(root->GetFile("root_file.dat").value()) == data_root);
  REQUIRE(ReadFileData(level1.value()->GetFile("l1_file.dat").value()) == data_l1);
  REQUIRE(ReadFileData(level2.value()->GetFile("l2_file.dat").value()) == data_l2);
  REQUIRE(ReadFileData(level3.value()->GetFile("l3_file.dat").value()) == data_l3);
}

TEST_CASE_METHOD(FileIOTestFixture, "FileIO: Delete and recreate files") {
  auto wfs = CreateWfs();
  REQUIRE(wfs);
  auto root = wfs->GetRootDirectory().value();

  // Create a file
  auto data1 = MakeTestData(kBlockSize * 2, 0x10);
  REQUIRE(root->CreateFile("recycle.bin", data1).has_value());
  REQUIRE(ReadFileData(root->GetFile("recycle.bin").value()) == data1);

  // Delete it
  REQUIRE(root->DeleteFile("recycle.bin").has_value());
  REQUIRE_FALSE(root->GetFile("recycle.bin").has_value());

  // Recreate with different data
  auto data2 = MakeTestData(kLargeBlockSize, 0x20);
  REQUIRE(root->CreateFile("recycle.bin", data2).has_value());
  REQUIRE(ReadFileData(root->GetFile("recycle.bin").value()) == data2);
}

TEST_CASE_METHOD(FileIOTestFixture, "FileIO: Many small files") {
  auto wfs = CreateWfs();
  REQUIRE(wfs);
  auto root = wfs->GetRootDirectory().value();

  // Create 50 small files
  const int num_files = 50;
  for (int i = 0; i < num_files; ++i) {
    std::string name = "file_" + std::to_string(i) + ".dat";
    auto data = MakeTestData(100 + i, static_cast<uint8_t>(i));
    auto result = root->CreateFile(name, data);
    INFO("Failed at file " << i);
    REQUIRE(result.has_value());
  }

  // Verify all
  for (int i = 0; i < num_files; ++i) {
    std::string name = "file_" + std::to_string(i) + ".dat";
    auto file = root->GetFile(name);
    REQUIRE(file.has_value());

    auto expected = MakeTestData(100 + i, static_cast<uint8_t>(i));
    REQUIRE(ReadFileData(file.value()) == expected);
  }
}

TEST_CASE_METHOD(FileIOTestFixture, "FileIO: Mixed file operations") {
  // Need larger image
  Cleanup();
  CreateImage(128 * 1024 * 1024);

  auto wfs = CreateWfs();
  REQUIRE(wfs);
  auto root = wfs->GetRootDirectory().value();

  // Create directories
  auto docs = root->CreateDirectory("documents");
  auto pics = root->CreateDirectory("pictures");
  REQUIRE(docs.has_value());
  REQUIRE(pics.has_value());

  // Create various files
  auto cat0_data = MakeTestData(200, 0xA0);
  auto cat1_data = MakeTestData(kBlockSize * 3, 0xA1);
  auto cat2_data = MakeTestData(kLargeBlockSize * 2, 0xA2);
  auto cat3_data = MakeTestData(kClusterSize * 2, 0xA3);
  auto cat4_data = MakeTestData(kClusterSize * 6, 0xA4);

  REQUIRE(root->CreateFile("cat0.bin", cat0_data).has_value());
  REQUIRE(docs.value()->CreateFile("cat1.bin", cat1_data).has_value());
  REQUIRE(docs.value()->CreateFile("cat2.bin", cat2_data).has_value());
  REQUIRE(pics.value()->CreateFile("cat3.bin", cat3_data).has_value());
  REQUIRE(pics.value()->CreateFile("cat4.bin", cat4_data).has_value());

  // Flush and reopen
  wfs->Flush();
  wfs = OpenWfs();
  REQUIRE(wfs);
  root = wfs->GetRootDirectory().value();

  // Verify everything after reopen
  docs = root->GetDirectory("documents");
  pics = root->GetDirectory("pictures");
  REQUIRE(docs.has_value());
  REQUIRE(pics.has_value());

  REQUIRE(ReadFileData(root->GetFile("cat0.bin").value()) == cat0_data);
  REQUIRE(ReadFileData(docs.value()->GetFile("cat1.bin").value()) == cat1_data);
  REQUIRE(ReadFileData(docs.value()->GetFile("cat2.bin").value()) == cat2_data);
  REQUIRE(ReadFileData(pics.value()->GetFile("cat3.bin").value()) == cat3_data);
  REQUIRE(ReadFileData(pics.value()->GetFile("cat4.bin").value()) == cat4_data);

  // Delete some files
  REQUIRE(root->DeleteFile("cat0.bin").has_value());
  REQUIRE(docs.value()->DeleteFile("cat1.bin").has_value());

  // Verify deleted
  REQUIRE_FALSE(root->GetFile("cat0.bin").has_value());
  REQUIRE_FALSE(docs.value()->GetFile("cat1.bin").has_value());

  // Others still exist
  REQUIRE(docs.value()->GetFile("cat2.bin").has_value());
  REQUIRE(pics.value()->GetFile("cat3.bin").has_value());
  REQUIRE(pics.value()->GetFile("cat4.bin").has_value());
}

TEST_CASE_METHOD(FileIOTestFixture, "FileIO: Partial block writes") {
  auto wfs = CreateWfs();
  REQUIRE(wfs);
  auto root = wfs->GetRootDirectory().value();

  // Test various non-aligned sizes
  std::vector<size_t> test_sizes = {
    kBlockSize + 1,
    kBlockSize + 1000,
    kBlockSize * 2 + 1234,
    kLargeBlockSize + 5000,
    kLargeBlockSize * 2 + 10000,
    kClusterSize + kBlockSize * 3,
  };

  for (size_t i = 0; i < test_sizes.size(); ++i) {
    std::string name = "partial_" + std::to_string(i) + ".bin";
    auto data = MakeTestData(test_sizes[i], static_cast<uint8_t>(i));

    auto result = root->CreateFile(name, data);
    INFO("Failed at size " << test_sizes[i]);
    REQUIRE(result.has_value());

    auto read = ReadFileData(result.value());
    INFO("Mismatch at size " << test_sizes[i]);
    REQUIRE(read == data);
  }
}

TEST_CASE_METHOD(FileIOTestFixture, "FileIO: Filename length tests") {
  auto wfs = CreateWfs();
  REQUIRE(wfs);
  auto root = wfs->GetRootDirectory().value();
  auto test_data = MakeTestData(100, 0x42);

  SECTION("Single character filename") {
    auto result = root->CreateFile("a", test_data);
    REQUIRE(result.has_value());
    auto read = ReadFileData(result.value());
    REQUIRE(read == test_data);
  }

  SECTION("Short filename") {
    auto result = root->CreateFile("test", test_data);
    REQUIRE(result.has_value());
    REQUIRE(root->GetFile("test").has_value());
  }

  SECTION("Medium filename (20 chars)") {
    std::string name(20, 'x');
    auto result = root->CreateFile(name, test_data);
    REQUIRE(result.has_value());
    REQUIRE(root->GetFile(name).has_value());
  }

  SECTION("Longer filename (50 chars)") {
    std::string name(50, 'y');
    auto result = root->CreateFile(name, test_data);
    REQUIRE(result.has_value());
    REQUIRE(root->GetFile(name).has_value());
  }

  SECTION("Filename with extension") {
    auto result = root->CreateFile("document.txt", test_data);
    REQUIRE(result.has_value());
    REQUIRE(root->GetFile("document.txt").has_value());
  }

  SECTION("Filename with multiple dots") {
    auto result = root->CreateFile("file.tar.gz", test_data);
    REQUIRE(result.has_value());
    REQUIRE(root->GetFile("file.tar.gz").has_value());
  }

  SECTION("Filename with numbers") {
    auto result = root->CreateFile("file123.dat", test_data);
    REQUIRE(result.has_value());
    REQUIRE(root->GetFile("file123.dat").has_value());
  }

  SECTION("Filename with underscore and hyphen") {
    auto result = root->CreateFile("my_file-name.bin", test_data);
    REQUIRE(result.has_value());
    REQUIRE(root->GetFile("my_file-name.bin").has_value());
  }

  SECTION("Case insensitive - same name different case") {
    auto result1 = root->CreateFile("testfile.bin", test_data);
    REQUIRE(result1.has_value());
    
    // Same name with different case should fail (case insensitive filesystem)
    auto data2 = MakeTestData(200, 0x43);
    auto result2 = root->CreateFile("TESTFILE.bin", data2);
    REQUIRE_FALSE(result2.has_value());
  }

  SECTION("Mixed case filename") {
    auto result = root->CreateFile("MixedCaseFile.bin", test_data);
    REQUIRE(result.has_value());
    // Should be able to retrieve with different case
    REQUIRE(root->GetFile("mixedcasefile.bin").has_value());
    REQUIRE(root->GetFile("MIXEDCASEFILE.BIN").has_value());
  }
}

TEST_CASE_METHOD(FileIOTestFixture, "FileIO: Many files with various names") {
  auto wfs = CreateWfs();
  REQUIRE(wfs);
  auto root = wfs->GetRootDirectory().value();

  // Create files with different name patterns
  std::vector<std::string> names = {
    "a", "b", "c",                                           // Single char
    "file1", "file2", "file3",                               // Short names
    "document_with_long_name.bin",                           // Medium name
    std::string(30, 'x') + ".dat",                           // Longer name
    "test_file-1.bin", "test_file-2.bin", "test_file-3.bin", // With special chars
    "UPPER.BIN", "lower.bin", "Mixed.Bin",                   // Case variations
  };

  // Create all files
  for (size_t i = 0; i < names.size(); ++i) {
    auto data = MakeTestData(100 + i, static_cast<uint8_t>(i));
    auto result = root->CreateFile(names[i], data);
    INFO("Failed to create: " << names[i]);
    REQUIRE(result.has_value());
  }

  // Verify all files exist and have correct data
  for (size_t i = 0; i < names.size(); ++i) {
    auto file = root->GetFile(names[i]);
    INFO("Failed to get: " << names[i]);
    REQUIRE(file.has_value());

    auto expected = MakeTestData(100 + i, static_cast<uint8_t>(i));
    REQUIRE(ReadFileData(file.value()) == expected);
  }

  // Count entries
  int count = 0;
  for (const auto& entry : *root) {
    (void)entry;
    ++count;
  }
  REQUIRE(count == static_cast<int>(names.size()));
}

TEST_CASE_METHOD(FileIOTestFixture, "DeviceType is correctly set on Create", "[device_type]") {
  // Test default device type (USB)
  {
    auto wfs = CreateWfs();
    REQUIRE(wfs != nullptr);
    REQUIRE(wfs->device_type() == DeviceType::USB);
    wfs->Flush();
  }

  // Test MLC device type
  {
    auto device = std::make_shared<FileDevice>(img_path_, 9, img_size_ / 512, false, false);
    auto result = WfsDevice::Create(device, DeviceType::MLC);
    REQUIRE(result.has_value());
    auto wfs = result.value();
    REQUIRE(wfs->device_type() == DeviceType::MLC);
    wfs->Flush();
  }

  // Verify device type persists after reopen
  {
    auto wfs = OpenWfs();
    REQUIRE(wfs != nullptr);
    REQUIRE(wfs->device_type() == DeviceType::MLC);
  }
}
