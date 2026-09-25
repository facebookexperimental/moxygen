/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/samples/media_server/sources/FileMediaSourceResolver.h>

#include <folly/FileUtil.h>
#include <folly/coro/BlockingWait.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <folly/testing/TestUtil.h>

#include <memory>
#include <string>
#include <vector>

using namespace std::chrono_literals;
using testing::HasSubstr;

namespace moxygen::media_server::test {
namespace {

// The catalog names a track whose fMP4 is absent; serving the catalog only
// needs the metadata.
constexpr std::string_view kCatalogJson =
    R"({"version":"draft-01","tracks":[{"name":"aliasTrack","role":"video","sourceFile":"missing.mp4"}]})";

class FileMediaSourceResolverTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_TRUE(folly::writeFile(kCatalogJson, catalogPath_.c_str()));
  }

  // The catalog document served for `ns`, or nullopt if `ns` is not served.
  std::optional<std::string> catalogFor(
      const std::vector<std::string>& aliases,
      const TrackNamespace& ns) {
    FileMediaSourceResolver resolver(
        catalogPath_, 1000ms, 10s, /*loop=*/false, aliases);
    auto source = folly::coro::blockingWait(
        resolver.openTrack(ns, std::string(kCatalogTrackName)));
    if (!source) {
      return std::nullopt;
    }
    auto objects = source->fetch({0, 0}, {0, 1});
    auto object = folly::coro::blockingWait(objects.next());
    EXPECT_TRUE(object.has_value());
    return object ? object->payload->moveToFbString().toStdString() : "";
  }

  folly::test::TemporaryDirectory dir_;
  const std::string catalogPath_{(dir_.path() / "catalog.json").string()};
};

} // namespace

TEST_F(FileMediaSourceResolverTest, AliasedNamespaceServesTheFileCatalog) {
  auto catalog = catalogFor({"moq-media"}, TrackNamespace({"moq-media"}));
  ASSERT_TRUE(catalog.has_value());
  EXPECT_THAT(*catalog, HasSubstr("\"aliasTrack\""));
}

TEST_F(FileMediaSourceResolverTest, OnlyFileAndAliasedNamespacesAreServed) {
  EXPECT_FALSE(catalogFor({}, TrackNamespace({"moq-media"})).has_value());
  EXPECT_FALSE(
      catalogFor({"moq-media"}, TrackNamespace({"other"})).has_value());
  EXPECT_TRUE(
      catalogFor({}, TrackNamespace({"file", "moq-media"})).has_value());
}

} // namespace moxygen::media_server::test
