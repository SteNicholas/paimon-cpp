/*
 * Copyright 2024-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/core/utils/tag_manager.h"

#include "gtest/gtest.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(TagManagerTest, TestGet) {
    ASSERT_OK_AND_ASSIGN(auto tag,
                         TagManager(std::make_shared<LocalFileSystem>(),
                                    paimon::test::GetDataDir() + "/orc/append_09.db/append_09")
                             .Get("tag"));
    ASSERT_EQ(tag, std::nullopt);
}

TEST(TagManagerTest, TestTagPath) {
    ASSERT_EQ(TagManager(nullptr, "/root").TagPath("data"), "/root/tag/tag-data");
    ASSERT_EQ(TagManager(nullptr, "/root", "data").TagPath("data"),
              "/root/branch/branch-data/tag/tag-data");
}
}  // namespace paimon::test
