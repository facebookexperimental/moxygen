#pragma once

#include <folly/io/IOBuf.h>

namespace moxygen::test {

std::unique_ptr<folly::IOBuf> writeAllMessages();

}
