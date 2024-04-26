#include "moxygen/MoQSession.h"

int main(int, char*[]) {
  moxygen::MoQSession sess(
      moxygen::MoQCodec::Direction::CLIENT, nullptr, nullptr);
  return 0;
}
