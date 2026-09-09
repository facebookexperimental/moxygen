/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/samples/media_server/FilePrControlServer.h>

#include <moxygen/samples/media_server/sources/FilePrFaultState.h>

#include <folly/dynamic.h>
#include <folly/json.h>
#include <folly/logging/xlog.h>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace moxygen::media_server {
namespace {

constexpr size_t kMaxRequestBytes = 64 * 1024;

constexpr std::string_view kControlPage = R"HTML(<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>file_pr fault control</title>
<style>
:root { color-scheme: light; font-family: ui-sans-serif, system-ui, sans-serif; }
* { box-sizing: border-box; }
body { margin: 0; color: #171717; background: #f5f6f7; font-size: 14px; }
header { background: #171717; color: #fff; padding: 18px 24px; }
h1 { margin: 0; font-size: 20px; font-weight: 650; letter-spacing: 0; }
h2 { margin: 0 0 8px; font-size: 14px; font-weight: 700; letter-spacing: 0; }
main { max-width: 1120px; margin: 0 auto; padding: 20px 24px 36px; }
.controls { display: grid; grid-template-columns: minmax(180px, 1fr) 130px auto auto auto auto; gap: 10px; align-items: end; padding: 16px 0 20px; }
label { display: grid; gap: 6px; color: #4b5563; font-size: 12px; font-weight: 600; }
input, button { min-height: 38px; border: 1px solid #b9bec5; border-radius: 5px; font: inherit; }
input { width: 100%; padding: 8px 10px; background: #fff; color: #171717; }
.check { display: flex; align-items: center; gap: 8px; min-height: 38px; color: #30343a; white-space: nowrap; }
.check input { width: 16px; min-height: 16px; margin: 0; }
button { padding: 8px 16px; color: #fff; font-weight: 650; cursor: pointer; }
button:disabled { cursor: not-allowed; opacity: .45; }
.drop { background: #b42318; border-color: #b42318; }
.hold { background: #9a6700; border-color: #9a6700; }
.clear { background: #4b5563; border-color: #4b5563; }
.status { min-height: 24px; padding: 2px 0 12px; color: #4b5563; }
.table-wrap { overflow-x: auto; border: 1px solid #d5d8dc; background: #fff; }
.operations { margin-top: 24px; }
.operations table { min-width: 980px; }
table { width: 100%; border-collapse: collapse; table-layout: fixed; }
th, td { padding: 11px 12px; border-bottom: 1px solid #e5e7eb; text-align: left; overflow-wrap: anywhere; }
th { background: #eceff1; color: #444b53; font-size: 12px; }
tr:last-child td { border-bottom: 0; }
.empty { color: #737980; }
@media (max-width: 760px) {
  main { padding: 14px 16px 28px; }
  .controls { grid-template-columns: 1fr 1fr; }
  .controls label:first-child { grid-column: 1 / -1; }
  button { width: 100%; }
  table { min-width: 850px; }
}
</style>
</head>
<body>
<header><h1>file_pr fault control</h1></header>
<main>
  <section class="controls">
    <label>Track<input id="track" list="track-list" value="video0" autocomplete="off"><datalist id="track-list"></datalist></label>
    <label>Seconds<input id="seconds" type="number" min="0.1" max="60" step="0.5" value="3"></label>
    <label class="check"><input id="affect-i" type="checkbox">Affect I-frames</label>
    <button class="drop" id="drop">Drop</button>
    <button class="hold" id="hold">Hold</button>
    <button class="clear" id="clear">Clear</button>
  </section>
  <div class="status" id="status"></div>
  <h2>Live tracks</h2>
  <div class="table-wrap">
    <table>
      <thead><tr><th>Track</th><th>Role</th><th>Group</th><th>Subgroup</th><th>Objects</th><th>Action</th><th>Remaining</th><th>Last event</th></tr></thead>
      <tbody id="tracks"><tr><td class="empty" colspan="8">No file_pr tracks opened</td></tr></tbody>
    </table>
  </div>
  <section class="operations">
    <h2>Operations</h2>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Command</th><th>Track</th><th>Action</th><th>Status</th><th>Duration</th><th>Subgroups</th><th>Dropped objects</th><th>Released objects</th><th>Preserved I-frames</th><th>Last group</th><th>Last subgroup</th></tr></thead>
        <tbody id="operations"><tr><td class="empty" colspan="11">No operations</td></tr></tbody>
      </table>
    </div>
  </section>
</main>
<script>
const trackInput = document.getElementById('track');
const secondsInput = document.getElementById('seconds');
const affectI = document.getElementById('affect-i');
const statusLine = document.getElementById('status');
let state = [];

function setStatus(text, failed = false) {
  statusLine.textContent = text;
  statusLine.style.color = failed ? '#b42318' : '#4b5563';
}

function selectedRole() {
  const item = state.find(entry => entry.track === trackInput.value.trim());
  return item ? item.role : '';
}

function updateCheckbox() {
  const disabled = selectedRole() === 'audio';
  affectI.disabled = disabled;
  if (disabled) affectI.checked = false;
}

function addCell(row, value, className = '') {
  const cell = document.createElement('td');
  cell.textContent = value;
  if (className) cell.className = className;
  row.appendChild(cell);
}

function render(items) {
  state = items;
  const list = document.getElementById('track-list');
  list.replaceChildren(...items.map(item => {
    const option = document.createElement('option');
    option.value = item.track;
    return option;
  }));
  const body = document.getElementById('tracks');
  if (!items.length) {
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = 8;
    cell.className = 'empty';
    cell.textContent = 'No file_pr tracks opened';
    row.appendChild(cell);
    body.replaceChildren(row);
    updateCheckbox();
    return;
  }
  body.replaceChildren(...items.map(item => {
    const row = document.createElement('tr');
    addCell(row, item.track);
    addCell(row, item.role || '-');
    addCell(row, item.group === null ? '-' : String(item.group));
    addCell(row, item.subgroup === null ? '-' : String(item.subgroup));
    addCell(row, String(item.objects));
    addCell(row, item.action || '-');
    addCell(row, item.action ? `${(item.remainingMs / 1000).toFixed(1)}s` : '-');
    addCell(row, item.lastEvent || '-');
    row.addEventListener('click', () => { trackInput.value = item.track; updateCheckbox(); });
    return row;
  }));
  updateCheckbox();
}

function renderOperations(items) {
  const body = document.getElementById('operations');
  if (!items.length) {
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = 11;
    cell.className = 'empty';
    cell.textContent = 'No operations';
    row.appendChild(cell);
    body.replaceChildren(row);
    return;
  }
  body.replaceChildren(...items.map(item => {
    const row = document.createElement('tr');
    const action = item.action === 'drop'
      ? (item.affectIFrames ? 'drop including I' : 'drop preserving I')
      : item.action === 'hold'
        ? (item.affectIFrames ? 'hold including I' : 'hold preserving I')
        : item.action;
    addCell(row, String(item.commandId));
    addCell(row, item.track);
    addCell(row, action);
    addCell(row, item.status);
    addCell(row, item.durationMs ? `${(item.durationMs / 1000).toFixed(1)}s` : '-');
    addCell(row, String(item.subgroupsAffected));
    addCell(row, String(item.objectsDropped));
    addCell(row, String(item.objectsReleased));
    addCell(row, String(item.preservedIFrames));
    addCell(row, item.lastGroup === null ? '-' : String(item.lastGroup));
    addCell(row, item.lastSubgroup === null ? '-' : String(item.lastSubgroup));
    return row;
  }));
}

async function refresh() {
  try {
    const response = await fetch('/api/state', {cache: 'no-store'});
    const payload = await response.json();
    render(payload.tracks || []);
    renderOperations(payload.operations || []);
  } catch (error) {
    setStatus(String(error), true);
  }
}

async function post(path, body) {
  const response = await fetch(path, {
    method: 'POST',
    headers: {'content-type': 'application/json'},
    body: JSON.stringify(body),
  });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
  return payload;
}

async function arm(action) {
  try {
    const track = trackInput.value.trim();
    const seconds = Number(secondsInput.value);
    const payload = await post('/api/fault', {
      track,
      action,
      seconds,
      affectIFrames: affectI.checked,
    });
    setStatus(`${action} armed on ${track} (command ${payload.commandId})`);
    await refresh();
  } catch (error) {
    setStatus(String(error), true);
  }
}

document.getElementById('drop').addEventListener('click', () => arm('drop'));
document.getElementById('hold').addEventListener('click', () => arm('hold'));
document.getElementById('clear').addEventListener('click', async () => {
  try {
    const track = trackInput.value.trim();
    await post('/api/clear', {track});
    setStatus(`fault cleared on ${track}`);
    await refresh();
  } catch (error) {
    setStatus(String(error), true);
  }
});
trackInput.addEventListener('input', updateCheckbox);
refresh();
setInterval(refresh, 500);
</script>
</body>
</html>)HTML";

struct HttpRequest {
  std::string method;
  std::string path;
  std::string body;
};

std::string lowercase(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](char c) {
    return static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  });
  return value;
}

std::optional<HttpRequest> readRequest(int fd) {
  std::string data;
  data.reserve(4096);
  std::array<char, 4096> buffer{};
  size_t contentLength = 0;
  size_t bodyStart = std::string::npos;

  while (data.size() < kMaxRequestBytes) {
    const ssize_t bytesRead = recv(fd, buffer.data(), buffer.size(), 0);
    if (bytesRead <= 0) {
      return std::nullopt;
    }
    data.append(buffer.data(), static_cast<size_t>(bytesRead));
    if (bodyStart == std::string::npos) {
      const size_t headerEnd = data.find("\r\n\r\n");
      if (headerEnd == std::string::npos) {
        continue;
      }
      bodyStart = headerEnd + 4;
      size_t lineStart = data.find("\r\n") + 2;
      while (lineStart < headerEnd) {
        const size_t lineEnd = data.find("\r\n", lineStart);
        const size_t colon = data.find(':', lineStart);
        if (colon != std::string::npos && colon < lineEnd) {
          const std::string name =
              lowercase(data.substr(lineStart, colon - lineStart));
          if (name == "content-length") {
            size_t valueStart = colon + 1;
            while (valueStart < lineEnd && data[valueStart] == ' ') {
              ++valueStart;
            }
            const auto* begin = data.data() + valueStart;
            const auto* end = data.data() + lineEnd;
            if (std::from_chars(begin, end, contentLength).ec != std::errc()) {
              return std::nullopt;
            }
          }
        }
        lineStart = lineEnd + 2;
      }
      if (contentLength > kMaxRequestBytes - bodyStart) {
        return std::nullopt;
      }
    }
    if (bodyStart != std::string::npos &&
        data.size() >= bodyStart + contentLength) {
      break;
    }
  }

  const size_t requestLineEnd = data.find("\r\n");
  if (requestLineEnd == std::string::npos || bodyStart == std::string::npos) {
    return std::nullopt;
  }
  const std::string requestLine = data.substr(0, requestLineEnd);
  const size_t firstSpace = requestLine.find(' ');
  const size_t secondSpace = requestLine.find(' ', firstSpace + 1);
  if (firstSpace == std::string::npos || secondSpace == std::string::npos) {
    return std::nullopt;
  }
  std::string path =
      requestLine.substr(firstSpace + 1, secondSpace - firstSpace - 1);
  if (const size_t query = path.find('?'); query != std::string::npos) {
    path.resize(query);
  }
  return HttpRequest{
      requestLine.substr(0, firstSpace),
      std::move(path),
      data.substr(bodyStart, contentLength)};
}

void sendAll(int fd, std::string_view data) {
  size_t written = 0;
  while (written < data.size()) {
    const ssize_t result =
        send(fd, data.data() + written, data.size() - written, MSG_NOSIGNAL);
    if (result <= 0) {
      return;
    }
    written += static_cast<size_t>(result);
  }
}

void respond(
    int fd,
    int status,
    std::string_view reason,
    std::string_view contentType,
    std::string body) {
  std::string response = "HTTP/1.1 " + std::to_string(status) + " " +
      std::string(reason) + "\r\nContent-Type: " + std::string(contentType) +
      "\r\nContent-Length: " + std::to_string(body.size()) +
      "\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n";
  response += body;
  sendAll(fd, response);
}

void respondJson(int fd, int status, folly::dynamic body) {
  respond(
      fd,
      status,
      status == 200 ? "OK" : "Bad Request",
      "application/json; charset=utf-8",
      folly::toJson(body));
}

folly::dynamic errorJson(const std::string& error) {
  folly::dynamic body = folly::dynamic::object;
  body["error"] = error;
  return body;
}

std::optional<std::string> requiredString(
    const folly::dynamic& body,
    const char* key) {
  const auto* value = body.get_ptr(key);
  if (!value || !value->isString() || value->asString().empty()) {
    return std::nullopt;
  }
  return value->asString();
}

std::optional<std::chrono::milliseconds> requiredDuration(
    const folly::dynamic& body) {
  const auto* value = body.get_ptr("seconds");
  if (!value || (!value->isInt() && !value->isDouble())) {
    return std::nullopt;
  }
  const double seconds =
      value->isInt() ? static_cast<double>(value->asInt()) : value->asDouble();
  if (seconds < 0.1 || seconds > 60.0) {
    return std::nullopt;
  }
  return std::chrono::milliseconds(static_cast<int64_t>(seconds * 1000));
}

folly::dynamic stateJson() {
  auto& state = filePrFaultState();
  folly::dynamic tracks = folly::dynamic::array;
  for (const auto& status : state.snapshot()) {
    folly::dynamic item = folly::dynamic::object;
    item["track"] = status.track;
    item["role"] = status.role;
    item["group"] = status.currentGroup
        ? folly::dynamic(static_cast<int64_t>(*status.currentGroup))
        : folly::dynamic(nullptr);
    item["subgroup"] = status.currentSubgroup
        ? folly::dynamic(static_cast<int64_t>(*status.currentSubgroup))
        : folly::dynamic(nullptr);
    item["objects"] = static_cast<int64_t>(status.groupObjects);
    item["action"] = status.action;
    item["durationMs"] = status.durationMs;
    item["remainingMs"] = status.remainingMs;
    item["affectIFrames"] = status.affectIFrames;
    item["lastEvent"] = status.lastEvent;
    tracks.push_back(std::move(item));
  }
  folly::dynamic operations = folly::dynamic::array;
  for (const auto& operation : state.operationSnapshot()) {
    folly::dynamic item = folly::dynamic::object;
    item["commandId"] = static_cast<int64_t>(operation.commandId);
    item["track"] = operation.track;
    item["action"] = operation.action;
    item["status"] = operation.status;
    item["durationMs"] = operation.durationMs;
    item["affectIFrames"] = operation.affectIFrames;
    item["subgroupsAffected"] =
        static_cast<int64_t>(operation.subgroupsAffected);
    item["objectsDropped"] = static_cast<int64_t>(operation.objectsDropped);
    item["objectsReleased"] = static_cast<int64_t>(operation.objectsReleased);
    item["preservedIFrames"] = static_cast<int64_t>(operation.preservedIFrames);
    item["lastGroup"] = operation.lastGroup
        ? folly::dynamic(static_cast<int64_t>(*operation.lastGroup))
        : folly::dynamic(nullptr);
    item["lastSubgroup"] = operation.lastSubgroup
        ? folly::dynamic(static_cast<int64_t>(*operation.lastSubgroup))
        : folly::dynamic(nullptr);
    operations.push_back(std::move(item));
  }
  folly::dynamic root = folly::dynamic::object;
  root["namespace"] = "file_pr";
  root["tracks"] = std::move(tracks);
  root["operations"] = std::move(operations);
  return root;
}

} // namespace

FilePrControlServer::FilePrControlServer(uint16_t port) : port_(port) {}

FilePrControlServer::~FilePrControlServer() {
  stop();
}

bool FilePrControlServer::start() {
  if (running_.exchange(true)) {
    return true;
  }
  int socketType = SOCK_STREAM;
#ifdef SOCK_CLOEXEC
  socketType |= SOCK_CLOEXEC;
#endif
  const int fd = socket(AF_INET6, socketType, 0);
  if (fd < 0) {
    running_ = false;
    return false;
  }
#ifndef SOCK_CLOEXEC
  if (fcntl(fd, F_SETFD, FD_CLOEXEC) != 0) {
    XLOG(ERR) << "[FilePrControl] cannot set close-on-exec: "
              << folly::errnoStr(errno);
    close(fd);
    running_ = false;
    return false;
  }
#endif
  const int reuse = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
  sockaddr_in6 address{};
  address.sin6_family = AF_INET6;
  address.sin6_port = htons(port_);
  // Loopback only: the control surface is unauthenticated, so it must not be
  // reachable off-host.
  address.sin6_addr = in6addr_loopback;
  if (bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
      listen(fd, 16) != 0) {
    XLOG(ERR) << "[FilePrControl] cannot listen on [::1]:" << port_
              << " error=" << folly::errnoStr(errno);
    close(fd);
    running_ = false;
    return false;
  }
  listenFd_ = fd;
  thread_ = std::thread([this] { run(); });
  XLOG(INFO) << "[FilePrControl] listening on http://[::1]:" << port_;
  return true;
}

void FilePrControlServer::stop() {
  if (!running_.exchange(false)) {
    return;
  }
  const int fd = listenFd_.exchange(-1);
  if (fd >= 0) {
    shutdown(fd, SHUT_RDWR);
    close(fd);
  }
  if (thread_.joinable()) {
    thread_.join();
  }
}

void FilePrControlServer::run() {
  while (running_) {
    const int fd = accept(listenFd_, nullptr, nullptr);
    if (fd < 0) {
      if (!running_) {
        break;
      }
      if (errno == EINTR) {
        continue;
      }
      XLOG(WARN) << "[FilePrControl] accept failed: " << std::strerror(errno);
      continue;
    }
    timeval timeout{2, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    handleClient(fd);
    close(fd);
  }
}

void FilePrControlServer::handleClient(int fd) {
  auto request = readRequest(fd);
  if (!request) {
    respondJson(fd, 400, errorJson("malformed request"));
    return;
  }
  if (request->method == "GET" &&
      (request->path == "/" || request->path == "/index.html")) {
    respond(
        fd, 200, "OK", "text/html; charset=utf-8", std::string(kControlPage));
    return;
  }
  if (request->method == "GET" && request->path == "/api/state") {
    respondJson(fd, 200, stateJson());
    return;
  }
  if (request->method != "POST" ||
      (request->path != "/api/fault" && request->path != "/api/clear")) {
    respond(
        fd,
        404,
        "Not Found",
        "application/json; charset=utf-8",
        folly::toJson(errorJson("not found")));
    return;
  }

  try {
    const auto body = folly::parseJson(request->body);
    if (!body.isObject()) {
      respondJson(fd, 400, errorJson("JSON object required"));
      return;
    }
    auto track = requiredString(body, "track");
    if (!track) {
      respondJson(fd, 400, errorJson("non-empty track required"));
      return;
    }
    if (request->path == "/api/clear") {
      const uint64_t commandId = filePrFaultState().clear(*track);
      folly::dynamic response = folly::dynamic::object;
      response["ok"] = true;
      response["commandId"] = static_cast<int64_t>(commandId);
      respondJson(fd, 200, std::move(response));
      return;
    }

    auto action = requiredString(body, "action");
    auto duration = requiredDuration(body);
    if (!action || (*action != "drop" && *action != "hold")) {
      respondJson(fd, 400, errorJson("action must be drop or hold"));
      return;
    }
    if (!duration) {
      respondJson(fd, 400, errorJson("seconds must be between 0.1 and 60"));
      return;
    }
    bool affectIFrames = false;
    if (const auto* value = body.get_ptr("affectIFrames")) {
      if (!value->isBool()) {
        respondJson(fd, 400, errorJson("affectIFrames must be boolean"));
        return;
      }
      affectIFrames = value->asBool();
    }
    const auto faultAction =
        *action == "drop" ? FilePrFaultAction::Drop : FilePrFaultAction::Hold;
    const uint64_t commandId =
        filePrFaultState().arm(*track, faultAction, *duration, affectIFrames);
    folly::dynamic response = folly::dynamic::object;
    response["ok"] = true;
    response["commandId"] = static_cast<int64_t>(commandId);
    respondJson(fd, 200, std::move(response));
  } catch (const std::exception& ex) {
    respondJson(fd, 400, errorJson(ex.what()));
  }
}

} // namespace moxygen::media_server
