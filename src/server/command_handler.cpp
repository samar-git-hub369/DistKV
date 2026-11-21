#include "server/command_handler.h"

#include "storage/storage_engine.h"

namespace distkv {

Status StandaloneBackend::Put(std::string_view key, std::string_view value) {
    return engine_->Put(key, value);
}

Status StandaloneBackend::Delete(std::string_view key) { return engine_->Delete(key); }

Status StandaloneBackend::Get(std::string_view key, std::string* value) {
    return engine_->Get(key, value);
}

std::string StandaloneBackend::StatusFields() {
    const EngineStats st = engine_->stats();
    return "role=standalone last_seq=" + std::to_string(st.last_seq) +
           " sstables=" + std::to_string(st.sstables) +
           " memtable_bytes=" + std::to_string(st.memtable_bytes) +
           " flushes=" + std::to_string(st.flushes) +
           " compactions=" + std::to_string(st.compactions);
}

std::string CommandHandler::HandleLine(std::string_view line) {
    Request request;
    std::string error;
    if (!ParseRequest(line, &request, &error)) {
        requests_.fetch_add(1, std::memory_order_relaxed);
        errors_.fetch_add(1, std::memory_order_relaxed);
        return ErrorResponse(error);
    }
    return Handle(request);
}

std::string CommandHandler::Handle(const Request& request) {
    requests_.fetch_add(1, std::memory_order_relaxed);
    Status s;
    switch (request.type) {
        case CommandType::kPing:
            return PongResponse();
        case CommandType::kStatus:
            return "STATUS " + backend_->StatusFields() + " requests=" +
                   std::to_string(requests()) + "\n";
        case CommandType::kPut:
            s = backend_->Put(request.key, request.value);
            break;
        case CommandType::kDelete:
            s = backend_->Delete(request.key);
            break;
        case CommandType::kGet: {
            std::string value;
            s = backend_->Get(request.key, &value);
            if (s.ok()) {
                return ValueResponse(value);
            }
            if (s.code() == StatusCode::kNotFound) {
                return NotFoundResponse();
            }
            break;
        }
    }
    if (s.ok()) {
        return OkResponse();
    }
    errors_.fetch_add(1, std::memory_order_relaxed);
    return ErrorResponse(s.ToString());
}

}  // namespace distkv
