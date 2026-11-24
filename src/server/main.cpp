// kvstore_server: one DistKV node serving the text protocol over TCP.
//
//   kvstore_server --port=7001 --data-dir=./data --threads=4
//
// With --node-id and --peers it also joins a Raft group over a separate peer
// port and takes part in leader election:
//
//   kvstore_server --port=7001 --data-dir=./n1 --node-id=1 --raft-port=7101 \
//                  --peers=2@127.0.0.1:7102,3@127.0.0.1:7103
//
// Stops cleanly on SIGINT / SIGTERM.

#include <csignal>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>

#include "network/tcp_server.h"
#include "raft/asio_transport.h"
#include "raft/raft_node.h"
#include "server/command_handler.h"
#include "server/kv_backend.h"
#include "storage/storage_engine.h"

namespace {

void PrintUsage() {
    std::cerr << "usage: kvstore_server [options]\n"
                 "  --host=ADDR            listen address (default 0.0.0.0)\n"
                 "  --port=N               client port (default 7001)\n"
                 "  --data-dir=PATH        storage directory (default ./data)\n"
                 "  --threads=N            io threads (default 4)\n"
                 "  --max-connections=N    connection limit (default 1024)\n"
                 "  --memtable-mb=N        flush threshold (default 4)\n"
                 "  --sync=always|none     fsync each write (default always)\n"
                 "raft (--node-id and --peers to join a group):\n"
                 "  --node-id=N            this node's id, 1 or more\n"
                 "  --raft-port=N          peer port (default 7101)\n"
                 "  --peers=ID@HOST:PORT,...  the other members\n";
}

bool ParseSize(std::string_view text, size_t* out) {
    if (text.empty()) return false;
    size_t n = 0;
    for (const char c : text) {
        if (c < '0' || c > '9') return false;
        n = n * 10 + static_cast<size_t>(c - '0');
        if (n > (size_t{1} << 40)) return false;
    }
    *out = n;
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    distkv::ServerOptions server_options;
    distkv::EngineOptions engine_options;
    std::string data_dir = "./data";
    distkv::raft::NodeId node_id = distkv::raft::kNoNode;
    uint16_t raft_port = 7101;
    std::vector<distkv::raft::PeerAddress> peers;

    for (int i = 1; i < argc; ++i) {
        const std::string_view arg(argv[i]);
        const size_t eq = arg.find('=');
        const std::string_view name = arg.substr(0, eq);
        const std::string_view value = eq == std::string_view::npos ? "" : arg.substr(eq + 1);
        size_t n = 0;

        if (name == "--help" || name == "-h") {
            PrintUsage();
            return 0;
        } else if (name == "--host" && !value.empty()) {
            server_options.host = std::string(value);
        } else if (name == "--port" && ParseSize(value, &n) && n <= 65535) {
            server_options.port = static_cast<uint16_t>(n);
        } else if (name == "--data-dir" && !value.empty()) {
            data_dir = std::string(value);
        } else if (name == "--threads" && ParseSize(value, &n) && n > 0) {
            server_options.threads = n;
        } else if (name == "--max-connections" && ParseSize(value, &n) && n > 0) {
            server_options.max_connections = n;
        } else if (name == "--memtable-mb" && ParseSize(value, &n) && n > 0) {
            engine_options.memtable_bytes = n << 20;
        } else if (name == "--node-id" && ParseSize(value, &n) && n > 0 && n <= UINT32_MAX) {
            node_id = static_cast<distkv::raft::NodeId>(n);
        } else if (name == "--raft-port" && ParseSize(value, &n) && n <= 65535) {
            raft_port = static_cast<uint16_t>(n);
        } else if (name == "--peers") {
            std::string error;
            if (!distkv::raft::ParsePeerList(value, &peers, &error)) {
                std::cerr << "bad --peers: " << error << "\n";
                return 2;
            }
        } else if (name == "--sync" && (value == "always" || value == "none")) {
            engine_options.wal_sync =
                value == "always" ? distkv::WalSyncMode::kEveryAppend : distkv::WalSyncMode::kNone;
        } else {
            std::cerr << "bad argument: " << arg << "\n";
            PrintUsage();
            return 2;
        }
    }

    if ((node_id == distkv::raft::kNoNode) != peers.empty()) {
        std::cerr << "--node-id and --peers go together\n";
        return 2;
    }

    std::unique_ptr<distkv::StorageEngine> engine;
    distkv::Status s = distkv::StorageEngine::Open(data_dir, engine_options, &engine);
    if (!s.ok()) {
        std::cerr << "failed to open storage at " << data_dir << ": " << s.ToString() << "\n";
        return 1;
    }

    // Raft: the transport hands incoming messages to the node, and the node
    // sends through the transport, so the node pointer is filled in before
    // the transport starts delivering.
    std::unique_ptr<distkv::raft::RaftNode> raft;
    std::unique_ptr<distkv::raft::AsioTransport> transport;
    if (node_id != distkv::raft::kNoNode) {
        distkv::raft::TransportOptions to;
        to.listen_host = server_options.host;
        to.listen_port = raft_port;
        to.peers = peers;
        transport = std::make_unique<distkv::raft::AsioTransport>(
            to, [&raft](const distkv::raft::RpcMessage& m) { raft->Step(m); });

        distkv::raft::RaftOptions ro;
        ro.id = node_id;
        for (const auto& p : peers) ro.peers.push_back(p.id);
        ro.data_dir = data_dir + "/raft";
        ro.sync = engine_options.wal_sync == distkv::WalSyncMode::kEveryAppend;
        ro.on_state_change = [](const distkv::raft::RaftStatus& st) {
            std::cout << "raft: node " << st.id << " is " << distkv::raft::RoleName(st.role)
                      << " in term " << st.term;
            if (st.leader_id != distkv::raft::kNoNode && st.leader_id != st.id) {
                std::cout << " (leader " << st.leader_id << ")";
            }
            std::cout << std::endl;
        };
        s = distkv::raft::RaftNode::Open(ro, transport.get(), &raft);
        if (s.ok()) s = transport->Start();
        if (!s.ok()) {
            std::cerr << "raft: " << s.ToString() << "\n";
            return 1;
        }
        raft->Start();
        std::cout << "raft: node " << node_id << " peers on port " << transport->port()
                  << std::endl;
    }

    distkv::StandaloneBackend backend(engine.get());
    distkv::CommandHandler handler(&backend);
    distkv::TcpServer server(server_options, &handler);
    s = server.Start();
    if (!s.ok()) {
        std::cerr << s.ToString() << "\n";
        return 1;
    }
    std::cout << "distkv listening on " << server_options.host << ":" << server.port()
              << " (data " << data_dir << ", " << server_options.threads << " io threads)"
              << std::endl;

    boost::asio::io_context signals_io;
    boost::asio::signal_set signals(signals_io, SIGINT, SIGTERM);
    signals.async_wait([&](const boost::system::error_code&, int signo) {
        std::cout << "received signal " << signo << ", shutting down" << std::endl;
    });
    signals_io.run();

    server.Stop();   // closes connections; in-flight requests finish first
    if (transport) transport->Stop();  // no more incoming raft messages
    if (raft) raft->Stop();
    engine.reset();  // waits for background flush and compaction
    std::cout << "stopped" << std::endl;
    return 0;
}
