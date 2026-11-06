// Smoke tests proving the Asio dependency is wired up correctly (headers,
// ASIO_STANDALONE, threading, sockets) before any networking code is built on it.

#include <gtest/gtest.h>

#include <array>
#include <asio.hpp>
#include <string>

namespace {

TEST(AsioSmokeTest, PostedHandlerRuns) {
    asio::io_context io;
    bool ran = false;
    asio::post(io, [&ran] { ran = true; });
    EXPECT_FALSE(ran);
    io.run();
    EXPECT_TRUE(ran);
}

TEST(AsioSmokeTest, LoopbackTcpRoundTrip) {
    using asio::ip::tcp;

    asio::io_context io;
    tcp::acceptor acceptor(io, tcp::endpoint(asio::ip::address_v4::loopback(), 0));
    const tcp::endpoint server_endpoint = acceptor.local_endpoint();
    ASSERT_NE(server_endpoint.port(), 0);

    // Server: accept one connection and echo back what it reads.
    tcp::socket server_socket(io);
    std::array<char, 4> server_buf{};
    acceptor.async_accept(server_socket, [&](const asio::error_code& ec) {
        ASSERT_FALSE(ec) << ec.message();
        asio::async_read(server_socket, asio::buffer(server_buf),
                         [&](const asio::error_code& read_ec, std::size_t) {
                             ASSERT_FALSE(read_ec) << read_ec.message();
                             asio::async_write(server_socket, asio::buffer(server_buf),
                                               [](const asio::error_code& write_ec, std::size_t) {
                                                   ASSERT_FALSE(write_ec) << write_ec.message();
                                               });
                         });
    });

    // Client: connect, send "ping", read the echo.
    tcp::socket client_socket(io);
    const std::string request = "ping";
    std::array<char, 4> client_buf{};
    client_socket.async_connect(server_endpoint, [&](const asio::error_code& ec) {
        ASSERT_FALSE(ec) << ec.message();
        asio::async_write(client_socket, asio::buffer(request),
                          [&](const asio::error_code& write_ec, std::size_t) {
                              ASSERT_FALSE(write_ec) << write_ec.message();
                              asio::async_read(client_socket, asio::buffer(client_buf),
                                               [](const asio::error_code& read_ec, std::size_t) {
                                                   ASSERT_FALSE(read_ec) << read_ec.message();
                                               });
                          });
    });

    io.run();
    EXPECT_EQ(std::string(client_buf.data(), client_buf.size()), "ping");
}

}  // namespace
