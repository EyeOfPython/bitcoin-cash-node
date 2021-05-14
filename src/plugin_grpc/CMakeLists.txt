cmake_minimum_required(VERSION 3.15)
project(plugin_grpc)

find_package(Protobuf CONFIG REQUIRED)
message(STATUS "Using protobuf ${Protobuf_VERSION}")

set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(_REFLECTION gRPC::grpc++_reflection)
find_program(_PROTOBUF_PROTOC protoc)

# Find gRPC installation
# Looks for gRPCConfig.cmake file installed by gRPC's cmake installation.
find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")

set(_GRPC_GRPCPP gRPC::grpc++)
find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)

message(STATUS "Using protoc executable ${_PROTOBUF_PROTOC}")
message(STATUS "Using grpc_cpp_plugin executable ${_GRPC_CPP_PLUGIN_EXECUTABLE}")

get_filename_component(grpc_proto "protos/plugin_grpc.proto" ABSOLUTE)
get_filename_component(grpc_proto_path "${grpc_proto}" PATH)

set(genproto_folder "${CMAKE_CURRENT_BINARY_DIR}")

message(STATUS "Using output folder ${genproto_folder}")

set(plugin_proto_source "${genproto_folder}/plugin_grpc.pb.cc")
set(plugin_proto_header "${genproto_folder}/plugin_grpc.pb.h")
set(plugin_proto_py "${genproto_folder}/plugin_grpc_pb2.py")
set(plugin_grpc_source "${genproto_folder}/plugin_grpc.grpc.pb.cc")
set(plugin_grpc_header "${genproto_folder}/plugin_grpc.grpc.pb.h")
set(plugin_grpc_py "${genproto_folder}/plugin_grpc_pb2_grpc.py")
add_custom_command(
  OUTPUT "${plugin_proto_source}" "${plugin_proto_header}" "${plugin_grpc_source}" "${plugin_grpc_header}"
  COMMAND ${_PROTOBUF_PROTOC}
  ARGS --grpc_out "${genproto_folder}"
    --cpp_out "${genproto_folder}"
    -I "${grpc_proto_path}"
    --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
    "${grpc_proto}"
  DEPENDS "${grpc_proto}")

add_custom_command(
  OUTPUT "${plugin_proto_py}" "${plugin_grpc_py}"
  COMMAND "${Python_EXECUTABLE}"
  ARGS
    -m grpc_tools.protoc
    -I "${grpc_proto_path}"
    --python_out="${genproto_folder}"
    --grpc_python_out="${genproto_folder}"
    "${grpc_proto}"
  DEPENDS "${grpc_proto}")
add_custom_target(plugin_grpc_py ALL
  DEPENDS "${plugin_proto_py}" "${plugin_grpc_py}")

# Include generated *.pb.h files
include_directories("${genproto_folder}")
include_directories("..")

find_package(Boost 1.58 REQUIRED filesystem)

add_library(plugin_grpc
  plugin_grpc.cpp
  ${plugin_proto_source}
  ${plugin_grpc_source}
)

target_link_libraries(plugin_grpc
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF}
  Boost::filesystem)
