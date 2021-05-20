cmake_minimum_required(VERSION 3.15)
project(plugin_nng)

if(BUILD_BITCOIN_PLUGIN_NNG)
  find_package(nng CONFIG REQUIRED)

  find_package(flatbuffers CONFIG REQUIRED)
  find_program(_FLATC flatc)

  message(STATUS "Using flatc executable ${_FLATC}")

  get_filename_component(flatbuffers_file "flatbuffers/plugin_interface.fbs" ABSOLUTE)
  get_filename_component(flatbuffers_path "${flatbuffers_file}" PATH)

  set(genflatbuf_folder "${CMAKE_CURRENT_BINARY_DIR}")

  message(STATUS "Using output folder ${genflatbuf_folder}")

  set(plugin_flatbuf_h "${genflatbuf_folder}/plugin_interface_generated.h")
  set(plugin_flatbuf_py "${genflatbuf_folder}/PluginInterface")
  add_custom_command(
    OUTPUT "${plugin_flatbuf_h}" "${plugin_flatbuf_py}"
    COMMAND ${_FLATC}
    ARGS -o "${genflatbuf_folder}"
      --cpp
      --python
      "${flatbuffers_file}"
    DEPENDS "${flatbuffers_file}")

  add_custom_target(plugin_files ALL
    DEPENDS "${plugin_flatbuf_h}" "${plugin_flatbuf_py}")

  # Include generated *.pb.h files
  include_directories("${genflatbuf_folder}")
  include_directories("..")
  include_directories("../univalue/include")

  find_package(Boost 1.58 REQUIRED filesystem)

  add_library(plugin_nng
    plugin_nng.cpp
  )

  target_link_libraries(plugin_nng
    nng::nng
    Boost::filesystem)
else()
  add_library(plugin_nng
    plugin_nng_stub.cpp
  )
endif()
