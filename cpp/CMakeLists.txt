cmake_minimum_required(VERSION 3.18)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

project(nn)

# Change to your vcpkg.cmake path or install libraries manually and remove this
# line
include(C:/vcpkg/scripts/buildsystems/vcpkg.cmake)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src
                    ${CMAKE_CURRENT_SOURCE_DIR}/includes)
set(Torch_DIR "C:/libtorch/cmake")

# Install these libraries
find_package(Torch REQUIRED)
find_package(Eigen3 CONFIG REQUIRED)
find_package(
  Python3
  COMPONENTS Development
  REQUIRED)
find_package(InfluxDB CONFIG REQUIRED)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${TORCH_CXX_FLAGS}")

set(SOURCES src/nn.cpp src/nn.h)

add_executable(nn ${SOURCES})

target_include_directories(nn PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/includes)
target_include_directories(nn PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)

target_link_libraries(nn PRIVATE "${TORCH_LIBRARIES}")
target_link_libraries(nn PRIVATE Eigen3::Eigen)
target_link_libraries(nn PRIVATE Python3::Python)
target_link_libraries(nn PRIVATE InfluxData::InfluxDB)

if(MSVC)
  file(GLOB TORCH_DLLS "C:/libtorch/lib/*.dll")
  add_custom_command(
    TARGET nn
    POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different ${TORCH_DLLS}
            $<TARGET_FILE_DIR:nn>)
endif(MSVC)
