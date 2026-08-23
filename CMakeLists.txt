cmake_minimum_required(VERSION 3.20)
project(hid_ingest LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

if(MSVC)
    add_compile_options(/O2 /W4 /permissive- /EHsc)
else()
    add_compile_options(-O3 -Wall -Wextra -pthread)
endif()

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# Core library (header-only core + platform producer)
add_library(hid_ingest_core INTERFACE)
target_include_directories(hid_ingest_core INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)

if(WIN32)
    add_library(hid_ingest_win32 STATIC src/raw_input_producer.cpp)
    target_link_libraries(hid_ingest_win32 PUBLIC hid_ingest_core user32 gdi32)
elseif(UNIX AND NOT APPLE)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(EVDEV REQUIRED libevdev)
    pkg_check_modules(UDEV REQUIRED libudev)
    add_library(hid_ingest_linux STATIC src/linux_evdev_producer.cpp)
    target_include_directories(hid_ingest_linux SYSTEM PRIVATE ${EVDEV_INCLUDE_DIRS} ${UDEV_INCLUDE_DIRS})
    target_link_libraries(hid_ingest_linux PUBLIC hid_ingest_core pthread ${EVDEV_LIBRARIES} ${UDEV_LIBRARIES})
endif()

# Tests
enable_testing()
add_executable(spsc_test tests/spsc_test.cpp)
target_link_libraries(spsc_test PRIVATE hid_ingest_core)
if(NOT MSVC)
    target_link_libraries(spsc_test PRIVATE pthread)
endif()
add_test(NAME spsc COMMAND spsc_test)

# Benchmark (spec 7.1 synthetic 8 kHz stress test)
add_executable(stress_ingest benchmark/stress_ingest.cpp)
target_link_libraries(stress_ingest PRIVATE hid_ingest_core)
if(NOT MSVC)
    target_link_libraries(stress_ingest PRIVATE pthread)
endif()

# Demo
if(WIN32)
    add_executable(console_demo examples/console_demo.cpp)
    target_link_libraries(console_demo PRIVATE hid_ingest_win32)
endif()
