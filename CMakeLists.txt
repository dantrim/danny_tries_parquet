cmake_minimum_required(VERSION 3.15 FATAL_ERROR)

project(parquet-test)

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}")

set(CMAKE_CXX_STANDARD 17)

# requires environment ARROW_HOME = /usr/local/Cellar/apache-arrow/5.0.0_1
find_package(Arrow REQUIRED)
# requires environment PARQUET_HOME = /usr/local/Cellar/apache-arrow/5.0.0_1
find_package(Parquet REQUIRED)

add_executable(parquet-test src/cpp/parquet-test.cpp)
target_link_libraries(parquet-test ${ARROW_SHARED_LIB} ${PARQUET_SHARED_LIB})
target_include_directories(parquet-test PRIVATE ${ARROW_INCLUDE_DIR} ${PARQUET_INCLUDE_DIR} src/cpp)

add_executable(dataset-gen src/cpp/dataset-gen.cpp)
target_link_libraries(dataset-gen ${ARROW_SHARED_LIB} ${PARQUET_SHARED_LIB})
target_include_directories(dataset-gen PRIVATE ${ARROW_INCLUDE_DIR} ${PARQUET_INCLUDE_DIR} src/cpp)

add_executable(parquet-test2 src/cpp/parquet-test2.cpp)
target_link_libraries(parquet-test2 ${ARROW_SHARED_LIB} ${PARQUET_SHARED_LIB})
target_include_directories(parquet-test2 PRIVATE ${ARROW_INCLUDE_DIR} ${PARQUET_INCLUDE_DIR})
