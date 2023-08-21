cmake_minimum_required (VERSION 2.8)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp")

project(pqnns-multi-stream)

# Required Libs
find_package(MPI REQUIRED)
find_package(Boost REQUIRED)

include_directories(SYSTEM ${MPI_INCLUDE_PATH})
include_directories(src)
include_directories(include)

link_directories(/opt/packages/cuda/10.1/lib64)
file(GLOB_RECURSE SRC src/*.cpp)
file(GLOB_RECURSE INCLUDE include/*.h)
list(REMOVE_ITEM SRC "src/main.cpp")

add_executable(pqnns-multi-stream src/main.cpp ${SRC} ${INCLUDE})
target_link_libraries(pqnns-multi-stream MPI::MPI_CXX -lfaiss -lboost_thread)