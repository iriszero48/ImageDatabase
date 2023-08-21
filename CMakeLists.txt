cmake_minimum_required(VERSION 3.9)
project(imgdb VERSION 1.0)
include(CMakePrintHelpers)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_VERBOSE_MAKEFILE ON)

option(CU_ROOT "CppUtility Root Path" "CppUtility")
cmake_print_variables(CU_ROOT)
file(GLOB_RECURSE CU_FILES "${CU_ROOT}/*.h" "${CU_ROOT}/*.hpp")

# find_package(Threads REQUIRED)
find_package(Eigen3 CONFIG REQUIRED)
find_package(OpenCV CONFIG REQUIRED)
find_package(libzip CONFIG REQUIRED)
# find_package(Boost REQUIRED COMPONENTS context)
find_package(FFMPEG REQUIRED)
# find_package(TBB CONFIG REQUIRED)
find_package(Boost REQUIRED)
find_package(unofficial-graphicsmagick CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(range-v3 CONFIG REQUIRED)
find_package(directxtex CONFIG REQUIRED)

add_executable(imgdb "ImageDatabase/ImageDatabase.hpp" "ImageDatabase/main.cpp" ${CU_FILES})

target_compile_definitions(imgdb PRIVATE
    CU_IMG_HAS_OPENCV
    CU_IMG_HAS_GRAPHICSMAGICK
    CU_IMG_HAS_DIRECTXTEX

    UNICODE
    _UNICODE
)

target_include_directories(imgdb PRIVATE
    "ImageDatabase"
    ${FFMPEG_INCLUDE_DIRS}
    ${CU_ROOT}
)

target_link_libraries(imgdb PRIVATE
    # Threads::Threads
    Eigen3::Eigen
    opencv_core opencv_imgcodecs opencv_dnn opencv_highgui
    libzip::zip
    # Boost::boost Boost::context
    ${FFMPEG_LIBRARIES}
    # TBB::tbb
    unofficial::graphicsmagick::graphicsmagick
    nlohmann_json nlohmann_json::nlohmann_json
    range-v3::range-v3
    Microsoft::DirectXTex
)

# set_target_properties(imgdb PROPERTIES LINKER_LANGUAGE CXX)
