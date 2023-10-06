cmake_minimum_required(VERSION 3.9)
project(imgdb VERSION 1.0)
include(CMakePrintHelpers)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_VERBOSE_MAKEFILE ON)

set(CU_ROOT "CppUtility")
cmake_print_variables(CU_ROOT)
file(GLOB_RECURSE CU_FILES "${CU_ROOT}/*.h" "${CU_ROOT}/*.hpp")

option(USE_LIBPQXX "use libpqxx" ON)
option(USE_SQLITE3 "use sqlite3" ON)

find_package(Eigen3 CONFIG REQUIRED)
find_package(OpenCV CONFIG REQUIRED)
find_package(libzip CONFIG REQUIRED)
find_package(FFMPEG REQUIRED)
find_package(Boost REQUIRED)
find_package(unofficial-graphicsmagick CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(range-v3 CONFIG REQUIRED)

set(ID_LIBS
    Eigen3::Eigen
    opencv_core opencv_imgcodecs opencv_dnn opencv_highgui
    libzip::zip
    ${FFMPEG_LIBRARIES}
    unofficial::graphicsmagick::graphicsmagick
    nlohmann_json nlohmann_json::nlohmann_json
    range-v3::range-v3
)

set(ID_DEFS
    UNICODE
    _UNICODE    

    CU_IMG_HAS_OPENCV
    CU_IMG_HAS_GRAPHICSMAGICK

    CU_EXCEPTION_USE_BOOST_STACKTRACE
)

if (WIN32)
    find_package(directxtex CONFIG REQUIRED)
    list(APPEND ID_DEFS CU_IMG_HAS_DIRECTXTEX)
    list(APPEND ID_LIBS Microsoft::DirectXTex)
endif()

if (UNIX)
    find_package(TBB CONFIG REQUIRED)
    list(APPEND ID_LIBS TBB::tbb)
endif()

if (APPLE)
    list(APPEND ID_DEFS _GNU_SOURCE)
endif()

if (${USE_LIBPQXX})
    find_package(libpqxx CONFIG REQUIRED)
    list(APPEND ID_DEFS ID_HAS_LIBPQXX)
    list(APPEND ID_LIBS libpqxx::pqxx)
endif()

if (${USE_SQLITE3})
    find_package(unofficial-sqlite3 CONFIG REQUIRED)
    list(APPEND ID_DEFS ID_HAS_SQLITE3)
    list(APPEND ID_LIBS unofficial::sqlite3::sqlite3)
endif()

add_executable(imgdb "ImageDatabase/ImageDatabase.hpp" "ImageDatabase/main.cpp" ${CU_FILES})

target_compile_definitions(imgdb PRIVATE ${ID_DEFS})

target_include_directories(imgdb PRIVATE
    "ImageDatabase"
    ${FFMPEG_INCLUDE_DIRS}
    ${CU_ROOT}
)

target_link_libraries(imgdb PRIVATE ${ID_LIBS})

if (WIN32)
    target_compile_options(imgdb PRIVATE /bigobj)
endif()