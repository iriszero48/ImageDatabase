cmake_minimum_required(VERSION 3.9)
project(imgdb VERSION 1.0)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_VERBOSE_MAKEFILE ON)

find_package(Threads REQUIRED)
find_package(Eigen3 CONFIG REQUIRED)
find_package(OpenCV CONFIG REQUIRED)
find_package(libzip CONFIG REQUIRED)
find_package(FFMPEG REQUIRED)
find_package(TBB CONFIG REQUIRED)

aux_source_directory(ImageDatabase src)
add_executable(imgdb ${src})

target_include_directories(imgdb PRIVATE
    ${FFMPEG_INCLUDE_DIRS}
)

target_link_libraries(imgdb PRIVATE
    Threads::Threads
    Eigen3::Eigen
    opencv_core opencv_imgcodecs opencv_dnn opencv_highgui
    libzip::zip
    ${FFMPEG_LIBRARIES}
    TBB::tbb
)
