cmake_minimum_required(VERSION 3.9)
project(imgdb VERSION 1.0)
include(CMakePrintHelpers)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_VERBOSE_MAKEFILE ON)

set(CU_ROOT "CppUtility")
cmake_print_variables(CU_ROOT)
file(GLOB_RECURSE CU_FILES "${CU_ROOT}/*.h" "${CU_ROOT}/*.hpp")

find_package(Eigen3 CONFIG REQUIRED)
find_package(OpenCV CONFIG REQUIRED)
find_package(libzip CONFIG REQUIRED)
find_package(FFMPEG REQUIRED)
find_package(Boost REQUIRED system)
find_package(unofficial-graphicsmagick CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(directxtex CONFIG REQUIRED)
find_package(Tesseract CONFIG REQUIRED)
find_package(ZXing CONFIG REQUIRED)

set(ID_LIBS
    Eigen3::Eigen
    opencv_core opencv_imgcodecs opencv_dnn opencv_highgui opencv_features2d opencv_cudafeatures2d
    libzip::zip
    ${FFMPEG_LIBRARIES}
    unofficial::graphicsmagick::graphicsmagick
    nlohmann_json nlohmann_json::nlohmann_json
    Microsoft::DirectXTex
    Tesseract::libtesseract
    ZXing::ZXing
)

set(ID_DEFS
    UNICODE
    _UNICODE    

    CU_IMG_HAS_OPENCV
    CU_IMG_HAS_GRAPHICSMAGICK
    CU_IMG_HAS_DIRECTXTEX

    CU_EXCEPTION_USE_BOOST_STACKTRACE
)

if (UNIX)
    find_package(TBB CONFIG REQUIRED)
    list(APPEND ID_LIBS TBB::tbb)
endif()


add_executable(imgdb "ImageDatabase/main.cpp" ${CU_FILES})

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