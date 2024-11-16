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
find_package(Boost CONFIG REQUIRED system)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(ZXing CONFIG REQUIRED)

find_package(PkgConfig REQUIRED)

find_package(FFMPEG)
if (FFMPEG_FOUND)
    set(ID_FFMPEG_LIBS ${FFMPEG_LIBRARIES})
else ()
    pkg_check_modules(FFMPEG REQUIRED IMPORTED_TARGET
            libavdevice
            libavfilter
            libavformat
            libavcodec
            libswresample
            libswscale
            libavutil
    )
    set(ID_FFMPEG_LIBS PkgConfig::FFMPEG)
endif ()

find_package(unofficial-graphicsmagick CONFIG)
if (unofficial-graphicsmagick_FOUND)
    set(ID_GRAPHICSMAGICK_LIBS unofficial::graphicsmagick::graphicsmagick)
else ()
    pkg_check_modules(GraphicsMagick++ REQUIRED IMPORTED_TARGET GraphicsMagick++)
    set(ID_GRAPHICSMAGICK_LIBS PkgConfig::GraphicsMagick++)
endif ()

find_package(Tesseract CONFIG)
if (Tesseract_FOUND)
    set(ID_TESSETACT_LIBS Tesseract::libtesseract)
else ()
    pkg_check_modules(Tesseract REQUIRED IMPORTED_TARGET tesseract)
    set(ID_TESSETACT_LIBS PkgConfig::Tesseract)
endif ()

find_package(directxtex CONFIG)
if (directxtex_FOUND)
    set(ID_DXTEX_LIBS Microsoft::DirectXTex)
else ()
    set(ID_DXTEX_LIBS)
endif ()

set(ID_LIBS
    Eigen3::Eigen
    opencv_core opencv_imgcodecs opencv_dnn opencv_highgui opencv_features2d
    libzip::zip
    ${ID_FFMPEG_LIBS}
    ${ID_GRAPHICSMAGICK_LIBS}
    nlohmann_json nlohmann_json::nlohmann_json
    ${ID_DXTEX_LIBS}
    ${ID_TESSETACT_LIBS}
    ZXing::ZXing
)

set(ID_DEFS
    UNICODE
    _UNICODE    

    CU_IMG_HAS_OPENCV
    CU_IMG_HAS_GRAPHICSMAGICK

    CU_EXCEPTION_USE_BOOST_STACKTRACE
)

if (directxtex_FOUND)
    list(APPEND ID_DEFS CU_IMG_HAS_DIRECTXTEX)
endif()

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
