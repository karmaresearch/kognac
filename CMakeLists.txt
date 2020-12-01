cmake_minimum_required (VERSION 2.8)

set(CMAKE_CXX_COMPILER "g++")
set(CMAKE_C_COMPILER "gcc")
#set(CMAKE_VS_PLATFORM_NAME, "x64")

project (kognac)
include(ExternalProject)
set(CMAKE_MACOSX_RPATH 1)

#Set to release mode if nothing is specified
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to 'Release' as none was specified.")
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
    # Set the possible values of build type for cmake-gui
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()


#Add macPorts
# Detect if the "port" command is valid on this system; if so, return full path
EXECUTE_PROCESS(COMMAND which port RESULT_VARIABLE DETECT_MACPORTS OUTPUT_VARIABLE MACPORTS_PREFIX ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)

IF (${DETECT_MACPORTS} EQUAL 0)
    # "/opt/local/bin/port" doesn't have libs, so we get the parent directory
    GET_FILENAME_COMPONENT(MACPORTS_PREFIX ${MACPORTS_PREFIX} DIRECTORY)

    # "/opt/local/bin" doesn't have libs, so we get the parent directory
    GET_FILENAME_COMPONENT(MACPORTS_PREFIX ${MACPORTS_PREFIX} DIRECTORY)

    # "/opt/local" is where MacPorts lives, add `/lib` suffix and link
    LINK_DIRECTORIES(${MACPORTS_PREFIX}/lib)
    INCLUDE_DIRECTORIES(${MACPORTS_PREFIX}/include)
ENDIF()

#Add homebrew
EXECUTE_PROCESS(COMMAND brew --prefix RESULT_VARIABLE exitstatus OUTPUT_VARIABLE BREW_PREFIX ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)

IF (${exitstatus} EQUAL 0)
    LINK_DIRECTORIES(${BREW_PREFIX}/lib)
    INCLUDE_DIRECTORIES(${BREW_PREFIX}/include)
ENDIF()

#add files to compile
file(GLOB kognac_SRC
    "src/kognac/compression/*.cpp"
    "src/kognac/schema/*.cpp"
    "src/kognac/sorting/*.cpp"
    "src/kognac/utils/*.cpp"
    "src/kognac/log/*.cpp"
)

message("CMAKE_CXX_COMPILER_ID = ${CMAKE_CXX_COMPILER_ID}")
#Set compiler options
if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  set(CMAKE_CXX_FLAGS_RELEASE "-DNDEBUG -O3")
  set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g")
  set(COMPILE_FLAGS "-c -fPIC -fsigned-char -std=c++11")
endif()

#Create both a library and the executable program
add_library(kognac-o OBJECT ${kognac_SRC})
add_library(kognac-core SHARED $<TARGET_OBJECTS:kognac-o>)
add_executable(kognac_exec src/launcher/main.cpp)

set_target_properties(kognac-o PROPERTIES COMPILE_FLAGS "${COMPILE_FLAGS}")
set_target_properties(kognac_exec PROPERTIES COMPILE_FLAGS "${COMPILE_FLAGS}")

#ZLIB
find_package(ZLIB REQUIRED)

#PTHREADS
find_package(Threads REQUIRED)
if (THREADS_HAVE_PTHREAD_ARG)
    target_compile_options(PUBLIC kognac_exec "-pthread")
endif()

#SPARSEHASH
find_path (sparsehash sparsehash/sparse_hash_map)
IF (${sparsehash} STREQUAL "sparsehash-NOTFOUND")
    message("Could not find sparsehash. I'm going to download it from the GIT repository ...")
    ExternalProject_Add(git-sparsehash
        DOWNLOAD_COMMAND git clone https://github.com/sparsehash/sparsehash.git
        DOWNLOAD_DIR external
        SOURCE_DIR external/sparsehash
        CONFIGURE_COMMAND ./configure
        BUILD_IN_SOURCE 1
        BUILD_COMMAND make src/sparsehash/internal/sparseconfig.h
        INSTALL_COMMAND ""
    )
    ExternalProject_Get_Property(git-sparsehash SOURCE_DIR)
    include_directories(${SOURCE_DIR}/src/)
    add_dependencies(kognac-o git-sparsehash)
ELSE()
    message("Found sparsehash")
ENDIF()

#LZ4
# we need it statically included, so download it, not only if it cannot be found!
# find_library(lz4 lz4)
# find_path (lz4h lz4.h)
# IF (${lz4h} STREQUAL "lz4h-NOTFOUND")
#    message("Could not find LZ4. I'm going to download it from the GIT repository ...")
    message("Downloading lz4, static version required")
    ExternalProject_Add(git-lz4
        DOWNLOAD_COMMAND git clone https://github.com/Cyan4973/lz4.git
        DOWNLOAD_DIR external
        SOURCE_DIR external/lz4/
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE 1
        BUILD_COMMAND make -C lib lib MOREFLAGS=-fPIC
        INSTALL_COMMAND ""
    )
    ExternalProject_Get_Property(git-lz4 SOURCE_DIR)
    include_directories(${SOURCE_DIR}/lib/)
    add_library(lz4 STATIC IMPORTED)
    set_property(TARGET lz4 PROPERTY IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/external/lz4/lib/${CMAKE_STATIC_LIBRARY_PREFIX}lz4${CMAKE_STATIC_LIBRARY_SUFFIX})
    add_dependencies(lz4 git-lz4)
    add_dependencies(kognac-o lz4)
#ELSE()
#    include_directories(lz4h)
#message("Found LZ4, lz4=${lz4}")
#ENDIF()

#standard include
include_directories(include/)
TARGET_LINK_LIBRARIES(kognac-core lz4)
TARGET_LINK_LIBRARIES(kognac-core "${ZLIB_LIBRARIES}")
TARGET_LINK_LIBRARIES(kognac_exec kognac-core lz4)
TARGET_LINK_LIBRARIES(kognac_exec "${ZLIB_LIBRARIES}")
if (CMAKE_THREAD_LIBS_INIT)
    TARGET_LINK_LIBRARIES(kognac_exec "${CMAKE_THREAD_LIBS_INIT}")
endif()
