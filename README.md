KOGNAC.

==Installation==

The project requires the Boost libraries, which must be compiled with
multi-threading support and made available in accessable locations. KOGNAC uses
also the LZ4 library, but if this library is not available then it will
automatically download it.

We used CMake to ease the installation process. To build KOGNAC, the following
commands should suffice:

mkdir build

cd build

cmake ..

(If you want to build the DEBUG version of the library, add the parameter: -DCMAKE_BUILD_TYPE=Debug. e.g. cmake -DCMAKE_BUILD_TYPE=Debug ..)

make

==Potential problems==

- The system already has a old version of Boost installed.

- In this case, you can override the Boost settings by setting the variables:
BOOST_NO_BOOST_CMAKE=TRUE
BOOST_NO_SYSTEM_PATHS=TRUE
BOOST_ROOT=<path to the BOOST directory>

==Compilation with Visual Studio==

This library can be compiled using Visual Studio, but some additional steps must be done because CMake does not prepare a 100% ready project.
First, you must install CMake for Windows. Then, you must ensure that you have installed the three external libraries used by KOGNAC: lz4, zlib, and google-sparsehash (this last library only contains headers). The libraries lz4 and google-sparsehash should be compiled as static libraries. Below, we assume the include files for these variables are available at the full paths <ext_includes> while the binaries are available at <ext_libraries>.

First, download with git the entire repository. Then, create a directory, e.g., "build", and from this directory type

``
cmake .. -G "Visual Studio 16 Win64" (Assuming you have Visual Studio 15 installed)
``

The "Win64" parameter is important because it instructs the compiler to compile the "x64" version of the program.
If the external libraries are installed in non-standard locations, then ensure that the environmental variables CMAKE_INCLUDE_PATH contains <ext_includes> and CMAKE_LIBRARY_PATH includes <ext_libraries>.

After cmake is terminated, "build" will contain a series of files. One of these should be "kognac.sln". Open it with Visual Studio. Unfortunately, CMake does not set up correctly the dependencies to external libraries. Thus, for the projects "kognac" and "kognac_exec" we must open "Project"->"Property"->"VC++ Directories" and add <ext_includes> to "Include directories". We might also change "Project"->"Property"->"Linker"->"Input"->"Additional Dependencies" for the project "kognac_exec" to point to <ext_libraries> for linking to the external libraries. Finally, we must pass a special directive to kognac-log because Windows does not export automatically all symbols. To this end, we must open "Project"->"Property"->"C/C++"->"Preprocessor"->"Preprocessor Definitions" and add the flag "LOG_SHARED_LIB".

If everything is fine, then "Build"->"Build Solution" should terminate successfully and kognac is available.
