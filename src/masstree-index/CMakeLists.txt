cmake_minimum_required(VERSION 3.16)
set(MASSTREEWRAPPER_DESCRIPTION "masstree-wrapper is a wrapper of masstree-beta (https://github.com/kohler/masstree-beta.git) written by Eddie Kohler.")
project(
  masstree-wrapper
  VERSION 1.0.0.0
  DESCRIPTION "${MASSTREEWRAPPER_DESCRIPTION}"
  )

message(STATUS "Welcome to mastree-wrapper, Version: ${PROJECT_VERSION}")
set(CMAKE_C_COMPILER "gcc" CACHE STRING "C compiler")
set(CMAKE_CXX_COMPILER "g++" CACHE STRING "C++ compiler")
message(STATUS "C Compiler: ${CMAKE_C_COMPILER} ${CMAKE_C_COMPILER_ID} ${CMAKE_C_COMPILER_VERSION}")
message(STATUS "C++ Compiler: ${CMAKE_CXX_COMPILER} ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "System: ${CMAKE_SYSTEM_NAME} ${CMAKE_SYSTEM_VERSION} ${CMAKE_SYSTEM_PROCESSOR}")

# Set a default build type if none was specified
set(default_build_type "Release")

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to '${default_build_type}' as none was specified.")
  set(CMAKE_BUILD_TYPE "${default_build_type}" CACHE
      STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
    "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()

set(CMAKE_CXX_FLAGS_DEBUG          "-O0 -g")
set(CMAKE_CXX_FLAGS_MINSIZEREL     "-Os -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE        "-Ofast -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-Ofast -DNDEBUG -g")
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_EXTENSIONS OFF)
list(APPEND COMMON_COMPILE_FLAGS "-W" "-Wall" "-fPIC")

set(CMAKE_EXPORT_COMPILE_COMMANDS ON) # For language server.

include(FetchContent)
function(add_dep NAME GIT_URL GIT_TAG)
    string(TOLOWER "${NAME}" NAME_LOWER)
    FetchContent_Declare(${NAME})
    if (NOT ${NAME}_POPULATED)
        FetchContent_Populate(${NAME}
                QUIET
                SOURCE_DIR ${CMAKE_BINARY_DIR}/_deps/src/${NAME}
                BINARY_DIR ${CMAKE_BINARY_DIR}/_deps/build/${NAME}
                SUBBUILD_DIR ${CMAKE_BINARY_DIR}/_deps/sub/${NAME}
                GIT_REPOSITORY ${GIT_URL}
                GIT_TAG ${GIT_TAG}
                )
        
        set(DEP_CUSTOM_CMAKELISTS ${PROJECT_SOURCE_DIR}/third_party/deps_override/${NAME}_CMakeLists.txt)
        if (EXISTS ${DEP_CUSTOM_CMAKELISTS})
            file(COPY ${DEP_CUSTOM_CMAKELISTS} DESTINATION ${${NAME_LOWER}_SOURCE_DIR})
            file(RENAME ${${NAME_LOWER}_SOURCE_DIR}/${NAME}_CMakeLists.txt ${${NAME_LOWER}_SOURCE_DIR}/CMakeLists.txt)
            message(STATUS "Copied ${DEP_CUSTOM_CMAKELISTS} -> ${${NAME_LOWER}_SOURCE_DIR}/CMakeLists.txt.")
        endif ()
        unset(DEP_CUSTOM_CMAKELISTS)

        add_subdirectory(${${NAME_LOWER}_SOURCE_DIR} ${${NAME_LOWER}_BINARY_DIR})
    endif ()

    execute_process(COMMAND git log -1 --format=%cd --date=short
            WORKING_DIRECTORY ${${NAME_LOWER}_SOURCE_DIR}
            OUTPUT_VARIABLE GIT_LAST_COMMIT)
    string(STRIP "${GIT_LAST_COMMIT}" GIT_LAST_COMMIT)

    message(STATUS "[FOUND] ${NAME} (${GIT_URL} ${GIT_TAG} ${GIT_LAST_COMMIT})")
endfunction()

add_dep(masstree https://github.com/kohler/masstree-beta.git master)

set(MASSTREEWRAPPER_INCLUDE_DIRECTORIES "")
set(MASSTREEWRAPPER_COMPILE_DEFINITIONS "")
set(MASSTREEWRAPPER_COMPILE_OPTIONS "")
set(MASSTREEWRAPPER_LINK_OPTIONS "")

list(APPEND MASSTREEWRAPPER_INCLUDE_DIRECTORIES "${CMAKE_BINARY_DIR}/_deps/src/" "${PROJECT_SOURCE_DIR}/src/" "${PROJECT_SOURCE_DIR}/include/")
list(APPEND MASSTREEWRAPPER_COMPILE_OPTIONS "${COMMON_COMPILE_FLAGS}")
list(APPEND MASSTREEWRAPPER_LINK_OPTIONS "-pthread")

file(GLOB_RECURSE EXECUTABLES "${PROJECT_SOURCE_DIR}/src/*.cpp")
foreach (EXECUTABLE ${EXECUTABLES})
  get_filename_component(FILENAME ${EXECUTABLE} NAME_WE)
  add_executable(${FILENAME} ${EXECUTABLE})

  target_include_directories(${FILENAME} PUBLIC "${MASSTREEWRAPPER_INCLUDE_DIRECTORIES}")
  target_compile_options(${FILENAME} PRIVATE "${MASSTREEWRAPPER_COMPILE_OPTIONS}")
  target_link_libraries(${FILENAME} PUBLIC masstree)
  target_link_options(${FILENAME} PUBLIC "${MASSTREEWRAPPER_LINK_OPTIONS}")

  set_target_properties(${FILENAME} PROPERTIES CXX_EXTENTIONS OFF ENABLE_EXPORTS ON)
  set_target_properties(
    ${FILENAME}
    PROPERTIES
    ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
  )
endforeach (EXECUTABLE ${EXECUTABLES})

add_library(masstree_index SHARED adapter.cpp)

target_include_directories(masstree_index PUBLIC "${MASSTREEWRAPPER_INCLUDE_DIRECTORIES}")
target_compile_options(masstree_index PRIVATE "${MASSTREEWRAPPER_COMPILE_OPTIONS}")
target_link_libraries(masstree_index PUBLIC masstree)
target_link_options(masstree_index PUBLIC "${MASSTREEWRAPPER_LINK_OPTIONS}")

set_target_properties(masstree_index PROPERTIES CXX_EXTENTIONS OFF ENABLE_EXPORTS ON)
set_target_properties(
masstree_index
PROPERTIES
ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)
