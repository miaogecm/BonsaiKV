# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.22

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:

#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:

# Disable VCS-based implicit rules.
% : %,v

# Disable VCS-based implicit rules.
% : RCS/%

# Disable VCS-based implicit rules.
% : RCS/%,v

# Disable VCS-based implicit rules.
% : SCCS/s.%

# Disable VCS-based implicit rules.
% : s.%

.SUFFIXES: .hpux_make_needs_suffix_list

# Command-line flag to silence nested $(MAKE).
$(VERBOSE)MAKESILENT = -s

#Suppress display of executed commands.
$(VERBOSE).SILENT:

# A target that is always out of date.
cmake_force:
.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E rm -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/hhusjr/Projects/bonsai/evaluation/kvstore/lib/slmdb

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/hhusjr/Projects/bonsai/evaluation/kvstore/lib/slmdb/build

# Include any dependencies generated for this target.
include CMakeFiles/pm_stream.dir/depend.make
# Include any dependencies generated by the compiler for this target.
include CMakeFiles/pm_stream.dir/compiler_depend.make

# Include the progress variables for this target.
include CMakeFiles/pm_stream.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/pm_stream.dir/flags.make

CMakeFiles/pm_stream.dir/util/persist_stream.cc.o: CMakeFiles/pm_stream.dir/flags.make
CMakeFiles/pm_stream.dir/util/persist_stream.cc.o: ../util/persist_stream.cc
CMakeFiles/pm_stream.dir/util/persist_stream.cc.o: CMakeFiles/pm_stream.dir/compiler_depend.ts
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/hhusjr/Projects/bonsai/evaluation/kvstore/lib/slmdb/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object CMakeFiles/pm_stream.dir/util/persist_stream.cc.o"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -MD -MT CMakeFiles/pm_stream.dir/util/persist_stream.cc.o -MF CMakeFiles/pm_stream.dir/util/persist_stream.cc.o.d -o CMakeFiles/pm_stream.dir/util/persist_stream.cc.o -c /home/hhusjr/Projects/bonsai/evaluation/kvstore/lib/slmdb/util/persist_stream.cc

CMakeFiles/pm_stream.dir/util/persist_stream.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/pm_stream.dir/util/persist_stream.cc.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/hhusjr/Projects/bonsai/evaluation/kvstore/lib/slmdb/util/persist_stream.cc > CMakeFiles/pm_stream.dir/util/persist_stream.cc.i

CMakeFiles/pm_stream.dir/util/persist_stream.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/pm_stream.dir/util/persist_stream.cc.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/hhusjr/Projects/bonsai/evaluation/kvstore/lib/slmdb/util/persist_stream.cc -o CMakeFiles/pm_stream.dir/util/persist_stream.cc.s

# Object files for target pm_stream
pm_stream_OBJECTS = \
"CMakeFiles/pm_stream.dir/util/persist_stream.cc.o"

# External object files for target pm_stream
pm_stream_EXTERNAL_OBJECTS =

pm_stream: CMakeFiles/pm_stream.dir/util/persist_stream.cc.o
pm_stream: CMakeFiles/pm_stream.dir/build.make
pm_stream: /usr/local/lib/libvmem.so
pm_stream: CMakeFiles/pm_stream.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/hhusjr/Projects/bonsai/evaluation/kvstore/lib/slmdb/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable pm_stream"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/pm_stream.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/pm_stream.dir/build: pm_stream
.PHONY : CMakeFiles/pm_stream.dir/build

CMakeFiles/pm_stream.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/pm_stream.dir/cmake_clean.cmake
.PHONY : CMakeFiles/pm_stream.dir/clean

CMakeFiles/pm_stream.dir/depend:
	cd /home/hhusjr/Projects/bonsai/evaluation/kvstore/lib/slmdb/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/hhusjr/Projects/bonsai/evaluation/kvstore/lib/slmdb /home/hhusjr/Projects/bonsai/evaluation/kvstore/lib/slmdb /home/hhusjr/Projects/bonsai/evaluation/kvstore/lib/slmdb/build /home/hhusjr/Projects/bonsai/evaluation/kvstore/lib/slmdb/build /home/hhusjr/Projects/bonsai/evaluation/kvstore/lib/slmdb/build/CMakeFiles/pm_stream.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/pm_stream.dir/depend
