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
include CMakeFiles/ff_btree_test.dir/depend.make
# Include any dependencies generated by the compiler for this target.
include CMakeFiles/ff_btree_test.dir/compiler_depend.make

# Include the progress variables for this target.
include CMakeFiles/ff_btree_test.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/ff_btree_test.dir/flags.make

CMakeFiles/ff_btree_test.dir/index/ff_btree_test.cc.o: CMakeFiles/ff_btree_test.dir/flags.make
CMakeFiles/ff_btree_test.dir/index/ff_btree_test.cc.o: ../index/ff_btree_test.cc
CMakeFiles/ff_btree_test.dir/index/ff_btree_test.cc.o: CMakeFiles/ff_btree_test.dir/compiler_depend.ts
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/hhusjr/Projects/bonsai/evaluation/kvstore/lib/slmdb/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object CMakeFiles/ff_btree_test.dir/index/ff_btree_test.cc.o"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -MD -MT CMakeFiles/ff_btree_test.dir/index/ff_btree_test.cc.o -MF CMakeFiles/ff_btree_test.dir/index/ff_btree_test.cc.o.d -o CMakeFiles/ff_btree_test.dir/index/ff_btree_test.cc.o -c /home/hhusjr/Projects/bonsai/evaluation/kvstore/lib/slmdb/index/ff_btree_test.cc

CMakeFiles/ff_btree_test.dir/index/ff_btree_test.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/ff_btree_test.dir/index/ff_btree_test.cc.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/hhusjr/Projects/bonsai/evaluation/kvstore/lib/slmdb/index/ff_btree_test.cc > CMakeFiles/ff_btree_test.dir/index/ff_btree_test.cc.i

CMakeFiles/ff_btree_test.dir/index/ff_btree_test.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/ff_btree_test.dir/index/ff_btree_test.cc.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/hhusjr/Projects/bonsai/evaluation/kvstore/lib/slmdb/index/ff_btree_test.cc -o CMakeFiles/ff_btree_test.dir/index/ff_btree_test.cc.s

# Object files for target ff_btree_test
ff_btree_test_OBJECTS = \
"CMakeFiles/ff_btree_test.dir/index/ff_btree_test.cc.o"

# External object files for target ff_btree_test
ff_btree_test_EXTERNAL_OBJECTS =

ff_btree_test: CMakeFiles/ff_btree_test.dir/index/ff_btree_test.cc.o
ff_btree_test: CMakeFiles/ff_btree_test.dir/build.make
ff_btree_test: libleveldb.a
ff_btree_test: /usr/lib/x86_64-linux-gnu/libpthread.a
ff_btree_test: /usr/local/lib/libpmem.so
ff_btree_test: /usr/local/lib/libpmemobj.so
ff_btree_test: /usr/local/lib/libpmemlog.so
ff_btree_test: /usr/local/lib/libvmem.so
ff_btree_test: CMakeFiles/ff_btree_test.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/hhusjr/Projects/bonsai/evaluation/kvstore/lib/slmdb/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable ff_btree_test"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/ff_btree_test.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/ff_btree_test.dir/build: ff_btree_test
.PHONY : CMakeFiles/ff_btree_test.dir/build

CMakeFiles/ff_btree_test.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/ff_btree_test.dir/cmake_clean.cmake
.PHONY : CMakeFiles/ff_btree_test.dir/clean

CMakeFiles/ff_btree_test.dir/depend:
	cd /home/hhusjr/Projects/bonsai/evaluation/kvstore/lib/slmdb/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/hhusjr/Projects/bonsai/evaluation/kvstore/lib/slmdb /home/hhusjr/Projects/bonsai/evaluation/kvstore/lib/slmdb /home/hhusjr/Projects/bonsai/evaluation/kvstore/lib/slmdb/build /home/hhusjr/Projects/bonsai/evaluation/kvstore/lib/slmdb/build /home/hhusjr/Projects/bonsai/evaluation/kvstore/lib/slmdb/build/CMakeFiles/ff_btree_test.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/ff_btree_test.dir/depend

