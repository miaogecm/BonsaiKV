file (GLOB pactree_SRCS "*.h" "*.cpp")
add_library(pactree SHARED
    ${pactree_SRCS}
)
target_include_directories(pactree PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries (pactree numa jemalloc pdlart pmemobj pmem)
INSTALL(TARGETS pactree 
        ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR}
        )
