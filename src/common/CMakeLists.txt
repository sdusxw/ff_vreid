
add_library(common
	common.h
	common.cpp
	concurrent_queue.h
	concurrent_queue.cpp
	jpg_codec.h
	jpg_codec.cpp
    pull_jpg2ram.h
    pull_jpg2ram.cpp
    push_json.h
    push_json.cpp
)

target_link_libraries(common pthread jpeg boost_system boost_thread curl)

