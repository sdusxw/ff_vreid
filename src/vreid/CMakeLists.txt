include_directories(
	${SOLUTION_INCLUDE_DIRS}
    /usr/local/include/cppkafka/
)
link_directories(
	${SOLUTION_LINK_DIRS}
    ${src_dir}/lib
    /usr/local/lib64
)

add_executable(vreid
    httplib.h
    lpr_alg.h
    lpr_alg.cpp
    vreid.cpp
    TH_PlateID.h
    TH_ErrorDef.h
)

target_link_libraries(vreid pthread jsoncpp common boost_thread boost_system thplateid curl ssl crypto cppkafka)
