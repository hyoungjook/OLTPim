set_property(GLOBAL APPEND PROPERTY ALL_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc-config.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../bench.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../dbtest.cc
)

add_executable(tpcc_SI_sequential ${CMAKE_CURRENT_SOURCE_DIR}/tpcc-sequential.cc)
set_target_properties(tpcc_SI_sequential PROPERTIES COMPILE_FLAGS "")
#set_target_properties(corobase_SI PROPERTIES COMPILE_FLAGS "-DCORO_BATCH_COMMIT -DBATCH_SAME_TRX -DCORO_UPDATE_VERSION_CHAIN")
target_link_libraries(tpcc_SI_sequential corobase_si)

add_executable(tpcc_SI_simple_coro ${CMAKE_CURRENT_SOURCE_DIR}/tpcc-simple-coro.cc)
set_target_properties(tpcc_SI_simple_coro PROPERTIES COMPILE_FLAGS "")
#set_target_properties(corobase_SI PROPERTIES COMPILE_FLAGS "-DCORO_BATCH_COMMIT -DBATCH_SAME_TRX -DCORO_UPDATE_VERSION_CHAIN")
target_link_libraries(tpcc_SI_simple_coro corobase_si)

add_executable(tpcc_SI_hybrid_coro ${CMAKE_CURRENT_SOURCE_DIR}/tpcc-hybrid-coro.cc)
set_target_properties(tpcc_SI_hybrid_coro PROPERTIES COMPILE_FLAGS "")
#set_target_properties(corobase_SI PROPERTIES COMPILE_FLAGS "-DCORO_BATCH_COMMIT -DBATCH_SAME_TRX -DCORO_UPDATE_VERSION_CHAIN")
target_link_libraries(tpcc_SI_hybrid_coro corobase_hybrid_coro_si)

if(NOT NO_UPMEM)
add_executable(tpcc_SI_oltpim ${CMAKE_CURRENT_SOURCE_DIR}/tpcc-oltpim.cc)
set_target_properties(tpcc_SI_oltpim PROPERTIES COMPILE_FLAGS "-DOLTPIM")
#set_target_properties(corobase_SI PROPERTIES COMPILE_FLAGS "-DCORO_BATCH_COMMIT -DBATCH_SAME_TRX -DCORO_UPDATE_VERSION_CHAIN")
target_link_libraries(tpcc_SI_oltpim corobase_oltpim_si)
endif()
