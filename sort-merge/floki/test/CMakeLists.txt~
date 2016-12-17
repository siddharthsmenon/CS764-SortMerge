
set(Boost_INCLUDE_DIR ../boost_1_61_0)
set(Boost_LIBRARY_DIR ../boost_1_61_0)
set(BoostSIMD_INCLUDE_DIR ../boostsimd/boost.simd/include)
FIND_PACKAGE ( Threads REQUIRED )
find_package(Boost)

find_path(BoostSIMD_INCLUDE_DIR boost/simd/include/pack.hpp
          PATHS /usr/local/include)


include_directories(. ${Boost_INCLUDE_DIRS} ${BoostSIMD_INCLUDE_DIR} ${Bandit_LIBRARY_DIR})

add_executable(test_bitonic bitonic_run.cpp ../floki/aa_sort.hpp ../floki/detail/aa_sort.hpp)

TARGET_LINK_LIBRARIES ( test_bitonic ${CMAKE_THREAD_LIBS_INIT} )


add_test(NAME aa_sort COMMAND test_aa_sort)

