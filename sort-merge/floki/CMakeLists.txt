PROJECT(floki)

CMAKE_MINIMUM_REQUIRED(VERSION 2.8)


set(Boost_INCLUDE_DIR boost_1_61_0)
set(Boost_LIBRARY_DIR boost_1_61_0)
set(BoostSIMD_INCLUDE_DIR boostsimd/boost.simd/include)

find_package(Boost)

find_path(BoostSIMD_INCLUDE_DIR boost/simd/include/pack.hpp
          PATHS /usr/local/include)


include_directories(. ${Boost_INCLUDE_DIRS} ${BoostSIMD_INCLUDE_DIR} ${Bandit_LIBRARY_DIR})
 

ADD_DEFINITIONS(
  -std=c++11
   -Wall
)


add_subdirectory(test)
enable_testing()

