
#include <algorithm>
#include <limits>
#include <vector>
#include <random>
#include </home/siddharth/sort-merge-simd/bitonic-sort-simd/floki/floki/aa_sort.hpp>

using boost::tuples::tie;
using namespace floki;



int main()
{



            std::vector<int> values{ { 50, 40, 30, 20 },
                                        { 10, 0, 60, 55 },
                                        { 88, 22, 44, 96 },
                                        { 24, 6, 4, 3 } };

            std::vector<int> sorted_values{ { 0, 3, 4, 6 },
                                               { 10, 20, 22, 24 },
                                               { 30, 40, 44, 50 },
                                               { 55, 60, 88, 96 } };

            tie(values[0], values[1], values[2], values[3])
                = floki::detail::bitonic_sort_16(values[0], values[1],
                                                 values[2], values[3]);
}
