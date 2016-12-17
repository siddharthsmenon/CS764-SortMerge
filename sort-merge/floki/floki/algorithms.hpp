#pragma once

#include <boost/simd/include/pack.hpp>
#include <boost/simd/function/ffs.hpp>
#include <boost/simd/function/hmsb.hpp>
#include <boost/simd/function/is_greater_equal.hpp>
#include <algorithm>

namespace floki {

struct equal_to {
  template <class T, class U>
  typename boost::simd::meta::as_logical<U>::type
  operator()(U const &t0, T const &key) const {
    typename boost::simd::meta::as_logical<U>::type ret;
    ret = t0 == key;
    return ret;
  }
};

struct greater_equal {

  template <class T, class U>
  typename boost::simd::meta::as_logical<T>::type
  operator()(T const &t0, U const &key) const {
    typedef typename boost::simd::meta::as_logical<T>::type result_type;
    return result_type(t0 >= key);
  }
};

template <class T, class UnOp>
const T *find_if(const T *begin, const T *end, UnOp f) {
  typedef boost::simd::native<T, BOOST_SIMD_DEFAULT_EXTENSION> vT;

  static const std::size_t N = vT::static_size;

  auto end2 = begin + (std::distance(begin, end) / N) * N;

  for (; begin != end2; begin += N) {
    auto mask = boost::simd::hmsb(f(boost::simd::load<vT>(begin)));
    if (mask) {
      begin += boost::simd::ffs(mask) - 1;
      break;
    }
  }
  // epilogue
  if (begin == end2) {
    begin = std::find_if(begin, end, f);
  }

  return begin;
}
}
