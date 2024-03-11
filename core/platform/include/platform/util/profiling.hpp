/*
    Proteus -- High-performance query processing on heterogeneous hardware.

                            Copyright (c) 2014
        Data Intensive Applications and Systems Laboratory (DIAS)
                École Polytechnique Fédérale de Lausanne

                            All Rights Reserved.

    Permission to use, copy, modify and distribute this software and
    its documentation is hereby granted, provided that both the
    copyright notice and this permission notice appear in all copies of
    the software, derivative works or modified versions, and any
    portions thereof, and that both notices appear in supporting
    documentation.

    This code is distributed in the hope that it will be useful, but
    WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
    DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
    RESULTING FROM THE USE OF THIS SOFTWARE.
*/

#ifndef PROFILING_HPP_
#define PROFILING_HPP_

#if __has_include("ittnotify.h")
#include <ittnotify.h>
#else
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-macro-identifier"
#define __itt_event void *
#pragma clang diagnostic pop
#endif

#if __has_include("nvtx3/nvToolsExt.h")
#include <nvtx3/nvToolsExt.h>
#endif

/**
 * Utility wrappers around the runtime APIs of some profilers
 * Currently VTune & Cuda profilers
 */
namespace profiling {
void resume();
void pause();

/**
 * Mark a region in a thread
 *
 * Region begins when ProfileRegion is constructed and ends with destructed
 */
class ProfileRegion {
 public:
#if __has_include("ittnotify.h")
  [[nodiscard]] explicit ProfileRegion(const std::string &region_name)
      : m_event(__itt_event_create(region_name.c_str(), region_name.length())) {
#else
  [[nodiscard]] explicit ProfileRegion(const std::string &region_name) {
#endif

#if __has_include("nvtx3/nvToolsExt.h")
    nvtxRangePushA(region_name.c_str());
#endif

#if __has_include("ittnotify.h")
    __itt_event_start(m_event);
#endif
  }

  ~ProfileRegion() {
#if __has_include("nvtx3/nvToolsExt.h")
    nvtxRangePop();
#endif

#if __has_include("ittnotify.h")
    __itt_event_end(m_event);
#endif
  }

 private:
#if __has_include("ittnotify.h")
  const __itt_event m_event;
#endif
};

/**
 * Mark a single point in time in a thread
 */
class ProfileMarkPoint {
 public:
  /**
   * Constructor does not mark a point. Points can be marked with @see
   * ProfileMarkPoint::mark
   */
#if __has_include("ittnotify.h")
  ProfileMarkPoint(std::string point_name)
      : m_point_name(std::move(point_name)),
        m_event(__itt_event_create(point_name.c_str(), point_name.length())) {}
#else
  ProfileMarkPoint(std::string point_name)
      : m_point_name(std::move(point_name)) {}
#endif

  /**
   * Mark a point in time. This can be called multiple times on the same
   * ProfileMarkPoint
   */
  inline void mark() {
#if __has_include("nvtx3/nvToolsExt.h")
    nvtxMarkA(m_point_name.c_str());
#endif

#if __has_include("ittnotify.h")
    __itt_event_start(m_event);
#endif
  }

 private:
  const std::string m_point_name;
#if __has_include("ittnotify.h")
  const __itt_event m_event;
#endif
};

}  // namespace profiling

#endif /* PROFILING_HPP_ */
