#pragma once

#include <functional>
#include <algorithm>

namespace __Detail
{
    namespace Environment
    {
        enum Platform
        {
            Windows = 0,
            Linux = 1,
            Macintosh = 2,
            Native =
#if defined(WIN32) || defined(_WIN32) || defined(__WIN32__) || defined(__NT__) || defined(_WIN64)
                Windows
#define WindowsPlatform
#elif __linux__
                Linux
#define LinuxPlatform
#elif __APPLE__
                Macintosh
#define MacintoshPlatform
#else
#error "Unknown Platform"
#endif
        };
    }
}

#if defined(WindowsPlatform) || defined(LinuxPlatform)
#include <execution>
#endif

#ifdef MacintoshPlatform
#include <tbb/parallel_sort.h>
#endif

namespace Algorithm
{
    template<bool Parallel = false, typename It, typename Cmp>
    void Sort(It&& begin, It&& end, Cmp&& cmp)
    {
        if constexpr (Parallel)
        {
            #ifdef MacintoshPlatform
            {
                tbb::parallel_sort(begin, end, cmp);
            }
            #else
            {
                std::sort(std::execution::par_unseq, begin, end, cmp);
            }
			#endif
        }
        else
        {
            std::sort(begin, end, cmp);
        }
    }
}
