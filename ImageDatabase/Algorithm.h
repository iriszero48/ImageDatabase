#pragma once

#include <algorithm>

#if defined(WindowsPlatform) || defined(LinuxPlatform)
#include <execution>
#endif

#ifdef MacintoshPlatform
#include <tbb/parallel_sort.h>
#include <tbb/parallel_for.h>
#endif

namespace Algorithm
{
    constexpr int Version[]{1, 0, 0, 0};

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
	
    template<bool Parallel = false, typename It, typename Cmp>
    void Sort(It&& begin, It&& end, Cmp&& cmp)
    {
        if constexpr (Parallel)
        {
#ifdef MacintoshPlatform
			tbb::parallel_sort(begin, end, cmp);
#else
			std::sort(std::execution::par_unseq, begin, end, cmp);
#endif
        }
        else
        {
            std::sort(begin, end, cmp);
        }
    }

    template<bool Parallel = false, typename It, typename Func>
    void ForEach(It&& begin, It&& end, Func&& op)
    {
        if constexpr (Parallel)
        {
            #ifdef MacintoshPlatform
            {
                tbb::parallel_for(0, end - begin, [&](std::size_t index)
                {
                    op(begin + index);
                });
            }
            #else
            {
                std::for_each(std::execution::par_unseq, begin, end, op);
            }
			#endif
        }
        else
        {
            std::for_each(begin, end, op);
        }
    }
}
