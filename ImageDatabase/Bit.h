#pragma once

#include <cstdint>
#include <type_traits>
#include <climits>
#include <utility>

namespace Bit
{
	constexpr int Version[] { 1, 0, 0, 0 };

    namespace __Detail
    {
        template<typename T, std::size_t... N>
        constexpr T __EndianSwapImpl__(T i, std::index_sequence<N...>)
        {
            return (((i >> N * CHAR_BIT & static_cast<std::uint8_t>(-1)) << (sizeof(T) - 1 - N) * CHAR_BIT) | ...);
        }
    }

	enum class Endian
	{
#ifdef _WIN32
		Little = 0,
		Big = 1,
		Native = Little
#else
		Little = __ORDER_LITTLE_ENDIAN__,
		Big = __ORDER_BIG_ENDIAN__,
		Native = __BYTE_ORDER__
#endif
	};
	
	template<typename T>
	constexpr T EndianSwap(T i)
	{
		if constexpr (std::is_floating_point_v<T>)
		{
			if constexpr (sizeof(T) == sizeof(uint32_t))
			{
				return (T)__Detail::__EndianSwapImpl__<uint32_t>((uint32_t)i, std::make_index_sequence<sizeof(T)>{});
			}
			else if constexpr (sizeof(T) == sizeof(uint64_t))
			{
				return (T)__Detail::__EndianSwapImpl__<uint64_t>((uint64_t)i, std::make_index_sequence<sizeof(T)>{});
			}
			else
			{

			}
		}
		else
		{
			return __Detail::__EndianSwapImpl__<T>(i, std::make_index_sequence<sizeof(T)>{});
		}
	}
}
