#pragma once

#include <cstdint>
#include <type_traits>
#include <climits>

namespace Bit
{
	constexpr int Version[] { 1, 0, 0, 0 };

    namespace __Detail
    {
        template<typename T, std::size_t... N>
        constexpr T EndianSwapImpl(T i, std::index_sequence<N...>)
        {
            return (((i >> N * CHAR_BIT & static_cast<std::uint8_t>(-1)) << (sizeof(T) - 1 - N) * CHAR_BIT) | ...);
        }

        template<decltype(sizeof(int)) N> struct IntegerTypes {};
		template<> struct IntegerTypes<sizeof(uint8_t)> { using Value = uint8_t; };
		template<> struct IntegerTypes<sizeof(uint16_t)> { using Value = uint16_t; };
		template<> struct IntegerTypes<sizeof(uint32_t)> { using Value = uint32_t; };
		template<> struct IntegerTypes<sizeof(uint64_t)> { using Value = uint64_t; };
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
	
	template<typename T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
	constexpr T EndianSwap(T i)
	{
		return __Detail::EndianSwapImpl<T>(i, std::make_index_sequence<sizeof(T)>{});
	}

	template<typename T, std::enable_if_t<std::is_floating_point_v<T>, int> = 0>
	constexpr T EndianSwap(T i)
	{
		constexpr auto size = sizeof(T);
		using Type = typename __Detail::IntegerTypes<size>::Value;
		return static_cast<T>(__Detail::EndianSwapImpl<Type>(static_cast<Type>(i), std::make_index_sequence<size>{}));
	}
}
