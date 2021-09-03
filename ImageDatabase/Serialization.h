#pragma once

#include <fstream>
#include <cstdint>
#include <type_traits>
#include <climits>

#include "String.h"

namespace Serialization
{
    namespace __Detail
    {
        template<typename T, std::size_t... N>
        constexpr T __EndianSwapImpl__(T i, std::index_sequence<N...>)
        {
            return (((i >> N * CHAR_BIT & static_cast<std::uint8_t>(-1)) << (sizeof(T) - 1 - N) * CHAR_BIT) | ...);
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
            
        template<typename T, typename U = std::make_unsigned_t<T>>
        [[nodiscard]] constexpr U EndianSwap(T i)
        {
           return __EndianSwapImpl__<U>(i, std::make_index_sequence<sizeof(T)>{});
        }

        template<typename T>
        T ReadArithmetic(std::istream& fs)
        {
            const auto size = sizeof(T);
            uint8_t buf[size];
            fs.read((char*)buf, size);
            auto val = *(T*)buf;
            if constexpr (Endian::Native == Endian::Big) val = EndianSwap(val);
            return val;
        }

        template<typename T>
        void WriteArithmetic(std::ostream& fs, T val)
        {
            if constexpr (Endian::Native == Endian::Big) val = EndianSwap(val);
            fs.write((char*)&val, sizeof(T));
        }
    }

    class Serialize
    {
    public:
        Serialize(std::ostream& fs) : fs(fs){};

        template<typename... Args>
        void WriteAsString(Args&&... args)
        {
            (WriteImpl<true>(args),...);
        }

        template<typename... Args>
        void Write(Args&&... args)
        {
            (WriteImpl<false>(args),...);
        }
    private:
        std::ostream& fs;

        template<bool ToStr, typename T>
        void WriteImpl(const T& val)
        {
            if constexpr (std::is_integral_v<T>)
			{
                if constexpr (ToStr) {}

                __Detail::WriteArithmetic(fs, val);
				//constexpr auto bufSiz = 65;
				//char buf[bufSiz]{0};
				//const auto [p, e] = std::to_chars(buf, buf + bufSiz, t);
				//if (e != std::errc{}) throw std::runtime_error("ToStringImpl error: invalid literal: " + std::string(p));
				//return buf;
			}
			else if constexpr (std::is_floating_point_v<T>)
			{
				if constexpr (ToStr) {}

                __Detail::WriteArithmetic(fs, val);
			}
			else if constexpr ((std::is_base_of_v<std::basic_string<typename T::value_type>, T>
					|| std::is_base_of_v<std::basic_string_view<typename T::value_type>, T>))
			{
                if constexpr (ToStr) {}

                const auto len = val.length();
                __Detail::WriteArithmetic<uint64_t>(fs, len);
                fs.write(val.data(), len);
			}
			else
			{
				
			}
        }
    };

    class Unserialize
    {
    public:
        Unserialize(std::istream& fs): fs(fs) {};

        template<typename... Args>
        decltype(auto) ReadFromString()
        {
            return std::make_tuple((ReadImpl<Args, true>(),...));
        }

        template<typename... Args>
        decltype(auto) Read()
        {
            return std::make_tuple((ReadImpl<Args, false>(),...));
        }
    private:
        std::istream& fs;

        template<typename T, bool FromStr>
        T ReadImpl()
        {
			if constexpr (std::is_integral_v<T>)
			{
                if constexpr (FromStr) {}

                return __Detail::ReadArithmetic<T>(fs);
				//constexpr auto bufSiz = 65;
				//char buf[bufSiz]{0};
				//const auto [p, e] = std::to_chars(buf, buf + bufSiz, t);
				//if (e != std::errc{}) throw std::runtime_error("ToStringImpl error: invalid literal: " + std::string(p));
				//return buf;
			}
			else if constexpr (std::is_floating_point_v<T>)
			{
				if constexpr (FromStr) {}

                return __Detail::ReadArithmetic<T>(fs);
			}
			else if constexpr ((std::is_base_of_v<std::basic_string<typename T::value_type>, T>
					|| std::is_base_of_v<std::basic_string_view<typename T::value_type>, T>))
			{
                if constexpr (FromStr) {}

                const auto len = __Detail::ReadArithmetic<uint64_t>(fs);
                std::string buf(len, 0);
                fs.read(&buf[0], len);
                return buf;
			}
			else
			{
				
			}
        }
    };   
}