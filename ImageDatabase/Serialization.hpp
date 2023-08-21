#pragma once

#include <cstdint>
#include <type_traits>
#include <climits>
#include <charconv>
#include <iomanip>

namespace Serialization
{
    constexpr int Version[]{ 1, 0, 0, 0 };

    class Exception: public std::runtime_error
    {
    public:
        using std::runtime_error::runtime_error;
    };
	
    class Eof: public Exception
    {
    public:
        using Exception::Exception;
    };

    namespace __Detail
    {
        template<typename T, std::size_t... N>
        constexpr T EndianSwapImpl(T i, std::index_sequence<N...>)
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
           return EndianSwapImpl<U>(i, std::make_index_sequence<sizeof(T)>{});
        }

        template<typename T>
        T ReadArithmetic(std::istream& fs)
        {
            const auto size = sizeof(T);
            uint8_t buf[size];
            fs.read(reinterpret_cast<char*>(buf), size);
            if (fs.gcount() == 0) throw Eof("[Serialization::__Detail::ReadArithmetic] fs.gcount() == 0");
            auto val = *reinterpret_cast<T*>(buf);
            if constexpr (Endian::Native == Endian::Big) val = EndianSwap(val);
            return val;
        }

        template<typename T>
        T ReadArithmeticStr(std::istream& fs)
        {
            const auto size = sizeof(T) * 2;
            char buf[size];
            fs.read(buf, size);
            T val;
            if (const auto [p, e] = std::from_chars(buf, buf + size, &val, 16); e != std::errc{})
                throw std::runtime_error("[Serialization::__Detail::ReadArithmeticStr<" + std::string(typeid(T).name()) + ">] error "
                    + std::to_string(e) + ": invalid literal: " + std::string(p));        	
            return val;
        }
    	
        template<typename T>
        void WriteArithmetic(std::ostream& fs, T val)
        {
            if constexpr (Endian::Native == Endian::Big) val = EndianSwap(val);
            fs.write(reinterpret_cast<char*>(&val), sizeof(T));
        }

        template<typename T>
        void WriteArithmeticStr(std::ostream& fs, T val)
        {
            constexpr auto size = sizeof(T);
            constexpr auto maxLen = size * 2;
            constexpr auto bufSiz = maxLen + 1;
            char buf[bufSiz]{ 0 };
            if (const auto [p, e] = std::to_chars(buf, buf + bufSiz, val); e != std::errc{})
                throw std::runtime_error("[Serialization::__Detail::WriteArithmeticStr<" + std::string(typeid(T).name()) +">] error "
                    + std::to_string(e) + ": invalid literal: " + std::string(p));
            const auto res = std::string_view(buf);
            const auto len = res.length();
            const std::string pad(maxLen - len, '0');
            fs.write(pad.data(), pad.length());
            fs.write(buf, len);
        }
    }

    class Serialize
    {
    public:
        Serialize(std::ostream& fs) : fs(fs) {}

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
                if constexpr (ToStr)
                {
                    __Detail::WriteArithmeticStr(fs, val);
                	return;
                }

                __Detail::WriteArithmetic(fs, val);
			}
			else if constexpr (std::is_floating_point_v<T>)
			{
				if constexpr (ToStr)
				{
                    std::ostringstream ss{};
                    ss << std::setfill('0') << std::setw(sizeof(val) * 2) << std::hex << val;
                    const auto res = ss.str();
                    fs.write(res.data(), res.length());
                    return;
				}

                __Detail::WriteArithmetic(fs, val);
			}
			else if constexpr ((std::is_base_of_v<std::basic_string<typename T::value_type>, T>
					|| std::is_base_of_v<std::basic_string_view<typename T::value_type>, T>))
			{
                try
                {
                    const uint64_t len = val.length();
                    
                    if constexpr (ToStr)
                    {
                        __Detail::WriteArithmeticStr(fs, len);
                        fs.write(val.data(), len);
                        return;
                    }

                    __Detail::WriteArithmetic(fs, len);
                    fs.write(val.data(), len);
                }
                catch(Eof)
                {
                    std::throw_with_nested(Eof("[Serialization::Serialize::WriteImpl<" + std::string(typeid(T).name()) + ">] len == NULL"));
                }
			}
            else if constexpr (std::is_same_v<T, std::vector<char>>
                || std::is_same_v<T, std::vector<char8_t>>)
            {
                try
                {
                    const uint64_t len = val.size();

                    if constexpr (ToStr)
                    {
                        __Detail::WriteArithmeticStr(fs, len);
                        fs.write((char*)val.data(), len);
                        return;
                    }

                    __Detail::WriteArithmetic(fs, len);
                    fs.write((char*)val.data(), len);
                }
                catch (Eof)
                {
                    std::throw_with_nested(Eof("[Serialization::Serialize::WriteImpl<" + std::string(typeid(T).name()) + ">] len == NULL"));
                }
            }
			else
			{
                fs << val;
			}
        }
    };

    class Deserialize
    {
    public:
        Deserialize(std::istream& fs): fs(fs) {};

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
                if constexpr (FromStr) return __Detail::ReadArithmeticStr<T>(fs);

                return __Detail::ReadArithmetic<T>(fs);
			}
			else if constexpr (std::is_floating_point_v<T>)
			{
				if constexpr (FromStr)
				{
                    constexpr auto size = sizeof(T) * 2;
                    std::string buf(size, 0);
                    fs.read(buf.data(), size);
                    std::stringstream ss(size);
                    T val;
                    ss >> std::hex >> val;
                    return val;
				}

                return __Detail::ReadArithmetic<T>(fs);
			}
			else if constexpr ((std::is_base_of_v<std::basic_string<typename T::value_type>, T>
					|| std::is_base_of_v<std::basic_string_view<typename T::value_type>, T>))
			{
                uint64_t len;
                if constexpr (FromStr)
                {
                    len = __Detail::ReadArithmeticStr<uint64_t>(fs);
                }
                else
                {
                    len = __Detail::ReadArithmetic<uint64_t>(fs);
                }
                std::string buf(len, 0);
                fs.read(&buf[0], len);
                return buf;
			}
            else if constexpr (std::is_same_v<T, std::vector<char>> || std::is_same_v<T, std::vector<char8_t>>)
            {
                uint64_t len;
                if constexpr (FromStr)
                {
                    len = __Detail::ReadArithmeticStr<uint64_t>(fs);
                }
                else
                {
                    len = __Detail::ReadArithmetic<uint64_t>(fs);
                }
                std::vector<typename T::value_type> buf(len);
                fs.read((char*)buf.data(), len);
                return buf;
            }
			else
			{
                T val;
                fs >> val;
                return val;
			}
        }
    };   
}