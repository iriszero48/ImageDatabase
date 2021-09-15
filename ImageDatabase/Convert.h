#pragma once

#include <string>
#include <charconv>
#include <optional>

#ifndef _MSC_VER
#include <sstream>
#endif

namespace Convert
{
	constexpr int Version[]{ 1, 0, 0, 0 };

	namespace __Detail
	{
#ifndef _MSC_VER

		template<typename T>
		[[nodiscard]] std::optional<std::string> ToStringStmImpl(const T& value) noexcept
		{
			try
			{
				std::ostringstream ss;
				ss << value;
				return ss.str();
			}
			catch (...)
			{
				return std::nullopt;
			}
		}

		template<typename T>
		[[nodiscard]] std::optional<T> FromStringStmImpl(const std::string& value) noexcept
		{
			try
			{
				T val;
				std::stringstream ss(value);
				ss >> val;
				return val;
			}
			catch (...)
			{
				return std::nullopt;
			}

		}
#endif

		template<typename T, typename Str, typename Args>
		[[nodiscard]] std::optional<T> FromStringImpl(const Str& value, const Args args) noexcept
		{
			T res;
			const auto begin = value.data();
			const auto end = begin + value.length();
			if (auto [p, e] = std::from_chars(begin, end, res, args); e != std::errc{}) return {};
			return res;
		}

		template<typename T, typename...Args>
		[[nodiscard]] std::optional<std::string> ToStringImpl(const T& value, Args&& ... args) noexcept
		{
			char res[sizeof(T) * 8 + 1] = { 0 };
			if (auto [p, e] = std::to_chars(res, res + 65, value, std::forward<Args>(args)...); e != std::errc{}) return {};
			return res;
		}
	}
	
	template<typename T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
	[[nodiscard]] decltype(auto) ToString(const T value, const int base = 10) noexcept
	{
		return __Detail::ToStringImpl<T>(value, base);
	}

#ifdef _MSC_VER
	template<typename T, std::enable_if_t<std::is_floating_point_v<T>, int> = 0>
	[[nodiscard]] decltype(auto) ToString(const T value) noexcept
	{
		return __Detail::ToStringImpl<T>(value);
	}

	template<typename T, std::enable_if_t<std::is_floating_point_v<T>, int> = 0>
	[[nodiscard]] decltype(auto) ToString(const T value, const std::chars_format& fmt) noexcept
	{
		return __Detail::ToStringImpl<T>(value, fmt);
	}

	template<typename T, std::enable_if_t<std::is_floating_point_v<T>, int> = 0>
	[[nodiscard]] decltype(auto) ToString(const T value, const std::chars_format& fmt, const int precision) noexcept
	{
		return __Detail::ToStringImpl<T>(value, fmt, precision);
	}
#else
	template<typename T, std::enable_if_t<std::is_floating_point_v<T>, int> = 0>
	[[nodiscard]] decltype(auto) ToString(const T value) noexcept
	{
		return __Detail::ToStringStmImpl<T>(value);
	}
#endif

	template<typename T, typename Str, std::enable_if_t<std::is_integral_v<T>, int> = 0>
	[[nodiscard]] decltype(auto) FromString(const Str value, const int base = 10) noexcept
	{
		return __Detail::FromStringImpl<T>(value, base);
	}
	
#ifdef _MSC_VER
	template<typename T, typename Str, std::enable_if_t<std::is_floating_point_v<T>, int> = 0>
	[[nodiscard]] decltype(auto) FromString(const Str value, const std::chars_format& fmt = std::chars_format::general) noexcept
	{
		return __Detail::FromStringImpl<T>(value, fmt);
	}
#else
	template<typename T, typename Str, std::enable_if_t<std::is_floating_point_v<T>, int> = 0>
	[[nodiscard]] decltype(auto) FromString(const Str& value) noexcept
	{
		return __Detail::FromStringStmImpl<T>(std::string(value));
	}
#endif
}
