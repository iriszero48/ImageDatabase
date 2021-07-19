#pragma once

#include <string>
#include <algorithm>
#include <sstream>

namespace String
{
	template<typename Func, typename ...Args>
	std::string NewString(const Func func, Args&&...args)
	{
		std::string buf{};
		func(buf, std::forward<Args>(args)...);
		return buf;
	}

	template<typename Func>
	struct New
	{
		Func Caller;

		explicit New(const Func caller) : Caller(caller) {}

		template<typename T, typename ...Args>
		std::string operator()(const T str, Args&&...args)
		{
			std::string buf(str);
			Caller(buf, std::forward<Args>(args)...);
			return buf;
		}
	};

	template<typename T>
	void ToUpper(T& string)
	{
		std::transform(string.begin(), string.end(), string.begin(), static_cast<int(*)(int)>(std::toupper));
	}

	template<typename T>
	void ToLower(T& string)
	{
		std::transform(string.begin(), string.end(), string.begin(), static_cast<int(*)(int)>(std::tolower));
	}

	template<typename Str>
	void PadLeft(Str& str, const std::uint32_t width, const typename Str::value_type pad)
	{
		std::int64_t n = width - str.length();
		if (n <= 0) return;
		str.insert(str.begin(), n, pad);
	}

	template<typename Str>
	void PadRight(Str& str, const std::uint32_t width, const typename Str::value_type pad)
	{
		std::int64_t n = width - str.length();
		if (n <= 0) return;
		str.append(n, pad);
	}

	template<typename Str, typename...Args>
	void StringCombine(Str& str, Args&&... args)
	{
		(str.append(args), ...);
	}

	template<typename...Args>
	std::string StringCombineNew(Args&&... args)
	{
		std::string str{};
		StringCombine(str, std::forward<Args>(args)...);
		return str;
	}

	template<typename T, typename...Args>
	void FromStream(std::string& str, const T& stream, Args&&...fmt)
	{
		std::ostringstream buf{};
		(buf << ... << fmt) << stream;
		str.append(buf.str());
	}

	template<typename T, typename...Args>
	std::string FromStreamNew(const T& stream, Args&&...fmt)
	{
		std::ostringstream buf{};
		(buf << ... << fmt) << stream;
		return buf.str();
	}

	template<typename It, typename Str, typename Out>
	void Join(Out& str, It beg, It end, const Str& seq)
	{
		auto i = beg;
		for (; i < end - 1; ++i)
		{
			str.append(*i);
			str.append(seq);
		}
		str.append(*i);
	}

	template<typename It, typename Str>
	std::string JoinNew(It beg, It end, const Str& seq)
	{
		std::string str{};
		Join(str, beg, end, seq);
		return str;
	}
}
