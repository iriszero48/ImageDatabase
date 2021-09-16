#pragma once

#include <iostream>

namespace Console
{
	constexpr int Version[]{ 1, 0, 0, 0 };

	enum class Color
	{
		Red,
		Yellow,
		White,
		Blue,
		Gray
	};

	namespace __Detail
	{
		static Color ForegroundColor = Color::White;

		template<typename Stream, typename ...Args>
		void StreamCombine(Stream& stream, Args&&...args)
		{
			(stream << ... << args);
		}

		template<typename Stream, typename ...Args>
		void StreamCombineLine(Stream& stream, Args&&...args)
		{
			(stream << ... << args) << std::endl;
		}
	}

	void SetForegroundColor(const Color& color)
	{
		__Detail::ForegroundColor = color;
	}

	template<typename ...Args>
	void Write(Args&&...args)
	{
#ifdef _MSC_VER
		__Detail::StreamCombine(std::cout, std::forward<Args>(args)...);
#else
		if (__Detail::ForegroundColor == Color::White)
		{
			__Detail::StreamCombine(std::cout, std::forward<Args>(args)...);
			return;
		}

		static std::unordered_map<Color, std::string_view> ColorMap
		{
			{ Color::Red, "31" },
			{ Color::Yellow, "33" },
			{ Color::Blue, "34" },
			{ Color::Gray, "30" },
		};	

#endif
	}

	template<typename ...Args>
	void WriteLine(Args&&...args)
	{
		__Detail::StreamCombineLine(std::cout, std::forward<Args>(args)...);
	}

	namespace Error
	{
		template<typename ...Args>
		void Write(Args&&...args)
		{
			__Detail::StreamCombine(std::cout, std::forward<Args>(args)...);
		}

		template<typename ...Args>
		void WriteLine(Args&&...args)
		{
			__Detail::StreamCombineLine(std::cerr, std::forward<Args>(args)...);
		}
	}
}