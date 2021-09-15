#pragma once

#include <iostream>

namespace Console
{
	constexpr int Version[]{ 1, 0, 0, 0 };
	
	namespace __Detail
	{
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

	template<typename ...Args>
	void Write(Args&&...args)
	{
		__Detail::StreamCombine(std::cout, std::forward<Args>(args)...);
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