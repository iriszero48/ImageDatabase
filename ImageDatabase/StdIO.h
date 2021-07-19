#pragma once

#include <iostream>

namespace Console
{
	namespace __detail
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
		__detail::StreamCombine(std::cout, std::forward<Args>(args)...);
	}

	template<typename ...Args>
	void WriteLine(Args&&...args)
	{
		__detail::StreamCombineLine(std::cout, std::forward<Args>(args)...);
	}

	namespace Error
	{
		template<typename ...Args>
		void Write(Args&&...args)
		{
			__detail::StreamCombine(std::cout, std::forward<Args>(args)...);
		}

		template<typename ...Args>
		void WriteLine(Args&&...args)
		{
			__detail::StreamCombineLine(std::cerr, std::forward<Args>(args)...);
		}
	}
}