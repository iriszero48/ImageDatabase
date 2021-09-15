#pragma once

namespace Function
{
	constexpr int Version[]{ 1, 0, 0, 0 };
	
	template<typename Func>
	decltype(auto) Combine(Func&& func)
	{
		return func;
	}
	
	template<typename Func, typename ...Tr>
	decltype(auto) Combine(Func&& func, Tr&&... tr)
	{
		return[func, tail = Combine(tr...)](auto&& ...x){ return tail(func(x...)); };
	}
}
