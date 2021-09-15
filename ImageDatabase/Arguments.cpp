#include "Arguments.h"

#include <algorithm>
#include <queue>
#include <numeric>

template<class... Ts> struct Visitor : Ts... { using Ts::operator()...; };
template<class... Ts> Visitor(Ts...)->Visitor<Ts...>;

namespace ArgumentsParse
{
	static_assert(Version[0] == 1 && Version[1] == 0 && Version[2] == 0 && Version[3] == 0);
	
	std::string Arguments::GetDesc()
	{
		std::priority_queue<std::string, std::vector<std::string>, std::greater<>> item{};
		std::vector<std::string::size_type> lens{};
		std::transform(
			args.begin(),
			args.end(),
			std::back_inserter(lens),
			[&](const std::pair<std::string, __Detail::IArgument*>& x)
			{
				item.push(x.first); return x.first.length();
			});
		const auto maxLen = *std::max_element(lens.begin(), lens.end()) + 1;
		std::ostringstream ss;
		while (!item.empty())
		{
			const auto i = item.top();
			ss << __Detail::StringCombine(
				i, std::string(maxLen - i.length(), ' '),
				args.at(i)->GetDesc(), "\n");
			item.pop();
		}
		return ss.str();
	}
	std::string Arguments::GetValuesDesc(
		const std::unordered_map<std::type_index, std::function<std::string(std::any const&)>>& map)
	{
		std::priority_queue<std::string, std::vector<std::string>, std::greater<>> item{};
		std::vector<std::string::size_type> lens{};
		for (const auto& [k, _] : args)
		{
			if (!args[k]->Empty())
			{
				item.emplace(k);
				lens.push_back(k.length());
			}
		}
		const auto maxLen = *std::max_element(lens.begin(), lens.end());
		std::ostringstream ss;
		while (!item.empty())
		{
			const auto i = item.top();
			const auto vAny = args[i]->Get();
			ss << __Detail::StringCombine("Arguments [info]: ",
				i, std::string(maxLen - i.length(), ' '), ": ");
			std::string v;
			try
			{
				ss << map.at(vAny.type())(vAny);
			}
			catch (...)
			{
				ss << "unregistered type " << vAny.type().name();
			}
			ss << "\n";
			item.pop();
		}
		return ss.str();
	}

	void Arguments::Parse(const int argc, char** argv)
	{
		if (argc < 2 && std::none_of(args.begin(), args.end(),
			[](const auto& x){ return x.second->Empty(); }) )
		{
			throw __Arguments_Ex__(
				Exception, "ArgumentsParse::Arguments::Parse", "An option must be specified");
		}

		for (auto i = 1; i < argc; ++i)
		{
			auto pos = args.find(argv[i]);
			__Detail::ArgLengthType def = 0;
			if (pos == args.end())
			{
				pos = args.find("");
				def = 1;
			}
			if (pos != args.end())
			{
				if (const auto len = pos->second->GetArgLength(); len + i - def < argc)
				{
					auto setValue = [&]() -> __Detail::IArgument::ParseValueType
					{
						switch (len)
						{
						case 0:
							return { std::nullopt };
						case 1:
							return { argv[i + 1 - def] };
						default:
							std::vector<std::string_view> values{};
							for (__Detail::ArgLengthType j = 0; j < len; ++j)
							{
								values.emplace_back(argv[i + j + 1 - def]);
							}
							return { values };
						}
					}();
					try
					{
						pos->second->Set(setValue);
					}
					catch (...)
					{
						std::throw_with_nested(__Arguments_Ex__(Exception,
							"ArgumentsParse::Arguments::Parse", "invalid data '", std::visit(Visitor {
								[](__Detail::ParseValueTypeImpl<0>::Type)
									-> std::string { return std::string("std::nullopt"); },
								[](const __Detail::ParseValueTypeImpl<1>::Type& val)
									-> std::string { return std::string(val); },
								[](const __Detail::ParseValueTypeImpl<2>::Type& val)
									-> std::string { return
										std::accumulate(val.begin(), val.end(), std::string{},
											[](auto& s, const auto& v) { return s + ";" + std::string(v); }); }
						}, setValue) , "' found when processing arg '", pos->first, "'"));
					}
					i += len - def;
				}
				else
				{
					throw __Arguments_Ex__(Exception,
						"ArgumentsParse::Arguments::Parse", "missing argument for option '", argv[i], "'");
				}
			}
			else
			{
				throw __Arguments_Ex__(Exception,
					"ArgumentsParse::Arguments::Parse", "unrecognized option '", argv[i], "'");
			}
		}
	}

	__Detail::IArgument* Arguments::operator[](const std::string& arg)
	{
		return args.at(arg);
	}
}