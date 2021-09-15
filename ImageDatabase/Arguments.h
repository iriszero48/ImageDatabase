#pragma once

#include <cstdint>
#include <string>
#include <any>
#include <functional>
#include <optional>
#include <sstream>
#include <typeindex>
#include <variant>
#include <unordered_map>
#include <filesystem>

#define __Arguments_Ex__(ex, func, ...)\
	ex("[", func, "] [", std::filesystem::path(__FILE__).filename().string(), ":", __LINE__, "] ", __VA_ARGS__)

namespace ArgumentsParse
{
	constexpr int Version[]{ 1, 0, 0, 0 };
	
	namespace __Detail
	{
		using ArgLengthType = uint32_t;
		
		template<ArgLengthType Len> struct ParseValueTypeImpl { using Type = std::vector<std::string_view>; };
		template<> struct ParseValueTypeImpl<1> { using Type = std::string_view; };
		template<> struct ParseValueTypeImpl<0> { using Type = std::nullopt_t; };

		class IArgument
		{
		public:
			using ParseValueType = std::variant<
				ParseValueTypeImpl<0>::Type,
				ParseValueTypeImpl<1>::Type,
				ParseValueTypeImpl<2>::Type>;

			IArgument() = default;
			virtual ~IArgument() = default;
			IArgument(const IArgument& iArgument) = default;
			IArgument(IArgument&& iArgument) = default;
			IArgument& operator=(const IArgument& iArgument) = default;
			IArgument& operator=(IArgument&& iArgument) = default;

			virtual void Set(const ParseValueType& value) = 0;
			[[nodiscard]] virtual bool Empty() const = 0;
			[[nodiscard]] virtual std::any Get() const = 0;
			[[nodiscard]] virtual std::string GetName() const = 0;
			[[nodiscard]] virtual std::string GetDesc() const = 0;
			[[nodiscard]] virtual ArgLengthType GetArgLength() const = 0;
		};

		template <typename ...Args>
		std::string StringCombine(Args&&... args)
		{
			std::ostringstream ss;
			(ss << ... << args);
			return ss.str();
		}
	}
	
	class Exception: public std::runtime_error
	{
	public:
		template <typename ...Args>
		Exception(Args&&... args): std::runtime_error(__Detail::StringCombine(std::forward<Args>(args)...)) {}
	};

	class ConvertException: public Exception
	{
	public:
		using Exception::Exception;
	};
	
	template <typename T = std::string, __Detail::ArgLengthType ArgLength = 1>
	class Argument final : public __Detail::IArgument
	{
		using ConvFuncParamType = typename __Detail::ParseValueTypeImpl<ArgLength>::Type;

		using ConvFuncParamTypeConstRef = const ConvFuncParamType&;
		using ConvFuncRetType = T;
		using ConvFuncType = std::function<ConvFuncRetType(ConvFuncParamTypeConstRef)>;
	public:
		using ValueType = T;

		explicit Argument(
			std::string name,
			std::string desc,
			ValueType defaultValue,
			ConvFuncType convert = DefaultConvert) :
			name(std::move(name)),
			desc(std::move(desc)),
			val(std::move(defaultValue)),
			convert(convert) {}

		explicit Argument(
			std::string name,
			std::string desc,
			ConvFuncType convert = DefaultConvert) :
			name(std::move(name)),
			desc(std::move(desc)),
			convert(convert) {}

		void Set(const ParseValueType& value) override
		{
			const auto valueUnwarp = std::get<ConvFuncParamType>(value);

			try
			{
				val = convert(valueUnwarp);
			}
			catch (...)
			{
				std::throw_with_nested(
					__Arguments_Ex__(
						ConvertException,
						"ArgumentsParse::Argument::Set", "convert to '", typeid(ValueType).name() ,"' fail"));
			}
		}

		[[nodiscard]] bool Empty() const override { return val.type() == typeid(std::nullopt); }
		[[nodiscard]] std::any Get() const override { return val; }
		[[nodiscard]] std::string GetName() const override { return name; }
		[[nodiscard]] std::string GetDesc() const override { return desc; }
		[[nodiscard]] __Detail::ArgLengthType GetArgLength() const override { return ArgLength; }

		static ConvFuncRetType DefaultConvert(ConvFuncParamTypeConstRef value) { return ValueType{ value }; }

	private:
		std::string name;
		std::string desc;
		std::any val = std::nullopt;
		ConvFuncType convert;
	};

	class Arguments
	{
	public:
		void Parse(int argc, char** argv);

		template <typename T, __Detail::ArgLengthType ArgLength>
		Arguments& Add(Argument<T, ArgLength>& arg)
		{
			if (args.find(arg.GetName()) != args.end())
			{
				throw __Arguments_Ex__(Exception, "ArgumentsParse::Arguments::Add", "arg '", arg.GetName(), "' exists");
			}
			args[arg.GetName()] = &arg;
			return *this;
		}

		std::string GetDesc();

		template<typename T, typename F>
		std::pair<const std::type_index, std::function<std::string(std::any const&)>> GetValuesDescConverter(F const& f)
		{
			return {
				std::type_index(typeid(T)),
				[g = f](std::any const& a)
				{
					return g(std::any_cast<T const&>(a));
				}
			};
		}

		std::string GetValuesDesc(const std::unordered_map<std::type_index, std::function<std::string(std::any const&)>>& map);

		template <typename T, __Detail::ArgLengthType Len>
		T Value(const Argument<T, Len>& arg) const
		{
			try
			{
				return Get<T, Len>(arg).value();
			}
			catch (...)
			{
				std::throw_with_nested(
					__Arguments_Ex__(Exception, "ArgumentsParse::Arguments::Value", "bad arg '", arg.GetName(), "' access"));
			}
		}

		template <typename T, __Detail::ArgLengthType Len>
		[[nodiscard]] std::optional<T> Get(const Argument<T, Len>& arg) const
		{
			return args.at(arg.GetName())->Empty() ?
				std::nullopt :
				std::make_optional(std::any_cast<T>(args.at(arg.GetName())->Get()));
		}

		__Detail::IArgument* operator[](const std::string& arg);

	private:
		std::unordered_map<std::string, __Detail::IArgument*> args;
	};

#define ArgumentOptionHpp(option, ...)\
	enum class option { __VA_ARGS__ };\
	static auto __##option##_kv_map__ = (([i = 0](std::string str) mutable\
	{\
		str.erase(std::remove(str.begin(), str.end(), ' '), str.end());\
		std::unordered_map<option, const std::string> res{};\
		std::string item;\
		std::stringstream stringStream(str);\
		while (std::getline(stringStream, item, ',')) res.emplace(static_cast<option>(i++), item);\
		return res;\
	})(#__VA_ARGS__));\
	static auto __##option##_vk_map__ = []()\
	{\
		std::unordered_map<std::string, option> res{};\
		for (const auto& [k, v] : __##option##_kv_map__)\
			res[v] = k;\
		return res;\
	}();\
	std::string ToString(const option& in);\
	option To##option(const std::string& in);\
	std::string option##Desc(const std::string& defaultValue = "");

#define ArgumentOptionCpp(option)\
	std::string ToString(const option& in)\
	{\
		try { return __##option##_kv_map__.at(in); }\
		catch (...) { std::throw_with_nested(__Arguments_Ex__(\
			ArgumentsParse::Exception, "ArgumentOption::ToString", "convert to std::string fail")); } }\
	option To##option(const std::string& in)\
	{\
		try { return __##option##_vk_map__.at(in); }\
		catch (...) { std::throw_with_nested(\
			__Arguments_Ex__(ArgumentsParse::Exception, "ArgumentOption::To"#option, "convert to "#option" fail")); } }\
	std::string option##Desc(const std::string& defaultValue)\
	{\
		std::ostringstream oss{};\
		oss << "[";\
		for (const auto kv : __##option##_kv_map__)\
		{\
			const auto sm = kv.second;\
			if (sm == defaultValue)\
			{\
				oss << "(" << sm << ")|";\
				continue;\
			}\
			oss << sm << "|";\
		}\
		auto res = oss.str();\
		res[res.length() - 1] = ']';\
		return res;\
	}

#define ArgumentOption(option, ...)\
	ArgumentOptionHpp(option, __VA_ARGS__)\
	ArgumentOptionCpp(option)
}

#undef __Arguments_ToStringFunc__
#undef __Arguments_ToString__
#undef __Arguments_Line__
#undef __Arguments_ThrowEx__