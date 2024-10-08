#pragma once
#include <regex>
#include <zip.h>
#include <Convert/Convert.hpp>

namespace ImageDatabase
{
	namespace Detail
	{
		inline std::string LibzipErrStr(const zip_error_t& err)
		{
			std::string buf;

			if (const auto se = err.sys_err; se != 0)
			{
				buf.append("system error: ");
				if (se == 1) buf.append("ZIP_ET_SYS");
				else if (se == 2) buf.append("ZIP_ET_ZLIB");
				else buf.append(*CuConv::ToString(se));
				buf.append(", ");
			}

			buf.append("zip error: ");
			zip_error_t error;
			zip_error_init_with_code(&error, err.zip_err);
			buf.append(zip_error_strerror(&error));
			zip_error_fini(&error);

			return buf;
		}
	}

	struct Regex
	{
		Regex(const std::string_view& regex)
		{
			RawString = regex;
			const auto regexU8 = CuStr::ToDirtyUtf8String(CuStr::ToU8String(regex));
			const std::regex re(R"(^\/(.+)\/([i]*?)$)");

			std::smatch match;
			if (!std::regex_search(regexU8, match, re))
				throw std::runtime_error("invalid regex");

			decltype(std::regex_constants::ECMAScript | std::regex_constants::icase) flags = std::regex_constants::ECMAScript;
			if (match[2].str() == "i") flags |= std::regex_constants::icase;
			Native = std::regex(match[1].str(), flags);
		}

		std::string RawString{};
		std::regex Native{};
	};

	template <typename T>
	struct Pointer
	{
		T* Native = nullptr;
		std::size_t Size = 0;

		Pointer() = default;

		explicit Pointer(const size_t size) : Native(new T[size]), Size(size) {}

		[[nodiscard]] static Pointer CopyFrom(const T* p, const size_t s)
		{
			Pointer ret(s);
			memcpy(ret.Native, p, s);
			return ret;
		}

		template <size_t S>
		[[nodiscard]] static Pointer CopyFrom(const std::span<T, S>& span)
		{
			return CopyFrom(span.data(), span.size());
		}

		[[nodiscard]] static Pointer CopyFrom(const Pointer& ptr)
		{
			return CopyFrom(ptr.Native, ptr.Size);
		}

		Pointer& operator=(const Pointer& ptr) = delete;

		Pointer& operator=(Pointer&& ptr) noexcept
		{
			delete[] Native;

			Native = ptr.Native;
			Size = ptr.Size;

			ptr.Native = nullptr;
			ptr.Size = 0;

			return *this;
		}

		Pointer(Pointer&& ptr) noexcept
		{
			Native = ptr.Native;
			Size = ptr.Size;

			ptr.Native = nullptr;
			ptr.Size = 0;
		}

		~Pointer()
		{
			delete[] Native;
		}

		operator std::span<T>()
		{
			return { Native, Size };
		}
	};

	template <typename T>
	struct StringPointer
	{
		Pointer<T> Native;

		[[nodiscard]] static StringPointer CopyFrom(const std::basic_string_view<T>& str)
		{
			StringPointer ret{};
			ret.Native = Pointer<T>(str.length() + 1);
			ret.Native.Native[str.length()] = 0;
			ret.Native.Size = str.length();
			memcpy(ret.Native.Native, str.data(), str.length());
			return ret;
		}

		operator std::basic_string_view<T>()
		{
			return { Native.Native, Native.Size };
		}
	};
}