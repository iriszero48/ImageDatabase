#pragma once

namespace ImageDatabase
{
	class Exception : public CuExcept::U8Exception
	{
		using CuExcept::U8Exception::U8Exception;
	};

	class ApiException : public Exception
	{
		using Exception::Exception;
	};

#define Id_MakeExceptImpl(ex, ...) ex(CuStr::FormatU8(__VA_ARGS__), CuStr::ToU8String(#ex))
#define Id_MakeExcept(...) Id_MakeExceptImpl(ImageDatabase::Exception, __VA_ARGS__)
#define Id_MakeApiExcept(api, ...) Id_MakeExceptImpl(ImageDatabase::ApiException, "[{}] {}", api, CuStr::Format(__VA_ARGS__))
}
