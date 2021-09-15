#pragma once

#include <filesystem>

#include "Arguments.h"
#include "Thread.h"

static_assert(Thread::Version[0] == 1 && Thread::Version[1] == 0 && Thread::Version[2] == 0 && Thread::Version[3] == 0);
static_assert(ArgumentsParse::Version[0] == 1 && ArgumentsParse::Version[1] == 0 && ArgumentsParse::Version[2] == 0 && ArgumentsParse::Version[3] == 0);

constexpr int Version[]{ 1, 0, 0, 0 };

ArgumentOptionHpp(LogLevel, None, Error, Warn, Log, Info, Debug)

template<typename T>
class Logger
{
public:
	struct MsgType
	{
		LogLevel Level;
		T Msg;
	};

	LogLevel level = LogLevel::Debug;
	Thread::Channel<MsgType> Chan{};

	template<LogLevel Level, typename...Args>
	void Write(Args&&... args)
	{
		if (Level <= level)
		{
			WriteImpl<Level>(T{std::forward<Args>(args)...});
		}
	}

private:
	template<LogLevel Level>
	void WriteImpl(const T msg)
	{
		Chan.Write(MsgType{ Level, msg });
	}
};
