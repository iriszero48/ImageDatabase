#pragma once

#include <filesystem>
#include <thread>

#include "Arguments.h"
#include "Thread.h"

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
			WriteImpl<Level>(T(std::forward<Args>(args)...));
		}
	}

private:
	template<LogLevel Level>
	void WriteImpl(const T msg)
	{
		Chan.Write(MsgType{ Level, msg });
	}
};
