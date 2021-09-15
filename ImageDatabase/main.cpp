#include <unordered_map>
#include <filesystem>
#include <utility>
#include <fstream>
#include <exception>
#include <optional>
#include <thread>
#include <string>
#include <chrono>

extern "C"
{
#include <libavutil/avutil.h>
}

#include "Arguments.h"
#include "Convert.h"
#include "ImageDatabase.h"
#include "StdIO.h"
#include "Thread.h"
#include "Log.h"
#include "Algorithm.h"
#include "Requires.h"
#include "Time.h"
#include "Function.h"

static_assert(Requires::Require(Requires::Version, { 1, 0, 0, 0 }));
static_assert(Requires::Require(Algorithm::Version, { 1, 0, 0, 0 }));
static_assert(Requires::Require(ArgumentsParse::Version, { 1, 0, 0, 0 }));
static_assert(Requires::Require(Bit::Version, { 1, 0, 0, 0 }));
static_assert(Requires::Require(Convert::Version, { 1, 0, 0, 0 }));
static_assert(Requires::Require(Cryptography::Version, { 1,0,0,0 }));
static_assert(Requires::Require(File::Version, { 1,0,0,0 }));
static_assert(Requires::Require(Function::Version, { 1,0,0,0 }));
static_assert(Requires::Require(Serialization::Version, { 1,0,0,0 }));
static_assert(Requires::Require(Console::Version, { 1,0,0,0 }));
static_assert(Requires::Require(String::Version, { 1,0,0,0 }));
static_assert(Requires::Require(Thread::Version, { 1,0,0,0 }));
static_assert(Requires::Require(Time::Version, { 1, 0, 0, 0 }));

ArgumentOption(Operator, build, query)

#define CatchEx

struct LogMsg
{
	decltype(std::chrono::system_clock::now()) Time;
	const char* File;
	decltype(__LINE__) Line;
	const char* Function;
	std::wstring Message;
}; 

static Logger<LogMsg> Log;
#define LogImpl(level, ...) Log.Write<level>(std::chrono::system_clock::now(), "main.cpp", __LINE__, __func__, String::FormatWstr(__VA_ARGS__))
#define LogNone(...) LogImpl(LogLevel::None, __VA_ARGS__)
#define LogErr(...) LogImpl(LogLevel::Error, __VA_ARGS__)
#define LogWarn(...) LogImpl(LogLevel::Warn, __VA_ARGS__)
#define LogLog(...) LogImpl(LogLevel::Log, __VA_ARGS__)
#define LogInfo(...) LogImpl(LogLevel::Info, __VA_ARGS__)
#define LogDebug(...) LogImpl(LogLevel::Debug, __VA_ARGS__)

static void LogExceptionImpl(std::vector<std::string>& out, const std::exception& e, const int level)
{
	out.push_back(String::FormatStr("{}{}", std::string(level * 2, ' '), e.what()));
	try
	{
		std::rethrow_if_nested(e);
	}
	catch(const std::exception& ex)
	{
		LogExceptionImpl(out, ex, level + 1);
	}
}

std::string LogException(const std::exception& e)
{
	std::vector<std::string> out{};
	LogExceptionImpl(out, e, 0);
	return String::JoinStr(out.begin(), out.end(), "\n");
}

decltype(auto) LogTime(const decltype(LogMsg::Time)& time)
{
	auto t = std::chrono::system_clock::to_time_t(time);
	tm local{};
	Time::Local(&local, &t);
	std::ostringstream ss;
	ss << std::put_time(&local, "%F %X");
	return ss.str();
}

int main(int argc, char* argv[])
{
	const auto toStr = [](const auto& x) { return std::string(x); };
	ArgumentsParse::Argument<Operator> opArg
	{
		"",
		"operator " + OperatorDesc(),
		Function::Combine(toStr, ToOperator)
	};
	ArgumentsParse::Argument<std::filesystem::path> dbArg
	{
		"-d",
		"database path"
	};
	ArgumentsParse::Argument<std::filesystem::path> pathArg
	{
		"-i",
		"input path"
	};
	ArgumentsParse::Argument<float> thresholdArg
	{
		"-t",
		"threshold [0.0, 1.0] {0.8}",
		0.8f,
		[](const auto& value)
		{
			const auto val = *Convert::FromString<float>(value);
			if (!(val >= 0.0f && val <= 1.0f))
				throw ArgumentsParse::ConvertException(String::FormatStr("[main::thresholdArg::convert] [main.cpp:{}] range error", __LINE__));
			return val;
		}
	};
	ArgumentsParse::Argument<LogLevel> logLevelArg
	{
		"--loglevel",
		"log level " + LogLevelDesc(ToString(LogLevel::Info)),
		LogLevel::Info,
		Function::Combine(toStr, ToLogLevel)
	};
	ArgumentsParse::Argument<std::filesystem::path> logFileArg
	{
		"--logfile",
		"log file path",
		""
	};
#undef ArgumentsFunc
	
	auto args = ArgumentsParse::Arguments{}
		.Add(opArg)
		.Add(dbArg)
		.Add(pathArg)
		.Add(thresholdArg)
		.Add(logLevelArg)
		.Add(logFileArg);

	const auto usage = [&]() { return String::FormatStr("Usage:\n{}\n{}", argv[0], args.GetDesc()); };

#ifdef CatchEx
	try
#endif
	{
		std::thread logThread;
		args.Parse(argc, argv);

		LogInfo("\n{}", args.GetValuesDesc({
			args.GetValuesDescConverter<std::string>([](const auto& x) { return x; }),
			args.GetValuesDescConverter<std::filesystem::path>([](const auto& x) { return x.string(); }),
			args.GetValuesDescConverter<Operator>([](const auto& x) { return ToString(x); }),
			args.GetValuesDescConverter<LogLevel>([](const auto& x) { return ToString(x); }),
			args.GetValuesDescConverter<float>([](const auto& x) { return *Convert::ToString(x); }),
		}));

		const auto logLevel = args.Value(logLevelArg);

		logThread = std::thread([](const LogLevel& level, std::filesystem::path logFile)
		{
			av_log_set_level([&]()
			{
				switch (level)
				{
				case LogLevel::None:
					return AV_LOG_QUIET;
				case LogLevel::Error:
					return AV_LOG_ERROR;
				case LogLevel::Warn:
					return AV_LOG_WARNING;
				case LogLevel::Debug:
					return AV_LOG_VERBOSE;
				default:
					return AV_LOG_INFO;
				}
			}());
			av_log_set_callback([](void* avc, int ffLevel, const char* fmt, va_list vl)
			{
				static char buf[4096]{ 0 };
				int ret = 1;
				av_log_format_line2(avc, ffLevel, fmt, vl, buf, 4096, &ret);
				auto data = String::ToString<std::wstring>(buf);
				if (const auto pos = data.find_last_of(L'\n'); pos != std::wstring::npos) data.erase(pos);
				if (ffLevel <= 16) LogErr(data);
				else if (ffLevel <= 24) LogWarn(data);
				else if (ffLevel <= 32) LogLog(data);
				else LogDebug(data);
			});

			Log.level = level;
			std::ofstream fs;
			if (!logFile.empty())
			{
				fs.open(logFile);
				const auto buf = std::make_unique<char[]>(4096);
				fs.rdbuf()->pubsetbuf(buf.get(), 4096);
				if (!fs)
				{
					LogErr(L"log file: " + logFile.wstring() + L": open fail");
					logFile = "";
				}
			}

			while (true)
			{
				const auto [level, raw] = Log.Chan.Read();
				const auto& [time, file, line, func, msg] = raw;

				const auto out = String::FormatWstr("[{}] [{}] [{}:{}] [{}] {}", ToString(level), LogTime(time), file, line, func, msg);
				const auto outU8 = std::filesystem::path(out).u8string();

				Console::WriteLine(outU8);
				if (!logFile.empty())
				{
					fs << outU8 << std::endl;
					fs.flush();
				}

				if (level == LogLevel::None)
				{
					fs.close();
					break;
				}
			}
		}, logLevel, args.Value(logFileArg));

		try
		{
			static const auto LogForward = [](const std::string& msg)
			{
				LogDebug(msg);
			};
			
			std::unordered_map<Operator, std::function<void()>>
			{
				{Operator::build, [&]()
				{
					const auto dbPath = args.Value(dbArg);
					const auto buildPath = args.Value(pathArg);

					ImageDatabase::Database db{};
					if (logLevel >= LogLevel::Debug) db.SetLog(LogForward);

					for (const auto& file : std::filesystem::recursive_directory_iterator(buildPath))
					{
						if (file.is_regular_file())
						{
							try
							{
								const auto& filePath = file.path();
								LogInfo("load file '{}'", filePath.wstring());
								ImageDatabase::ImageFile img(filePath);
								if (logLevel >= LogLevel::Debug) img.SetLog(LogForward);
								img.Parse();
								uint64_t size = 0;
								while (true)
								{
									const auto i = img.MessageQueue.Read();
									if (i.Path.empty()) break;
									db.Images.push_back(i);
									++size;
								}
								LogInfo("image count: {}", size);
							}
							catch(const std::exception& e)
							{
								LogErr(LogException(e));
							}
							
						}
					}

					LogLog("save database: {}", dbPath.wstring());
					db.Save(dbPath);
					LogLog("database size: {}", db.Images.size());
				}},
				{Operator::query, [&]()
				{
					const auto dbPath = args.Value(dbArg);
					const auto input = args.Value(pathArg);
					const auto threshold = args.Value(thresholdArg);

					LogInfo("load database: {}", dbPath.wstring());
					ImageDatabase::Database db(dbPath);
					if (logLevel >= LogLevel::Debug) db.SetLog(LogForward);
					
					LogInfo("database size: {}", db.Images.size());

					LogInfo("load file: {}", input.wstring());
					ImageDatabase::ImageFile file(input);
					if (logLevel >= LogLevel::Debug) file.SetLog(LogForward);
					file.WhiteList = { 0 };
					const auto img = file.MessageQueue.Read();

					LogInfo("search start ...");

					Algorithm::ForEach<true>(db.Images.begin(), db.Images.end(), [&](auto& x) { x.Dot = x.Vgg16.dot(img.Vgg16); });
					Algorithm::Sort<true>(db.Images.begin(), db.Images.end(), [](const auto& a, const auto& b)
						{ return std::greater()(a.Dot, b.Dot); });

					LogInfo("search done.");
					for (const auto& [p, _, _unused, v] : db.Images)
					{
						if (v >= threshold)
						{
							LogLog("found '{}': {}", p.wstring(), v);
						}
						else
						{
							break;
						}
					}
				}}
			}[args.Value(opArg)]();
		}
		catch (const std::exception& ex)
		{
			LogErr(LogException(ex));
			LogLog(usage());
		}

		LogNone("{ok}.");

		logThread.join();
	}
#ifdef CatchEx
	catch (const std::exception& e)
	{
		Console::Error::WriteLine(LogException(e));
		Console::Error::WriteLine(usage());
	}
#endif
}
