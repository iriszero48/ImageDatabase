#include <unordered_map>
#include <filesystem>
#include <utility>
#include <fstream>
#include <exception>
#include <optional>
#include <thread>
#include <string>
#include <chrono>

#include <zip.h>
#include <boost/context/continuation.hpp>

extern "C" {
#include <libavformat/avformat.h>
#include <libavcodec/avcodec.h>
#include <libavutil/avutil.h>
#include <libavutil/imgutils.h>
#include <libavutil/pixdesc.h>
#include <libavutil/error.h>
#include <libswscale/swscale.h>
}

#include "Arguments.h"
#include "Convert.h"
#include "ImageDatabase.h"
#include "StdIO.h"
#include "Thread.h"
#include "Log.h"
#include "Algorithm.h"
#include "File.h"
#include "Requires.h"
#include "Time.h"

static_assert(Requires::Require(Requires::Version, {1, 0, 0, 0}));
static_assert(Requires::Require(Algorithm::Version, {1, 0, 0, 0}));
static_assert(Requires::Require(Bit::Version, {1, 0, 0, 0}));

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

static void __LogException_Impl__(std::vector<std::string>& out, const std::exception& e, int level)
{
	out.push_back(String::FormatStr("{}{}", std::string(level * 2, ' '), e.what()));
	try
	{
		std::rethrow_if_nested(e);
	}
	catch(const std::exception& e)
	{
		__LogException_Impl__(out, e, level + 1);
	}
}

std::string LogException(const std::exception& e)
{
	std::vector<std::string> out{};
	__LogException_Impl__(out, e, 0);
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
#define ArgumentsFunc(arg) [&](decltype(arg)::ConvertFuncParamType value) -> decltype(arg)::ConvertResult
	
	ArgumentsParse::Argument<Operator> opArg
	{
		"",
		"operator " + OperatorDesc(),
		ArgumentsFunc(opArg) { return { *ToOperator(std::string(value)), {} }; }
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
		0.8,
		ArgumentsFunc(thresholdArg)
		{
			const auto val = *Convert::FromString<float>(value);
			if (!(val >= 0.0 && val <= 1.0)) return { std::nullopt, "range error" };
			return { val, {} };
		}
	};
	ArgumentsParse::Argument<LogLevel> logLevelArg
	{
		"--loglevel",
		"log level " + LogLevelDesc(ToString(LogLevel::Info)),
		LogLevel::Info,
		ArgumentsFunc(logLevelArg) { return { *ToLogLevel(std::string(value)), {} };	}
	};
	ArgumentsParse::Argument<std::filesystem::path> logFileArg
	{
		"--logfile",
		"log level " + LogLevelDesc(ToString(LogLevel::Info)),
		""
	};
#undef ArgumentsFunc
	
	ArgumentsParse::Arguments args;
	args.Add(opArg);
	args.Add(dbArg);
	args.Add(pathArg);
	args.Add(thresholdArg);
	args.Add(logLevelArg);
	args.Add(logFileArg);

#ifdef CatchEx
	try
#endif
	{
		std::thread logThread;
		args.Parse(argc, argv);

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

				const auto out = String::FormatStr("[{}] [{}] [{}:{}] [{}] {}", ToString(level), LogTime(time), file, line, func, std::filesystem::path(msg).u8string());

				Console::WriteLine(out);
				if (!logFile.empty())
				{
					fs << out << std::endl;
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
					//if (logLevel >= LogLevel::Debug)
					 db.SetLog(LogForward);

					for (const auto& file : std::filesystem::recursive_directory_iterator(buildPath))
					{
						if (file.is_regular_file())
						{
							try
							{
								const auto filePath = file.path();
								LogInfo("load file '{}'", filePath.wstring());
								ImageDatabase::ImageFile img(filePath);
								//if (logLevel >= LogLevel::Debug) 
								img.SetLog(LogForward);
								std::copy(img.Images.begin(), img.Images.end(), std::back_inserter(db.Images));
								LogInfo("image count: {}", img.Images.size());
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
					const auto img = file.Images[0];

					LogInfo("search start ...");

					Algorithm::Sort<true>(db.Images.begin(), db.Images.end(), [&](ImageDatabase::Image& a, ImageDatabase::Image& b)
					{
						if (a._Dot == 0) a._Dot = a.Vgg16.dot(img.Vgg16);
						if (b._Dot == 0) b._Dot = b.Vgg16.dot(img.Vgg16);
						return std::greater()(a._Dot, b._Dot);
					});

					LogInfo("search done.");
					for (const auto& i : db.Images)
					{
						if (const auto v = i._Dot; v >= threshold)
						{
							LogLog("found '{}': {}", i.Path.wstring(), v);
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
		}

		LogNone("{ok}.");

		logThread.join();
	}
#ifdef CatchEx
	catch (const std::exception& e)
	{
		Console::Error::WriteLine(e.what());
		Console::Error::WriteLine(args.GetDesc());
	}
#endif
}
