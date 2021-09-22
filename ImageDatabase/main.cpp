#include <unordered_map>
#include <filesystem>
#include <utility>
#include <fstream>
#include <exception>
#include <optional>
#include <thread>
#include <string>
#include <chrono>
#include <regex>

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
	std::u16string Message;
}; 

static Logger<LogMsg> Log;
#define LogImpl(level, func, ...) Log.Write<level>(std::chrono::system_clock::now(), "main.cpp", __LINE__, func, String::FormatU16str(__VA_ARGS__))
#define LogNone(func, ...) LogImpl(LogLevel::None, func, __VA_ARGS__)
#define LogErr(func, ...) LogImpl(LogLevel::Error, func, __VA_ARGS__)
#define LogWarn(func, ...) LogImpl(LogLevel::Warn, func, __VA_ARGS__)
#define LogLog(func, ...) LogImpl(LogLevel::Log, func, __VA_ARGS__)
#define LogInfo(func, ...) LogImpl(LogLevel::Info, func, __VA_ARGS__)
#define LogDebug(func, ...) LogImpl(LogLevel::Debug, func, __VA_ARGS__)

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
	ArgumentsParse::Argument<std::unordered_set<uint64_t>> whiteListArg
	{
		"--white",
		"white list: 'index1;index2;...'",
		[](const auto& value)
		{
			const auto vf = std::string(value) + ";";
			const std::regex re(R"([^;]+?;)");
			auto begin = std::sregex_iterator(vf.begin(), vf.end(), re);
			const auto end = std::sregex_iterator();
			std::unordered_set<uint64_t> wl{};
			for (auto i = begin; i != end; ++i)
			{
				const auto match = i->str();
				const auto f = match.substr(0, match.length() - 1);
				wl.emplace(*Convert::FromString<uint64_t>(f));
			}
			return wl;
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
		.Add(whiteListArg)
		.Add(logLevelArg)
		.Add(logFileArg);

	const auto usage = [&]() { return String::FormatStr("Usage:\n{}\n{}", argv[0], args.GetDesc()); };

#ifdef CatchEx
	try
#endif
	{
		std::thread logThread;
		args.Parse(argc, argv);

		LogInfo("main", "\n{}", args.GetValuesDesc({
			args.GetValuesDescConverter<std::string>([](const auto& x) { return x; }),
			args.GetValuesDescConverter<std::filesystem::path>([](const auto& x) { return x.string(); }),
			args.GetValuesDescConverter<Operator>([](const auto& x) { return ToString(x); }),
			args.GetValuesDescConverter<LogLevel>([](const auto& x) { return ToString(x); }),
			args.GetValuesDescConverter<float>([](const auto& x) { return *Convert::ToString(x); }),
		}));

		const auto logLevel = args.Value(logLevelArg);

		av_log_set_level([&]()
		{
			switch (logLevel)
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
			auto data = std::filesystem::u8path(buf).u16string();
			if (ffLevel <= 16) LogErr("main::av", data);
			else if (ffLevel <= 24) LogWarn("main::av", data);
			else if (ffLevel <= 32) LogLog("main::av", data);
			else LogDebug("main::av", data);
		});

		cv::redirectError([](const int status, const char* funcName, const char* errMsg,
			const char* fileName, const int line, void*)
		{
			LogErr("main::cv", "[{}:{}] [{}] error {}: {}", fileName, line, funcName, status, errMsg);
			return 0;
		});
		
		logThread = std::thread([](const LogLevel& level, std::filesystem::path logFile)
		{
			Log.level = level;
			std::ofstream fs;
			if (!logFile.empty())
			{
				fs.open(logFile);
				const auto buf = std::make_unique<char[]>(4096);
				fs.rdbuf()->pubsetbuf(buf.get(), 4096);
				if (!fs)
				{
					LogErr("main::logThread", "log file '{}': open fail", logFile.u16string());
					logFile = "";
				}
			}

			std::unordered_map<LogLevel, Console::Color> colorMap
			{
				{ LogLevel::None, Console::Color::White },
				{ LogLevel::Error, Console::Color::Red },
				{ LogLevel::Warn, Console::Color::Yellow },
				{ LogLevel::Log, Console::Color::White },
				{ LogLevel::Info, Console::Color::Blue },
				{ LogLevel::Debug, Console::Color::Gray }
			};

			while (true)
			{
				const auto [level, raw] = Log.Chan.Read();
				const auto& [time, file, line, func, msg] = raw;

				auto out = String::FormatU16str("[{}] [{}] [{}:{}] [{}] {}", ToString(level), LogTime(time), file, line, func, msg);
				if (out[out.length() - 1] == u'\n') out.erase(out.length() - 1);
				const auto outU8 = std::filesystem::path(out).u8string();

				SetForegroundColor(colorMap.at(level));
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
			static const auto LogForward = [](const std::string& u8Msg)
			{
				LogDebug("main::LogForward", std::filesystem::u8path(u8Msg).u16string());
			};
			
			std::unordered_map<Operator, std::function<void()>>
			{
				{Operator::build, [&]()
				{
					const auto dbPath = args.Value(dbArg);
					const auto buildPath = args.Value(pathArg);
					const auto whiteList = args.Get(whiteListArg);

					ImageDatabase::Database db{};
					if (logLevel >= LogLevel::Info) db.SetLog(LogForward);

					Thread::Channel<ImageDatabase::Image> mq{};

					std::thread receiver([](decltype(Log)& Log, Thread::Channel<ImageDatabase::Image>& queue, ImageDatabase::Database& database)
					{
						while (true)
						{
							auto i = queue.Read();
							const auto& [p, m, v, _] = i;
							if (p.empty()) break;
							LogInfo("main::map::build::receiver", "{ path: {}; md5: {}; vgg16.[0]: {}; }", p.u16string(), std::string_view(m.data(), 32), v(0, 0));
							database.Images.push_back(std::move(i));
						}
					}, std::ref(Log), std::ref(mq), std::ref(db));

					std::error_code ec;
					std::filesystem::recursive_directory_iterator rec(buildPath, ec);
					for (const auto& file : rec)
					{
						if (ec != std::error_code{})
						{
							LogErr("main::map::build::receiver", "[std::filesystem::recursive_directory_iterator] path '{}': {}", file.path().u16string(), ec.message());
							rec.pop();
							continue;
						}
						if (file.is_regular_file())
						{
							try
							{
								const auto& filePath = file.path();
								LogInfo("main::map::build", "load file '{}'", filePath.u16string());
								ImageDatabase::ImageFile img(mq, filePath);
								if (whiteList.has_value()) img.WhiteList = *whiteList;
								if (logLevel >= LogLevel::Debug) img.SetLog(LogForward);
								img.Parse();
							}
							catch(const std::exception& e)
							{
								LogErr("main::map::build", LogException(e));
							}
							
						}
					}

					mq.Write({});
					receiver.join();
					
					LogLog("main::map::build", "save database: {}", dbPath.u16string());
					db.Save(dbPath);
					LogLog("main::map::build", "database size: {}", db.Images.size());
				}},
				{Operator::query, [&]()
				{
					const auto dbPath = args.Value(dbArg);
					const auto input = args.Value(pathArg);
					const auto threshold = args.Value(thresholdArg);

					LogInfo("main::map::query", "load database: {}", dbPath.u16string());
					ImageDatabase::Database db(dbPath);
					if (logLevel >= LogLevel::Debug) db.SetLog(LogForward);

					Thread::Channel<ImageDatabase::Image> mq{};
					
					LogInfo("main::map::query", "database size: {}", db.Images.size());

					LogInfo("main::map::query", "load file: {}", input.u16string());
					ImageDatabase::ImageFile file(mq, input);
					if (logLevel >= LogLevel::Debug) file.SetLog(LogForward);
					file.WhiteList = { 0 };
					file.Parse();
					const auto img = file.MessageQueue.Read();

					LogInfo("main::map::query", "search start ...");

					Algorithm::ForEach<true>(db.Images.begin(), db.Images.end(), [&](auto& x) { x.Dot = x.Vgg16.dot(img.Vgg16); });
					Algorithm::Sort<true>(db.Images.begin(), db.Images.end(), [](const auto& a, const auto& b)
						{ return std::greater()(a.Dot, b.Dot); });

					LogInfo("main::map::query", "search done.");
					for (const auto& [p, _, _unused, v] : db.Images)
					{
						if (v >= threshold)
						{
							LogLog("main::map::query", "found '{}': {}", p.u16string(), v);
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
			LogErr("main", LogException(ex));
			LogLog("main", usage());
		}

		LogNone("main", "{ok}.");

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
